use anyhow::Result;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::collections::VecDeque;
use tokio::fs;
use tokio::io::{self, AsyncBufReadExt, AsyncWriteExt, BufReader};
use tracing::info;
use uuid::Uuid;

#[derive(Debug, Clone, Serialize, Deserialize)]
struct StackTask {
    id: Uuid,
    function_name: String,
    args: Value,
    delay_ms: u64,
    enqueued_at_ms: u128,
    note: Option<String>,
}

#[derive(Debug, Serialize, Deserialize)]
struct RpcRequest {
    id: Option<Value>,
    method: String,
    #[serde(default)]
    params: Value,
}

#[derive(Debug, Serialize)]
struct RpcResponse {
    jsonrpc: &'static str,
    id: Option<Value>,
    result: Value,
}

#[derive(Default)]
struct AppState {
    stack: VecDeque<StackTask>,
}

fn now_unix_ms() -> u128 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis())
        .unwrap_or(0)
}

fn remaining_delay_ms(task: &StackTask, now_ms: u128) -> u64 {
    let elapsed = now_ms.saturating_sub(task.enqueued_at_ms);
    task.delay_ms.saturating_sub(elapsed as u64)
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter("info")
        .with_writer(std::io::stderr)
        .init();

    let stdin = BufReader::new(io::stdin());
    let mut lines = stdin.lines();
    let mut stdout = io::stdout();
    let mut state = AppState::default();

    info!("message-stack-claw MCP server started");

    while let Some(line) = lines.next_line().await? {
        if line.trim().is_empty() {
            continue;
        }

        let req: RpcRequest = match serde_json::from_str(&line) {
            Ok(v) => v,
            Err(err) => {
                write_response(
                    &mut stdout,
                    RpcResponse {
                        jsonrpc: "2.0",
                        id: None,
                        result: json!({"ok":false,"error":format!("invalid json: {err}")}),
                    },
                )
                .await?;
                continue;
            }
        };

        let result = handle_request(&mut state, &req).await;
        write_response(
            &mut stdout,
            RpcResponse {
                jsonrpc: "2.0",
                id: req.id,
                result,
            },
        )
        .await?;
    }

    Ok(())
}

async fn handle_request(state: &mut AppState, req: &RpcRequest) -> Value {
    match req.method.as_str() {
        "tools/list" => json!({
          "ok": true,
          "tools": [
            {"name":"stack_push","description":"Push a deferred function call onto a LIFO stack","input_schema":{"function_name":"string","args":"object|any","delay_ms":"number","note":"string?"}},
            {"name":"stack_list","description":"List queued deferred calls"},
            {"name":"stack_run_next","description":"Pop top call, wait only remaining delay (based on enqueue time), then execute/simulate"},
            {"name":"stack_run_due","description":"Run due calls in batch without waiting","input_schema":{"limit":"number?","grace_ms":"number?"}},
            {"name":"stack_run_all","description":"Run queued calls from top to bottom"},
            {"name":"stack_clear","description":"Clear all queued calls"},
            {"name":"stack_save","description":"Persist current stack to disk","input_schema":{"path":"string?"}},
            {"name":"stack_load","description":"Load stack from disk and replace current stack","input_schema":{"path":"string?"}}
          ]
        }),
        "tools/call" => call_tool(state, &req.params).await,
        _ => json!({"ok":false,"error":format!("unknown method: {}", req.method)}),
    }
}

async fn call_tool(state: &mut AppState, params: &Value) -> Value {
    let tool = params
        .get("name")
        .and_then(Value::as_str)
        .unwrap_or_default();
    let args = params
        .get("arguments")
        .cloned()
        .unwrap_or_else(|| json!({}));

    match tool {
        "stack_push" => {
            let function_name = args
                .get("function_name")
                .and_then(Value::as_str)
                .unwrap_or_default()
                .to_string();
            if function_name.is_empty() {
                return json!({"ok":false,"error":"function_name is required"});
            }
            let task = StackTask {
                id: Uuid::new_v4(),
                function_name,
                args: args.get("args").cloned().unwrap_or_else(|| json!({})),
                delay_ms: args.get("delay_ms").and_then(Value::as_u64).unwrap_or(0),
                enqueued_at_ms: now_unix_ms(),
                note: args
                    .get("note")
                    .and_then(Value::as_str)
                    .map(ToString::to_string),
            };
            state.stack.push_back(task.clone());
            json!({"ok":true,"task":task,"stack_depth":state.stack.len()})
        }
        "stack_list" => {
            let now_ms = now_unix_ms();
            let tasks: Vec<Value> = state
                .stack
                .iter()
                .map(|task| {
                    json!({
                        "id": task.id,
                        "function_name": task.function_name,
                        "args": task.args,
                        "note": task.note,
                        "delay_ms": task.delay_ms,
                        "enqueued_at_ms": task.enqueued_at_ms,
                        "remaining_delay_ms": remaining_delay_ms(task, now_ms)
                    })
                })
                .collect();
            json!({"ok":true,"stack_depth":state.stack.len(),"tasks":tasks,"now_ms":now_ms})
        }
        "stack_run_next" => run_next(state).await,
        "stack_run_due" => run_due(state, &args).await,
        "stack_run_all" => {
            let mut results = vec![];
            while !state.stack.is_empty() {
                results.push(run_next(state).await);
            }
            json!({"ok":true,"results":results})
        }
        "stack_clear" => {
            let cleared = state.stack.len();
            state.stack.clear();
            json!({"ok":true,"cleared":cleared})
        }
        "stack_save" => save_stack(state, &args).await,
        "stack_load" => load_stack(state, &args).await,
        _ => json!({"ok":false,"error":format!("unknown tool: {tool}")}),
    }
}

fn executed_payload(task: StackTask, status: &str) -> Value {
    json!({
      "id": task.id,
      "function_name": task.function_name,
      "args": task.args,
      "note": task.note,
      "delay_ms": task.delay_ms,
      "enqueued_at_ms": task.enqueued_at_ms,
      "status": status
    })
}

async fn run_next(state: &mut AppState) -> Value {
    let Some(task) = state.stack.pop_back() else {
        return json!({"ok":false,"error":"stack is empty"});
    };

    let remaining_ms = remaining_delay_ms(&task, now_unix_ms());
    if remaining_ms > 0 {
        tokio::time::sleep(std::time::Duration::from_millis(remaining_ms)).await;
    }

    json!({
      "ok": true,
      "waited_ms": remaining_ms,
      "executed": executed_payload(task, "executed"),
      "stack_depth": state.stack.len()
    })
}

fn arg_u64(args: &Value, key: &str) -> Result<Option<u64>, String> {
    match args.get(key) {
        None => Ok(None),
        Some(v) => v
            .as_u64()
            .map(Some)
            .ok_or_else(|| format!("{key} must be a non-negative integer")),
    }
}

async fn run_due(state: &mut AppState, args: &Value) -> Value {
    let limit = match arg_u64(args, "limit") {
        Ok(v) => v,
        Err(err) => return json!({"ok":false,"error":err}),
    };
    let grace_ms = match arg_u64(args, "grace_ms") {
        Ok(v) => v.unwrap_or(0),
        Err(err) => return json!({"ok":false,"error":err}),
    };

    if limit == Some(0) {
        return json!({"ok":true,"ran":0,"results":[],"stack_depth":state.stack.len(),"reason":"limit is 0"});
    }

    if state.stack.is_empty() {
        return json!({"ok":true,"ran":0,"results":[],"stack_depth":0});
    }

    let now_ms = now_unix_ms();
    let mut due_indices = Vec::new();
    for idx in (0..state.stack.len()).rev() {
        if let Some(task) = state.stack.get(idx) {
            let remaining = remaining_delay_ms(task, now_ms);
            if remaining <= grace_ms {
                due_indices.push(idx);
            }
        }

        if let Some(max) = limit {
            if due_indices.len() as u64 >= max {
                break;
            }
        }
    }

    let selected = due_indices.len();
    let mut results = Vec::with_capacity(selected);
    for idx in due_indices {
        if let Some(task) = state.stack.remove(idx) {
            results.push(executed_payload(task, "executed_due"));
        }
    }

    json!({
        "ok": true,
        "ran": results.len(),
        "results": results,
        "stack_depth": state.stack.len(),
        "now_ms": now_ms,
        "grace_ms": grace_ms,
        "limit": limit,
        "selected": selected
    })
}

fn stack_path_from_args(args: &Value) -> String {
    args.get("path")
        .and_then(Value::as_str)
        .unwrap_or(".message-stack-claw.stack.json")
        .to_string()
}

async fn save_stack(state: &AppState, args: &Value) -> Value {
    let path = stack_path_from_args(args);
    let payload = json!({
      "version": 1,
      "saved_at_ms": now_unix_ms(),
      "tasks": state.stack
    });

    match serde_json::to_vec_pretty(&payload) {
        Ok(bytes) => match fs::write(&path, bytes).await {
            Ok(_) => json!({"ok":true,"path":path,"saved":state.stack.len()}),
            Err(err) => json!({"ok":false,"error":format!("save failed: {err}")}),
        },
        Err(err) => json!({"ok":false,"error":format!("serialize failed: {err}")}),
    }
}

async fn load_stack(state: &mut AppState, args: &Value) -> Value {
    let path = stack_path_from_args(args);
    let content = match fs::read_to_string(&path).await {
        Ok(s) => s,
        Err(err) => return json!({"ok":false,"error":format!("load failed: {err}")}),
    };

    let parsed: Value = match serde_json::from_str(&content) {
        Ok(v) => v,
        Err(err) => return json!({"ok":false,"error":format!("invalid stack json: {err}")}),
    };

    let tasks_val = parsed.get("tasks").cloned().unwrap_or_else(|| json!([]));
    let tasks: Vec<StackTask> = match serde_json::from_value(tasks_val) {
        Ok(t) => t,
        Err(err) => {
            return json!({"ok":false,"error":format!("invalid task list in stack file: {err}")})
        }
    };

    state.stack = VecDeque::from(tasks);
    json!({"ok":true,"path":path,"loaded":state.stack.len(),"stack_depth":state.stack.len()})
}

async fn write_response(stdout: &mut io::Stdout, response: RpcResponse) -> Result<()> {
    let out = serde_json::to_string(&response)?;
    stdout.write_all(out.as_bytes()).await?;
    stdout.write_all(b"\n").await?;
    stdout.flush().await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn task_with_times(delay_ms: u64, enqueued_at_ms: u128) -> StackTask {
        StackTask {
            id: Uuid::new_v4(),
            function_name: "message.send".to_string(),
            args: json!({"text":"hi"}),
            delay_ms,
            enqueued_at_ms,
            note: None,
        }
    }

    #[test]
    fn remaining_delay_is_reduced_by_elapsed_time() {
        let task = task_with_times(5_000, 10_000);
        assert_eq!(remaining_delay_ms(&task, 12_000), 3_000);
    }

    #[test]
    fn remaining_delay_saturates_at_zero() {
        let task = task_with_times(1_000, 10_000);
        assert_eq!(remaining_delay_ms(&task, 20_000), 0);
    }

    #[tokio::test]
    async fn run_due_executes_only_due_tasks() {
        let now = now_unix_ms();
        let mut state = AppState {
            stack: VecDeque::from(vec![
                task_with_times(100_000, now),    // not due
                task_with_times(10, now - 5_000), // due
                task_with_times(50, now - 5_000), // due
            ]),
        };

        let result = run_due(&mut state, &json!({})).await;
        assert_eq!(result.get("ok").and_then(Value::as_bool), Some(true));
        assert_eq!(result.get("ran").and_then(Value::as_u64), Some(2));
        assert_eq!(state.stack.len(), 1);
    }

    #[tokio::test]
    async fn run_due_respects_limit() {
        let now = now_unix_ms();
        let mut state = AppState {
            stack: VecDeque::from(vec![
                task_with_times(10, now - 5_000),
                task_with_times(10, now - 5_000),
                task_with_times(10, now - 5_000),
            ]),
        };

        let result = run_due(&mut state, &json!({"limit": 2})).await;
        assert_eq!(result.get("ok").and_then(Value::as_bool), Some(true));
        assert_eq!(result.get("ran").and_then(Value::as_u64), Some(2));
        assert_eq!(state.stack.len(), 1);
    }

    #[tokio::test]
    async fn run_due_grace_ms_includes_near_due_tasks() {
        let now = now_unix_ms();
        let mut state = AppState {
            stack: VecDeque::from(vec![task_with_times(2_000, now - 1_500)]),
        };

        let result = run_due(&mut state, &json!({"grace_ms": 600})).await;
        assert_eq!(result.get("ok").and_then(Value::as_bool), Some(true));
        assert_eq!(result.get("ran").and_then(Value::as_u64), Some(1));
        assert!(state.stack.is_empty());
    }

    #[tokio::test]
    async fn run_due_rejects_invalid_limit_type() {
        let mut state = AppState::default();
        let result = run_due(&mut state, &json!({"limit": "two"})).await;
        assert_eq!(result.get("ok").and_then(Value::as_bool), Some(false));
        assert!(result
            .get("error")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .contains("limit must be a non-negative integer"));
    }
}
