use anyhow::Result;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::collections::VecDeque;
use std::path::{Path, PathBuf};
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

#[derive(Debug, Serialize, Deserialize)]
struct StackFile {
    version: u64,
    saved_at_ms: u128,
    tasks: Vec<StackTask>,
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
            {"name":"stack_push_batch","description":"Push multiple deferred function calls atomically (all-or-nothing)","input_schema":{"items":"array<{function_name,args?,delay_ms?,note?}>"}},
            {"name":"stack_list","description":"List queued deferred calls"},
            {"name":"stack_run_next","description":"Pop top call, wait only remaining delay (based on enqueue time), then execute/simulate"},
            {"name":"stack_run_due","description":"Run due calls in batch without waiting","input_schema":{"limit":"number?","grace_ms":"number?"}},
            {"name":"stack_run_all","description":"Run queued calls from top to bottom"},
            {"name":"stack_clear","description":"Clear all queued calls"},
            {"name":"stack_save","description":"Persist current stack to disk","input_schema":{"path":"string?"}},
            {"name":"stack_load","description":"Load stack from disk (replace or append)","input_schema":{"path":"string?","mode":"replace|append?","dedupe_ids":"boolean? (default true when append)","pause_during_downtime":"boolean? (default false; preserves remaining delays across save→load downtime)"}}
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
            let task = match parse_stack_task(&args) {
                Ok(task) => task,
                Err(err) => return json!({"ok":false,"error":err}),
            };
            state.stack.push_back(task.clone());
            json!({"ok":true,"task":task,"stack_depth":state.stack.len()})
        }
        "stack_push_batch" => push_batch(state, &args),
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

fn parse_stack_task(args: &Value) -> Result<StackTask, String> {
    let function_name = args
        .get("function_name")
        .and_then(Value::as_str)
        .unwrap_or_default()
        .trim()
        .to_string();
    if function_name.is_empty() {
        return Err("function_name is required".to_string());
    }

    let delay_ms = match args.get("delay_ms") {
        None => 0,
        Some(v) => v
            .as_u64()
            .ok_or_else(|| "delay_ms must be a non-negative integer".to_string())?,
    };

    let note = match args.get("note") {
        None | Some(Value::Null) => None,
        Some(v) => Some(
            v.as_str()
                .ok_or_else(|| "note must be a string".to_string())?
                .to_string(),
        ),
    };

    Ok(StackTask {
        id: Uuid::new_v4(),
        function_name,
        args: args.get("args").cloned().unwrap_or_else(|| json!({})),
        delay_ms,
        enqueued_at_ms: now_unix_ms(),
        note,
    })
}

fn push_batch(state: &mut AppState, args: &Value) -> Value {
    let Some(items) = args.get("items").and_then(Value::as_array) else {
        return json!({"ok":false,"error":"items is required and must be an array"});
    };

    if items.is_empty() {
        return json!({"ok":true,"pushed":0,"tasks":[],"stack_depth":state.stack.len()});
    }

    let mut parsed = Vec::with_capacity(items.len());
    for (idx, item) in items.iter().enumerate() {
        match parse_stack_task(item) {
            Ok(task) => parsed.push(task),
            Err(err) => {
                return json!({
                    "ok": false,
                    "error": format!("items[{idx}] invalid: {err}"),
                    "pushed": 0,
                    "stack_depth": state.stack.len()
                })
            }
        }
    }

    for task in &parsed {
        state.stack.push_back(task.clone());
    }

    json!({"ok":true,"pushed":parsed.len(),"tasks":parsed,"stack_depth":state.stack.len()})
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

fn arg_bool(args: &Value, key: &str) -> Result<Option<bool>, String> {
    match args.get(key) {
        None => Ok(None),
        Some(v) => v
            .as_bool()
            .map(Some)
            .ok_or_else(|| format!("{key} must be a boolean")),
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

fn stack_file_from_content(content: &str) -> Result<StackFile, String> {
    let stack_file: StackFile =
        serde_json::from_str(content).map_err(|err| format!("invalid stack json: {err}"))?;

    if stack_file.version != 1 {
        return Err(format!(
            "unsupported stack file version: {} (expected 1)",
            stack_file.version
        ));
    }

    Ok(stack_file)
}

fn validate_loaded_tasks(tasks: &[StackTask]) -> Result<(), String> {
    for (idx, task) in tasks.iter().enumerate() {
        if task.function_name.trim().is_empty() {
            return Err(format!("tasks[{idx}] has empty function_name"));
        }
    }
    Ok(())
}

fn pause_tasks_for_downtime(tasks: &mut [StackTask], saved_at_ms: u128, now_ms: u128) {
    let downtime_ms = now_ms.saturating_sub(saved_at_ms);
    if downtime_ms == 0 {
        return;
    }

    for task in tasks {
        task.enqueued_at_ms = task.enqueued_at_ms.saturating_add(downtime_ms);
    }
}

fn parent_dir(path: &Path) -> Option<&Path> {
    let parent = path.parent()?;
    if parent.as_os_str().is_empty() {
        return None;
    }
    Some(parent)
}

async fn save_stack(state: &AppState, args: &Value) -> Value {
    let path = stack_path_from_args(args);
    let path_buf = PathBuf::from(&path);

    if let Some(parent) = parent_dir(&path_buf) {
        if let Err(err) = fs::create_dir_all(parent).await {
            return json!({"ok":false,"error":format!("save failed creating parent dir: {err}")});
        }
    }

    let payload = StackFile {
        version: 1,
        saved_at_ms: now_unix_ms(),
        tasks: state.stack.iter().cloned().collect(),
    };

    let bytes = match serde_json::to_vec_pretty(&payload) {
        Ok(bytes) => bytes,
        Err(err) => return json!({"ok":false,"error":format!("serialize failed: {err}")}),
    };

    let tmp_path = format!("{path}.tmp-{}", Uuid::new_v4());
    if let Err(err) = fs::write(&tmp_path, bytes).await {
        return json!({"ok":false,"error":format!("save failed writing temp file: {err}")});
    }

    if let Err(err) = fs::rename(&tmp_path, &path).await {
        let _ = fs::remove_file(&tmp_path).await;
        return json!({"ok":false,"error":format!("save failed replacing target file: {err}")});
    }

    json!({"ok":true,"path":path,"saved":state.stack.len()})
}

async fn load_stack(state: &mut AppState, args: &Value) -> Value {
    let path = stack_path_from_args(args);
    let content = match fs::read_to_string(&path).await {
        Ok(s) => s,
        Err(err) => return json!({"ok":false,"error":format!("load failed: {err}")}),
    };

    let mut stack_file = match stack_file_from_content(&content) {
        Ok(file) => file,
        Err(err) => return json!({"ok":false,"error":err}),
    };

    if let Err(err) = validate_loaded_tasks(&stack_file.tasks) {
        return json!({"ok":false,"error":format!("invalid task payload: {err}")});
    }

    let mode = args
        .get("mode")
        .and_then(Value::as_str)
        .unwrap_or("replace");

    let pause_during_downtime = match arg_bool(args, "pause_during_downtime") {
        Ok(v) => v.unwrap_or(false),
        Err(err) => return json!({"ok":false,"error":err}),
    };

    if pause_during_downtime {
        pause_tasks_for_downtime(&mut stack_file.tasks, stack_file.saved_at_ms, now_unix_ms());
    }

    match mode {
        "replace" => {
            state.stack = VecDeque::from(stack_file.tasks);
            json!({"ok":true,"path":path,"loaded":state.stack.len(),"stack_depth":state.stack.len(),"mode":"replace","pause_during_downtime":pause_during_downtime})
        }
        "append" => {
            let dedupe_ids = match arg_bool(args, "dedupe_ids") {
                Ok(v) => v.unwrap_or(true),
                Err(err) => return json!({"ok":false,"error":err}),
            };

            let before = state.stack.len();
            let mut skipped = 0usize;

            for task in stack_file.tasks {
                if dedupe_ids && state.stack.iter().any(|existing| existing.id == task.id) {
                    skipped += 1;
                    continue;
                }
                state.stack.push_back(task);
            }

            let loaded = state.stack.len().saturating_sub(before);
            json!({
                "ok": true,
                "path": path,
                "mode": "append",
                "loaded": loaded,
                "skipped": skipped,
                "dedupe_ids": dedupe_ids,
                "pause_during_downtime": pause_during_downtime,
                "stack_depth": state.stack.len()
            })
        }
        _ => json!({"ok":false,"error":"mode must be one of: replace, append"}),
    }
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
    use tempfile::tempdir;

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

    #[test]
    fn push_batch_is_atomic_on_validation_error() {
        let mut state = AppState::default();
        state.stack.push_back(task_with_times(5, now_unix_ms()));

        let result = push_batch(
            &mut state,
            &json!({
                "items": [
                    {"function_name":"message.send","args":{"text":"ok"},"delay_ms":10},
                    {"function_name":"message.send","delay_ms":"bad"}
                ]
            }),
        );

        assert_eq!(result.get("ok").and_then(Value::as_bool), Some(false));
        assert_eq!(state.stack.len(), 1);
    }

    #[test]
    fn push_batch_pushes_all_items() {
        let mut state = AppState::default();
        let result = push_batch(
            &mut state,
            &json!({
                "items": [
                    {"function_name":"message.send","args":{"text":"one"},"delay_ms":10},
                    {"function_name":"message.send","args":{"text":"two"},"delay_ms":20,"note":"n"}
                ]
            }),
        );

        assert_eq!(result.get("ok").and_then(Value::as_bool), Some(true));
        assert_eq!(result.get("pushed").and_then(Value::as_u64), Some(2));
        assert_eq!(state.stack.len(), 2);
    }

    #[test]
    fn stack_file_parser_rejects_unsupported_version() {
        let content = json!({
            "version": 99,
            "saved_at_ms": 123,
            "tasks": []
        })
        .to_string();

        let err = stack_file_from_content(&content).unwrap_err();
        assert!(err.contains("unsupported stack file version"));
    }

    #[tokio::test]
    async fn save_stack_creates_parent_directory_and_is_loadable() {
        let tmp = tempdir().expect("temp dir");
        let nested_path = tmp.path().join("nested").join("stack.json");
        let nested_path_str = nested_path.to_string_lossy().to_string();

        let mut state = AppState::default();
        state.stack.push_back(task_with_times(500, now_unix_ms()));

        let save_result = save_stack(&state, &json!({"path": nested_path_str})).await;
        assert_eq!(save_result.get("ok").and_then(Value::as_bool), Some(true));
        assert!(nested_path.exists());

        let mut loaded_state = AppState::default();
        let load_result = load_stack(&mut loaded_state, &json!({"path": nested_path})).await;
        assert_eq!(load_result.get("ok").and_then(Value::as_bool), Some(true));
        assert_eq!(loaded_state.stack.len(), 1);
    }

    #[tokio::test]
    async fn load_stack_append_mode_dedupes_ids_by_default() {
        let tmp = tempdir().expect("temp dir");
        let path = tmp.path().join("stack.json");

        let shared_id = Uuid::new_v4();
        let existing = StackTask {
            id: shared_id,
            function_name: "message.send".to_string(),
            args: json!({"text":"existing"}),
            delay_ms: 0,
            enqueued_at_ms: now_unix_ms(),
            note: None,
        };
        let duplicate = StackTask {
            id: shared_id,
            function_name: "message.send".to_string(),
            args: json!({"text":"dup"}),
            delay_ms: 0,
            enqueued_at_ms: now_unix_ms(),
            note: None,
        };
        let unique = StackTask {
            id: Uuid::new_v4(),
            function_name: "message.send".to_string(),
            args: json!({"text":"unique"}),
            delay_ms: 0,
            enqueued_at_ms: now_unix_ms(),
            note: None,
        };

        let file_payload = StackFile {
            version: 1,
            saved_at_ms: now_unix_ms(),
            tasks: vec![duplicate, unique],
        };
        fs::write(
            &path,
            serde_json::to_vec_pretty(&file_payload).expect("serialize"),
        )
        .await
        .expect("write stack file");

        let mut state = AppState {
            stack: VecDeque::from(vec![existing]),
        };

        let result = load_stack(
            &mut state,
            &json!({"path": path.to_string_lossy().to_string(), "mode": "append"}),
        )
        .await;

        assert_eq!(result.get("ok").and_then(Value::as_bool), Some(true));
        assert_eq!(result.get("loaded").and_then(Value::as_u64), Some(1));
        assert_eq!(result.get("skipped").and_then(Value::as_u64), Some(1));
        assert_eq!(state.stack.len(), 2);
    }

    #[tokio::test]
    async fn load_stack_pause_during_downtime_preserves_remaining_delay() {
        let tmp = tempdir().expect("temp dir");
        let path = tmp.path().join("stack.json");

        let now = now_unix_ms();
        let saved_at_ms = now.saturating_sub(5_000);
        let task = StackTask {
            id: Uuid::new_v4(),
            function_name: "message.send".to_string(),
            args: json!({"text":"paused"}),
            delay_ms: 10_000,
            enqueued_at_ms: now.saturating_sub(7_000),
            note: None,
        };

        let file_payload = StackFile {
            version: 1,
            saved_at_ms,
            tasks: vec![task],
        };
        fs::write(
            &path,
            serde_json::to_vec_pretty(&file_payload).expect("serialize"),
        )
        .await
        .expect("write stack file");

        let mut state = AppState::default();
        let result = load_stack(
            &mut state,
            &json!({"path": path.to_string_lossy().to_string(), "pause_during_downtime": true}),
        )
        .await;

        assert_eq!(result.get("ok").and_then(Value::as_bool), Some(true));
        assert_eq!(state.stack.len(), 1);

        let loaded = state.stack.front().expect("loaded task");
        let remaining = remaining_delay_ms(loaded, now_unix_ms());
        assert!(remaining <= 8_200, "remaining too high: {remaining}");
        assert!(remaining >= 7_700, "remaining too low: {remaining}");
    }

    #[tokio::test]
    async fn load_stack_rejects_invalid_boolean_options() {
        let tmp = tempdir().expect("temp dir");
        let path = tmp.path().join("stack.json");

        let file_payload = StackFile {
            version: 1,
            saved_at_ms: now_unix_ms(),
            tasks: vec![],
        };
        fs::write(
            &path,
            serde_json::to_vec_pretty(&file_payload).expect("serialize"),
        )
        .await
        .expect("write stack file");

        let mut state = AppState::default();
        let result = load_stack(
            &mut state,
            &json!({"path": path.to_string_lossy().to_string(), "mode": "append", "dedupe_ids": "yes"}),
        )
        .await;

        assert_eq!(result.get("ok").and_then(Value::as_bool), Some(false));
        assert!(result
            .get("error")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .contains("dedupe_ids must be a boolean"));
    }

    #[tokio::test]
    async fn load_stack_rejects_empty_function_name() {
        let tmp = tempdir().expect("temp dir");
        let path = tmp.path().join("invalid-stack.json");

        let file_payload = StackFile {
            version: 1,
            saved_at_ms: now_unix_ms(),
            tasks: vec![StackTask {
                id: Uuid::new_v4(),
                function_name: "   ".to_string(),
                args: json!({}),
                delay_ms: 0,
                enqueued_at_ms: now_unix_ms(),
                note: None,
            }],
        };
        fs::write(
            &path,
            serde_json::to_vec_pretty(&file_payload).expect("serialize"),
        )
        .await
        .expect("write stack file");

        let mut state = AppState::default();
        let result = load_stack(
            &mut state,
            &json!({"path": path.to_string_lossy().to_string()}),
        )
        .await;

        assert_eq!(result.get("ok").and_then(Value::as_bool), Some(false));
        assert!(result
            .get("error")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .contains("empty function_name"));
        assert!(state.stack.is_empty());
    }
}
