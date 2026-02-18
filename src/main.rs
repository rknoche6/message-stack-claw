use anyhow::Result;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::collections::VecDeque;
use tokio::io::{self, AsyncBufReadExt, AsyncWriteExt, BufReader};
use tracing::info;
use uuid::Uuid;

#[derive(Debug, Clone, Serialize, Deserialize)]
struct StackTask {
    id: Uuid,
    function_name: String,
    args: Value,
    delay_ms: u64,
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
            {"name":"stack_run_next","description":"Pop top call, wait delay_ms, then execute/simulate"},
            {"name":"stack_run_all","description":"Run queued calls from top to bottom"},
            {"name":"stack_clear","description":"Clear all queued calls"}
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
                note: args
                    .get("note")
                    .and_then(Value::as_str)
                    .map(ToString::to_string),
            };
            state.stack.push_back(task.clone());
            json!({"ok":true,"task":task,"stack_depth":state.stack.len()})
        }
        "stack_list" => json!({"ok":true,"stack_depth":state.stack.len(),"tasks":state.stack}),
        "stack_run_next" => run_next(state).await,
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
        _ => json!({"ok":false,"error":format!("unknown tool: {tool}")}),
    }
}

async fn run_next(state: &mut AppState) -> Value {
    let Some(task) = state.stack.pop_back() else {
        return json!({"ok":false,"error":"stack is empty"});
    };

    if task.delay_ms > 0 {
        tokio::time::sleep(std::time::Duration::from_millis(task.delay_ms)).await;
    }

    // Useful default behavior for OpenClaw orchestrations:
    // return a normalized execution payload that another agent/tool can consume.
    json!({
      "ok": true,
      "executed": {
        "id": task.id,
        "function_name": task.function_name,
        "args": task.args,
        "note": task.note,
        "delay_ms": task.delay_ms,
        "status": "executed"
      },
      "stack_depth": state.stack.len()
    })
}

async fn write_response(stdout: &mut io::Stdout, response: RpcResponse) -> Result<()> {
    let out = serde_json::to_string(&response)?;
    stdout.write_all(out.as_bytes()).await?;
    stdout.write_all(b"\n").await?;
    stdout.flush().await?;
    Ok(())
}
