use anyhow::Result;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::collections::{HashMap, HashSet, VecDeque};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use tokio::fs;
use tokio::io::{self, AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::signal;
use tracing::{info, warn};
use uuid::Uuid;

#[derive(Debug, Clone, Serialize, Deserialize)]
struct StackTask {
    id: Uuid,
    function_name: String,
    args: Value,
    delay_ms: u64,
    enqueued_at_ms: u128,
    note: Option<String>,
    dedupe_key: Option<String>,
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

#[derive(Debug, Clone, Default)]
struct ToolMetrics {
    call_count: u64,
    total_duration_ms: u64,
    error_count: u64,
}

#[derive(Debug, Clone, Default)]
struct AppMetrics {
    requests_total: u64,
    tools: HashMap<String, ToolMetrics>,
    started_at_ms: u128,
}

impl AppMetrics {
    fn new() -> Self {
        Self {
            started_at_ms: now_unix_ms(),
            ..Default::default()
        }
    }

    fn record_tool_call(&mut self, tool_name: &str, duration_ms: u64, is_error: bool) {
        self.requests_total += 1;
        let entry = self.tools.entry(tool_name.to_string()).or_default();
        entry.call_count += 1;
        entry.total_duration_ms += duration_ms;
        if is_error {
            entry.error_count += 1;
        }
    }

    fn uptime_ms(&self) -> u128 {
        now_unix_ms().saturating_sub(self.started_at_ms)
    }

    fn summary(&self) -> Value {
        let tools: HashMap<String, Value> = self
            .tools
            .iter()
            .map(|(name, m)| {
                let avg_ms = if m.call_count > 0 {
                    m.total_duration_ms / m.call_count
                } else {
                    0
                };
                (
                    name.clone(),
                    json!({
                        "call_count": m.call_count,
                        "error_count": m.error_count,
                        "avg_duration_ms": avg_ms,
                        "total_duration_ms": m.total_duration_ms,
                    }),
                )
            })
            .collect();

        json!({
            "requests_total": self.requests_total,
            "uptime_ms": self.uptime_ms(),
            "tools": tools,
        })
    }
}

#[derive(Default)]
struct AppState {
    stack: VecDeque<StackTask>,
    metrics: AppMetrics,
}

const MAX_STACK_DEPTH: usize = 10_000;
const MAX_BATCH_ITEMS: usize = 1_000;
const MAX_FUNCTION_NAME_LEN: usize = 256;
const MAX_NOTE_LEN: usize = 4_096;
const MAX_DEDUPE_KEY_LEN: usize = 512;
const MAX_ARGS_JSON_BYTES: usize = 256 * 1024;
const DEFAULT_SHUTDOWN_SAVE_PATH: &str = ".message-stack-claw.shutdown.json";

/// Guard that auto-saves the stack on graceful shutdown.
/// Explicitly dropped before exit to ensure persistence.
#[derive(Debug)]
struct ShutdownGuard {
    enabled: bool,
    path: PathBuf,
}

impl ShutdownGuard {
    fn new(path: impl Into<PathBuf>) -> Self {
        Self {
            enabled: true,
            path: path.into(),
        }
    }

    fn disable(&mut self) {
        self.enabled = false;
    }

    async fn save(&self, state: &AppState) -> Result<(), String> {
        if state.stack.is_empty() {
            return Ok(());
        }
        let payload = StackFile {
            version: 1,
            saved_at_ms: now_unix_ms(),
            tasks: state.stack.iter().cloned().collect(),
        };
        let bytes = serde_json::to_vec_pretty(&payload)
            .map_err(|err| format!("serialize failed: {err}"))?;

        if let Some(parent) = self.path.parent() {
            tokio::fs::create_dir_all(parent)
                .await
                .map_err(|err| format!("shutdown save: failed creating parent dir: {err}"))?;
        }
        let tmp_path = format!("{}.tmp-shutdown-{}", self.path.display(), Uuid::new_v4());
        tokio::fs::write(&tmp_path, bytes)
            .await
            .map_err(|err| format!("shutdown save: failed writing temp file: {err}"))?;
        tokio::fs::rename(&tmp_path, &self.path)
            .await
            .map_err(|err| {
                let _ = tokio::fs::remove_file(&tmp_path);
                format!("shutdown save: failed renaming temp file: {err}")
            })?;
        info!(path = %self.path.display(), saved = state.stack.len(), "Stack auto-saved on shutdown");
        Ok(())
    }
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

    let shutdown_path = std::env::var("STACK_SHUTDOWN_PATH")
        .unwrap_or_else(|_| DEFAULT_SHUTDOWN_SAVE_PATH.to_string());
    let mut guard = ShutdownGuard::new(&shutdown_path);

    let stdin = BufReader::new(io::stdin());
    let mut lines = stdin.lines();
    let mut stdout = io::stdout();
    let mut state = AppState {
        stack: VecDeque::default(),
        metrics: AppMetrics::new(),
    };

    info!("message-stack-claw MCP server started");
    info!(shutdown_path = %shutdown_path, "Shutdown auto-save enabled");

    // Attempt to recover from previous unclean shutdown
    if let Ok(content) = fs::read_to_string(&shutdown_path).await {
        match stack_file_from_content(&content) {
            Ok(stack_file) if !stack_file.tasks.is_empty() => {
                warn!(tasks = stack_file.tasks.len(), path = %shutdown_path, "Recovered tasks from unclean shutdown");
                for task in stack_file.tasks {
                    if state.stack.len() < MAX_STACK_DEPTH {
                        state.stack.push_back(task);
                    }
                }
                // Delete the recovery file after loading
                let _ = fs::remove_file(&shutdown_path).await;
            }
            _ => {}
        }
    }

    let shutdown_requested = Arc::new(AtomicBool::new(false));
    let shutdown_clone = shutdown_requested.clone();

    tokio::spawn(async move {
        let mut sigint = signal::unix::signal(signal::unix::SignalKind::interrupt())
            .expect("Failed to create SIGINT handler");
        let mut sigterm = signal::unix::signal(signal::unix::SignalKind::terminate())
            .expect("Failed to create SIGTERM handler");

        tokio::select! {
            _ = sigint.recv() => {},
            _ = sigterm.recv() => {},
        }
        info!("Shutdown signal received, initiating graceful shutdown...");
        shutdown_clone.store(true, Ordering::SeqCst);
    });

    while let Some(line) = lines.next_line().await? {
        if shutdown_requested.load(Ordering::SeqCst) {
            info!("Shutdown requested, completing current request...");
            break;
        }

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

    // Graceful shutdown: auto-save stack before exit
    if guard.enabled && !state.stack.is_empty() {
        if let Err(err) = guard.save(&state).await {
            warn!(error = %err, "Failed to auto-save stack on shutdown");
        }
    }
    guard.disable();

    info!("message-stack-claw MCP server shutdown complete");
    Ok(())
}

async fn handle_request(state: &mut AppState, req: &RpcRequest) -> Value {
    match req.method.as_str() {
        "tools/list" => json!({
          "ok": true,
          "tools": [
            {"name":"stack_push","description":"Push a deferred function call onto a LIFO stack","input_schema":{"function_name":"string","args":"object|any","delay_ms":"number","note":"string?","dedupe_key":"string?","on_duplicate":"skip|replace|error? (default skip when dedupe_key provided)"}},
            {"name":"stack_push_batch","description":"Push multiple deferred function calls atomically (all-or-nothing, max 1000 items)","input_schema":{"items":"array<{function_name,args?,delay_ms?,note?,dedupe_key?}>","on_duplicate":"skip|replace|error? (default skip when any dedupe_key provided)"}},
            {"name":"stack_list","description":"List queued deferred calls"},
            {"name":"stack_run_next","description":"Pop top call, wait only remaining delay (based on enqueue time), then execute/simulate"},
            {"name":"stack_run_due","description":"Run due calls in batch without waiting","input_schema":{"limit":"number?","grace_ms":"number?"}},
            {"name":"stack_run_all","description":"Run queued calls from top to bottom"},
            {"name":"stack_cancel","description":"Cancel queued calls by id, dedupe_key, or function_name","input_schema":{"id":"uuid?","dedupe_key":"string?","function_name":"string?","limit":"number? (default all matches)"}},
            {"name":"stack_clear","description":"Clear all queued calls"},
            {"name":"stack_save","description":"Persist current stack to disk","input_schema":{"path":"string?"}},
            {"name":"stack_load","description":"Load stack from disk (replace or append)","input_schema":{"path":"string?","mode":"replace|append?","dedupe_ids":"boolean? (default true when append)","pause_during_downtime":"boolean? (default false; preserves remaining delays across save→load downtime)"}},
            {"name":"health_check","description":"Health check and server metrics","input_schema":{}}
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

    let start_ms = now_unix_ms();
    let result = call_tool_inner(state, tool, &args).await;
    let duration_ms = (now_unix_ms() - start_ms) as u64;
    let is_error = result.get("ok").and_then(Value::as_bool) == Some(false);
    state.metrics.record_tool_call(tool, duration_ms, is_error);
    result
}

async fn call_tool_inner(state: &mut AppState, tool: &str, args: &Value) -> Value {
    match tool {
        "stack_push" => {
            let task = match parse_stack_task(&args) {
                Ok(task) => task,
                Err(err) => return json!({"ok":false,"error":err}),
            };

            if let Some(dedupe_key) = task.dedupe_key.as_deref() {
                let on_duplicate = match parse_on_duplicate(&args) {
                    Ok(mode) => mode,
                    Err(err) => return json!({"ok":false,"error":err}),
                };

                if let Some(existing_idx) = state
                    .stack
                    .iter()
                    .position(|existing| existing.dedupe_key.as_deref() == Some(dedupe_key))
                {
                    let existing_task = state.stack.get(existing_idx).cloned();
                    match on_duplicate {
                        OnDuplicate::Skip => {
                            return json!({
                                "ok": true,
                                "inserted": false,
                                "reason": "duplicate_dedupe_key",
                                "dedupe_key": dedupe_key,
                                "existing_task": existing_task,
                                "stack_depth": state.stack.len()
                            });
                        }
                        OnDuplicate::Error => {
                            return json!({
                                "ok": false,
                                "error": format!("task with dedupe_key '{dedupe_key}' already exists"),
                                "dedupe_key": dedupe_key,
                                "existing_task": existing_task,
                                "stack_depth": state.stack.len()
                            });
                        }
                        OnDuplicate::Replace => {
                            state.stack.remove(existing_idx);
                        }
                    }
                }
            }
            if let Err(err) = ensure_stack_capacity(state.stack.len(), 1) {
                return json!({"ok":false,"error":err,"stack_depth":state.stack.len()});
            }
            state.stack.push_back(task.clone());
            json!({"ok":true,"inserted":true,"task":task,"stack_depth":state.stack.len()})
        }
        "stack_push_batch" => push_batch(state, args),
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
                        "dedupe_key": task.dedupe_key,
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
        "stack_cancel" => cancel_tasks(state, &args),
        "stack_clear" => {
            let cleared = state.stack.len();
            state.stack.clear();
            json!({"ok":true,"cleared":cleared})
        }
        "stack_save" => save_stack(state, &args).await,
        "stack_load" => load_stack(state, &args).await,
        "health_check" => json!({
            "ok": true,
            "status": "healthy",
            "stack_depth": state.stack.len(),
            "metrics": state.metrics.summary()
        }),
        _ => json!({"ok":false,"error":format!("unknown tool: {tool}")}),
    }
}

fn json_size_bytes(value: &Value) -> Result<usize, String> {
    serde_json::to_vec(value)
        .map(|bytes| bytes.len())
        .map_err(|err| format!("failed to serialize args: {err}"))
}

fn validate_task_fields(
    function_name: &str,
    note: Option<&str>,
    dedupe_key: Option<&str>,
    args_value: &Value,
) -> Result<(), String> {
    if function_name.is_empty() {
        return Err("function_name is required".to_string());
    }

    if function_name.len() > MAX_FUNCTION_NAME_LEN {
        return Err(format!(
            "function_name too long: {} bytes (max {MAX_FUNCTION_NAME_LEN})",
            function_name.len()
        ));
    }

    if let Some(note) = note {
        if note.len() > MAX_NOTE_LEN {
            return Err(format!(
                "note too long: {} bytes (max {MAX_NOTE_LEN})",
                note.len()
            ));
        }
    }

    if let Some(dedupe_key) = dedupe_key {
        if dedupe_key.len() > MAX_DEDUPE_KEY_LEN {
            return Err(format!(
                "dedupe_key too long: {} bytes (max {MAX_DEDUPE_KEY_LEN})",
                dedupe_key.len()
            ));
        }
    }

    let args_size = json_size_bytes(args_value)?;
    if args_size > MAX_ARGS_JSON_BYTES {
        return Err(format!(
            "args payload too large: {args_size} bytes (max {MAX_ARGS_JSON_BYTES})"
        ));
    }

    Ok(())
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

    let dedupe_key = match args.get("dedupe_key") {
        None | Some(Value::Null) => None,
        Some(v) => {
            let key = v
                .as_str()
                .ok_or_else(|| "dedupe_key must be a string".to_string())?
                .trim()
                .to_string();

            if key.is_empty() {
                return Err("dedupe_key must not be empty when provided".to_string());
            }

            Some(key)
        }
    };

    let args_value = args.get("args").cloned().unwrap_or_else(|| json!({}));

    validate_task_fields(
        &function_name,
        note.as_deref(),
        dedupe_key.as_deref(),
        &args_value,
    )?;

    Ok(StackTask {
        id: Uuid::new_v4(),
        function_name,
        args: args_value,
        delay_ms,
        enqueued_at_ms: now_unix_ms(),
        note,
        dedupe_key,
    })
}

#[derive(Debug, Clone, Copy)]
enum OnDuplicate {
    Skip,
    Replace,
    Error,
}

fn parse_on_duplicate(args: &Value) -> Result<OnDuplicate, String> {
    let mode = args
        .get("on_duplicate")
        .and_then(Value::as_str)
        .unwrap_or("skip");

    match mode {
        "skip" => Ok(OnDuplicate::Skip),
        "replace" => Ok(OnDuplicate::Replace),
        "error" => Ok(OnDuplicate::Error),
        _ => Err("on_duplicate must be one of: skip, replace, error".to_string()),
    }
}

fn push_batch(state: &mut AppState, args: &Value) -> Value {
    let Some(items) = args.get("items").and_then(Value::as_array) else {
        return json!({"ok":false,"error":"items is required and must be an array"});
    };

    if items.is_empty() {
        return json!({"ok":true,"pushed":0,"tasks":[],"stack_depth":state.stack.len()});
    }

    if items.len() > MAX_BATCH_ITEMS {
        return json!({
            "ok": false,
            "error": format!("batch too large: {} items (max {MAX_BATCH_ITEMS})", items.len()),
            "pushed": 0,
            "stack_depth": state.stack.len()
        });
    }

    let on_duplicate = match parse_on_duplicate(args) {
        Ok(mode) => mode,
        Err(err) => {
            return json!({"ok":false,"error":err,"pushed":0,"stack_depth":state.stack.len()})
        }
    };

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

    let existing_dedupe_keys: HashSet<String> = state
        .stack
        .iter()
        .filter_map(|task| task.dedupe_key.clone())
        .collect();

    let mut tasks_to_insert = Vec::with_capacity(parsed.len());
    let mut insert_positions: HashMap<String, usize> = HashMap::new();
    let mut keys_to_replace_existing: HashSet<String> = HashSet::new();

    let mut skipped = 0usize;
    let mut replaced_existing = 0usize;
    let mut replaced_in_batch = 0usize;

    for task in parsed {
        let Some(dedupe_key) = task.dedupe_key.clone() else {
            tasks_to_insert.push(task);
            continue;
        };

        if let Some(existing_insert_pos) = insert_positions.get(&dedupe_key).copied() {
            match on_duplicate {
                OnDuplicate::Skip => {
                    skipped += 1;
                }
                OnDuplicate::Error => {
                    return json!({
                        "ok": false,
                        "error": format!("batch contains duplicate dedupe_key '{dedupe_key}'"),
                        "dedupe_key": dedupe_key,
                        "pushed": 0,
                        "stack_depth": state.stack.len()
                    });
                }
                OnDuplicate::Replace => {
                    tasks_to_insert[existing_insert_pos] = task;
                    replaced_in_batch += 1;
                }
            }
            continue;
        }

        if existing_dedupe_keys.contains(&dedupe_key) {
            match on_duplicate {
                OnDuplicate::Skip => {
                    skipped += 1;
                }
                OnDuplicate::Error => {
                    return json!({
                        "ok": false,
                        "error": format!("task with dedupe_key '{dedupe_key}' already exists"),
                        "dedupe_key": dedupe_key,
                        "pushed": 0,
                        "stack_depth": state.stack.len()
                    });
                }
                OnDuplicate::Replace => {
                    tasks_to_insert.push(task);
                    insert_positions.insert(dedupe_key.clone(), tasks_to_insert.len() - 1);
                    if keys_to_replace_existing.insert(dedupe_key) {
                        replaced_existing += 1;
                    }
                }
            }
            continue;
        }

        tasks_to_insert.push(task);
        insert_positions.insert(dedupe_key, tasks_to_insert.len() - 1);
    }

    let replaced_existing_tasks = if keys_to_replace_existing.is_empty() {
        0
    } else {
        state
            .stack
            .iter()
            .filter(|task| {
                task.dedupe_key
                    .as_ref()
                    .map(|key| keys_to_replace_existing.contains(key))
                    .unwrap_or(false)
            })
            .count()
    };

    let projected_depth = state
        .stack
        .len()
        .saturating_sub(replaced_existing_tasks)
        .saturating_add(tasks_to_insert.len());
    if projected_depth > MAX_STACK_DEPTH {
        return json!({
            "ok": false,
            "error": format!("stack depth limit exceeded: projected {projected_depth}, max {MAX_STACK_DEPTH}"),
            "pushed": 0,
            "stack_depth": state.stack.len()
        });
    }

    if !keys_to_replace_existing.is_empty() {
        state.stack.retain(|task| {
            task.dedupe_key
                .as_ref()
                .map(|key| !keys_to_replace_existing.contains(key))
                .unwrap_or(true)
        });
    }

    for task in &tasks_to_insert {
        state.stack.push_back(task.clone());
    }

    json!({
        "ok":true,
        "pushed":tasks_to_insert.len(),
        "tasks":tasks_to_insert,
        "skipped":skipped,
        "replaced_existing":replaced_existing,
        "replaced_in_batch":replaced_in_batch,
        "stack_depth":state.stack.len()
    })
}

fn executed_payload(task: StackTask, status: &str) -> Value {
    json!({
      "id": task.id,
      "function_name": task.function_name,
      "args": task.args,
      "note": task.note,
      "dedupe_key": task.dedupe_key,
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

fn ensure_stack_capacity(current: usize, additional: usize) -> Result<(), String> {
    let Some(projected) = current.checked_add(additional) else {
        return Err("stack capacity overflow".to_string());
    };

    if projected > MAX_STACK_DEPTH {
        return Err(format!(
            "stack depth limit exceeded: projected {projected}, max {MAX_STACK_DEPTH}"
        ));
    }

    Ok(())
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

fn cancel_tasks(state: &mut AppState, args: &Value) -> Value {
    let id = match args.get("id") {
        None => None,
        Some(v) => {
            let raw = match v.as_str() {
                Some(raw) => raw,
                None => return json!({"ok":false,"error":"id must be a uuid string"}),
            };

            match Uuid::parse_str(raw) {
                Ok(id) => Some(id),
                Err(err) => {
                    return json!({"ok":false,"error":format!("invalid id: {err}")});
                }
            }
        }
    };

    let dedupe_key = match args.get("dedupe_key") {
        None => None,
        Some(v) => {
            let key = match v.as_str() {
                Some(s) => s.trim(),
                None => return json!({"ok":false,"error":"dedupe_key must be a string"}),
            };

            if key.is_empty() {
                return json!({"ok":false,"error":"dedupe_key must not be empty"});
            }

            Some(key.to_string())
        }
    };

    let function_name = match args.get("function_name") {
        None => None,
        Some(v) => {
            let name = match v.as_str() {
                Some(s) => s.trim(),
                None => return json!({"ok":false,"error":"function_name must be a string"}),
            };

            if name.is_empty() {
                return json!({"ok":false,"error":"function_name must not be empty"});
            }

            Some(name.to_string())
        }
    };

    if id.is_none() && dedupe_key.is_none() && function_name.is_none() {
        return json!({
            "ok":false,
            "error":"one selector is required: id, dedupe_key, or function_name"
        });
    }

    let limit = match arg_u64(args, "limit") {
        Ok(v) => v,
        Err(err) => return json!({"ok":false,"error":err}),
    };

    if limit == Some(0) {
        return json!({"ok":true,"canceled":0,"tasks":[],"stack_depth":state.stack.len(),"reason":"limit is 0"});
    }

    let mut canceled = Vec::new();
    let mut remaining = VecDeque::with_capacity(state.stack.len());

    for task in state.stack.drain(..) {
        let matches_id = id.map(|value| task.id == value).unwrap_or(false);
        let matches_dedupe = dedupe_key
            .as_deref()
            .map(|value| task.dedupe_key.as_deref() == Some(value))
            .unwrap_or(false);
        let matches_function = function_name
            .as_deref()
            .map(|value| task.function_name == value)
            .unwrap_or(false);

        let is_match = matches_id || matches_dedupe || matches_function;
        let under_limit = limit
            .map(|max| (canceled.len() as u64) < max)
            .unwrap_or(true);

        if is_match && under_limit {
            canceled.push(task);
        } else {
            remaining.push_back(task);
        }
    }

    state.stack = remaining;

    json!({
        "ok": true,
        "canceled": canceled.len(),
        "tasks": canceled,
        "selectors": {
            "id": id,
            "dedupe_key": dedupe_key,
            "function_name": function_name
        },
        "limit": limit,
        "stack_depth": state.stack.len()
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
        let function_name = task.function_name.trim();
        validate_task_fields(
            function_name,
            task.note.as_deref(),
            task.dedupe_key.as_deref(),
            &task.args,
        )
        .map_err(|err| format!("tasks[{idx}] invalid: {err}"))?;
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
            if stack_file.tasks.len() > MAX_STACK_DEPTH {
                return json!({
                    "ok": false,
                    "error": format!("stack depth limit exceeded: file has {} tasks, max {MAX_STACK_DEPTH}", stack_file.tasks.len())
                });
            }

            state.stack = VecDeque::from(stack_file.tasks);
            json!({"ok":true,"path":path,"loaded":state.stack.len(),"stack_depth":state.stack.len(),"mode":"replace","pause_during_downtime":pause_during_downtime})
        }
        "append" => {
            let dedupe_ids = match arg_bool(args, "dedupe_ids") {
                Ok(v) => v.unwrap_or(true),
                Err(err) => return json!({"ok":false,"error":err}),
            };

            let before = state.stack.len();
            if let Err(err) = ensure_stack_capacity(before, stack_file.tasks.len()) {
                return json!({"ok":false,"error":err,"mode":"append","stack_depth":state.stack.len()});
            }

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
            dedupe_key: None,
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
            ..Default::default()
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
            ..Default::default()
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
            ..Default::default()
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
    fn push_batch_skips_existing_dedupe_keys_by_default() {
        let mut state = AppState::default();
        state.stack.push_back(StackTask {
            id: Uuid::new_v4(),
            function_name: "message.send".to_string(),
            args: json!({"text":"existing"}),
            delay_ms: 0,
            enqueued_at_ms: now_unix_ms(),
            note: None,
            dedupe_key: Some("dup-1".to_string()),
        });

        let result = push_batch(
            &mut state,
            &json!({
                "items": [
                    {"function_name":"message.send","args":{"text":"new-1"},"dedupe_key":"dup-1"},
                    {"function_name":"message.send","args":{"text":"new-2"},"dedupe_key":"dup-2"}
                ]
            }),
        );

        assert_eq!(result.get("ok").and_then(Value::as_bool), Some(true));
        assert_eq!(result.get("pushed").and_then(Value::as_u64), Some(1));
        assert_eq!(result.get("skipped").and_then(Value::as_u64), Some(1));
        assert_eq!(state.stack.len(), 2);
    }

    #[test]
    fn push_batch_replace_mode_replaces_existing_and_batch_duplicates() {
        let mut state = AppState::default();
        state.stack.push_back(StackTask {
            id: Uuid::new_v4(),
            function_name: "message.send".to_string(),
            args: json!({"text":"old"}),
            delay_ms: 0,
            enqueued_at_ms: now_unix_ms(),
            note: None,
            dedupe_key: Some("job-1".to_string()),
        });

        let result = push_batch(
            &mut state,
            &json!({
                "on_duplicate": "replace",
                "items": [
                    {"function_name":"message.send","args":{"text":"new-1"},"dedupe_key":"job-1"},
                    {"function_name":"message.send","args":{"text":"first"},"dedupe_key":"job-2"},
                    {"function_name":"message.send","args":{"text":"second"},"dedupe_key":"job-2"}
                ]
            }),
        );

        assert_eq!(result.get("ok").and_then(Value::as_bool), Some(true));
        assert_eq!(result.get("pushed").and_then(Value::as_u64), Some(2));
        assert_eq!(
            result.get("replaced_existing").and_then(Value::as_u64),
            Some(1)
        );
        assert_eq!(
            result.get("replaced_in_batch").and_then(Value::as_u64),
            Some(1)
        );
        assert_eq!(state.stack.len(), 2);

        let job_2 = state
            .stack
            .iter()
            .find(|task| task.dedupe_key.as_deref() == Some("job-2"))
            .expect("job-2 task");
        assert_eq!(
            job_2.args.get("text").and_then(Value::as_str),
            Some("second")
        );
    }

    #[test]
    fn push_batch_error_mode_rejects_existing_duplicate_without_mutation() {
        let mut state = AppState::default();
        state.stack.push_back(StackTask {
            id: Uuid::new_v4(),
            function_name: "message.send".to_string(),
            args: json!({"text":"old"}),
            delay_ms: 0,
            enqueued_at_ms: now_unix_ms(),
            note: None,
            dedupe_key: Some("job-1".to_string()),
        });

        let result = push_batch(
            &mut state,
            &json!({
                "on_duplicate": "error",
                "items": [
                    {"function_name":"message.send","args":{"text":"new"},"dedupe_key":"job-1"}
                ]
            }),
        );

        assert_eq!(result.get("ok").and_then(Value::as_bool), Some(false));
        assert_eq!(state.stack.len(), 1);
        assert_eq!(
            state.stack[0].args.get("text").and_then(Value::as_str),
            Some("old")
        );
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
            dedupe_key: None,
        };
        let duplicate = StackTask {
            id: shared_id,
            function_name: "message.send".to_string(),
            args: json!({"text":"dup"}),
            delay_ms: 0,
            enqueued_at_ms: now_unix_ms(),
            note: None,
            dedupe_key: None,
        };
        let unique = StackTask {
            id: Uuid::new_v4(),
            function_name: "message.send".to_string(),
            args: json!({"text":"unique"}),
            delay_ms: 0,
            enqueued_at_ms: now_unix_ms(),
            note: None,
            dedupe_key: None,
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
            ..Default::default()
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
            dedupe_key: None,
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
                dedupe_key: None,
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
            .contains("function_name is required"));
        assert!(state.stack.is_empty());
    }

    #[tokio::test]
    async fn stack_push_rejects_oversized_args_payload() {
        let mut state = AppState::default();
        let oversized = "x".repeat(MAX_ARGS_JSON_BYTES + 1);

        let result = call_tool(
            &mut state,
            &json!({
                "name": "stack_push",
                "arguments": {
                    "function_name": "message.send",
                    "args": {"blob": oversized}
                }
            }),
        )
        .await;

        assert_eq!(result.get("ok").and_then(Value::as_bool), Some(false));
        assert!(result
            .get("error")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .contains("args payload too large"));
    }

    #[tokio::test]
    async fn load_stack_rejects_oversized_args_payload() {
        let tmp = tempdir().expect("temp dir");
        let path = tmp.path().join("invalid-stack.json");

        let oversized = "x".repeat(MAX_ARGS_JSON_BYTES + 1);
        let file_payload = StackFile {
            version: 1,
            saved_at_ms: now_unix_ms(),
            tasks: vec![StackTask {
                id: Uuid::new_v4(),
                function_name: "message.send".to_string(),
                args: json!({"blob": oversized}),
                delay_ms: 0,
                enqueued_at_ms: now_unix_ms(),
                note: None,
                dedupe_key: None,
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
            .contains("args payload too large"));
    }

    #[test]
    fn push_batch_rejects_batches_over_limit() {
        let mut state = AppState::default();
        let items: Vec<Value> = (0..(MAX_BATCH_ITEMS + 1))
            .map(|i| json!({"function_name":"message.send","args":{"index":i}}))
            .collect();

        let result = push_batch(&mut state, &json!({"items": items}));
        assert_eq!(result.get("ok").and_then(Value::as_bool), Some(false));
        assert!(result
            .get("error")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .contains("batch too large"));
    }

    #[tokio::test]
    async fn stack_push_rejects_when_stack_is_at_capacity() {
        let mut state = AppState::default();
        for _ in 0..MAX_STACK_DEPTH {
            state.stack.push_back(task_with_times(0, now_unix_ms()));
        }

        let result = call_tool(
            &mut state,
            &json!({
                "name": "stack_push",
                "arguments": {"function_name": "message.send", "args": {"text": "extra"}}
            }),
        )
        .await;

        assert_eq!(result.get("ok").and_then(Value::as_bool), Some(false));
        assert!(result
            .get("error")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .contains("stack depth limit exceeded"));
    }

    #[tokio::test]
    async fn stack_push_with_dedupe_key_skips_duplicate_by_default() {
        let mut state = AppState::default();

        let first = call_tool(
            &mut state,
            &json!({
                "name": "stack_push",
                "arguments": {"function_name": "message.send", "dedupe_key": "welcome-1"}
            }),
        )
        .await;
        assert_eq!(first.get("ok").and_then(Value::as_bool), Some(true));
        assert_eq!(first.get("inserted").and_then(Value::as_bool), Some(true));

        let second = call_tool(
            &mut state,
            &json!({
                "name": "stack_push",
                "arguments": {"function_name": "message.send", "dedupe_key": "welcome-1"}
            }),
        )
        .await;

        assert_eq!(second.get("ok").and_then(Value::as_bool), Some(true));
        assert_eq!(second.get("inserted").and_then(Value::as_bool), Some(false));
        assert_eq!(state.stack.len(), 1);
    }

    #[tokio::test]
    async fn stack_push_with_dedupe_key_replace_mode_replaces_existing_task() {
        let mut state = AppState::default();

        let _ = call_tool(
            &mut state,
            &json!({
                "name": "stack_push",
                "arguments": {"function_name": "message.send", "args": {"text": "old"}, "dedupe_key": "job-1"}
            }),
        )
        .await;

        let replaced = call_tool(
            &mut state,
            &json!({
                "name": "stack_push",
                "arguments": {"function_name": "message.send", "args": {"text": "new"}, "dedupe_key": "job-1", "on_duplicate": "replace"}
            }),
        )
        .await;

        assert_eq!(replaced.get("ok").and_then(Value::as_bool), Some(true));
        assert_eq!(
            replaced.get("inserted").and_then(Value::as_bool),
            Some(true)
        );
        assert_eq!(state.stack.len(), 1);

        let task = state.stack.back().expect("task");
        assert_eq!(task.args.get("text").and_then(Value::as_str), Some("new"));
    }

    #[tokio::test]
    async fn stack_cancel_removes_matching_dedupe_key() {
        let mut state = AppState::default();
        let t1 = StackTask {
            id: Uuid::new_v4(),
            function_name: "message.send".to_string(),
            args: json!({"text":"a"}),
            delay_ms: 0,
            enqueued_at_ms: now_unix_ms(),
            note: None,
            dedupe_key: Some("k-1".to_string()),
        };
        let t2 = StackTask {
            id: Uuid::new_v4(),
            function_name: "message.send".to_string(),
            args: json!({"text":"b"}),
            delay_ms: 0,
            enqueued_at_ms: now_unix_ms(),
            note: None,
            dedupe_key: Some("k-2".to_string()),
        };
        state.stack.push_back(t1);
        state.stack.push_back(t2);

        let result = call_tool(
            &mut state,
            &json!({
                "name":"stack_cancel",
                "arguments":{"dedupe_key":"k-1"}
            }),
        )
        .await;

        assert_eq!(result.get("ok").and_then(Value::as_bool), Some(true));
        assert_eq!(result.get("canceled").and_then(Value::as_u64), Some(1));
        assert_eq!(state.stack.len(), 1);
        assert_eq!(state.stack[0].dedupe_key.as_deref(), Some("k-2"));
    }

    #[tokio::test]
    async fn stack_cancel_respects_limit() {
        let mut state = AppState::default();
        state.stack.push_back(task_with_times(0, now_unix_ms()));
        state.stack.push_back(task_with_times(0, now_unix_ms()));
        state.stack.push_back(task_with_times(0, now_unix_ms()));

        let result = call_tool(
            &mut state,
            &json!({
                "name":"stack_cancel",
                "arguments":{"function_name":"message.send","limit":2}
            }),
        )
        .await;

        assert_eq!(result.get("ok").and_then(Value::as_bool), Some(true));
        assert_eq!(result.get("canceled").and_then(Value::as_u64), Some(2));
        assert_eq!(state.stack.len(), 1);
    }

    #[tokio::test]
    async fn stack_cancel_requires_selector() {
        let mut state = AppState::default();
        state.stack.push_back(task_with_times(0, now_unix_ms()));

        let result = call_tool(
            &mut state,
            &json!({
                "name":"stack_cancel",
                "arguments":{}
            }),
        )
        .await;

        assert_eq!(result.get("ok").and_then(Value::as_bool), Some(false));
        assert_eq!(state.stack.len(), 1);
    }

    #[tokio::test]
    async fn stack_list_includes_dedupe_key() {
        let mut state = AppState::default();
        let dedupe_key = "order-123".to_string();
        state.stack.push_back(StackTask {
            id: Uuid::new_v4(),
            function_name: "message.send".to_string(),
            args: json!({"text":"hello"}),
            delay_ms: 0,
            enqueued_at_ms: now_unix_ms(),
            note: Some("note".to_string()),
            dedupe_key: Some(dedupe_key.clone()),
        });

        let result = call_tool(
            &mut state,
            &json!({
                "name":"stack_list",
                "arguments":{}
            }),
        )
        .await;

        assert_eq!(result.get("ok").and_then(Value::as_bool), Some(true));
        let tasks = result
            .get("tasks")
            .and_then(Value::as_array)
            .expect("tasks array");
        assert_eq!(tasks.len(), 1);
        assert_eq!(
            tasks[0].get("dedupe_key").and_then(Value::as_str),
            Some(dedupe_key.as_str())
        );
    }

    #[tokio::test]
    async fn health_check_returns_ok_and_metrics() {
        let mut state = AppState {
            stack: VecDeque::from(vec![task_with_times(0, now_unix_ms())]),
            ..Default::default()
        };

        let result = call_tool(
            &mut state,
            &json!({
                "name":"health_check",
                "arguments":{}
            }),
        )
        .await;

        assert_eq!(result.get("ok").and_then(Value::as_bool), Some(true));
        assert_eq!(
            result.get("status").and_then(Value::as_str),
            Some("healthy")
        );
        assert_eq!(result.get("stack_depth").and_then(Value::as_u64), Some(1));
        assert!(result.get("metrics").is_some());

        let metrics = result.get("metrics").expect("metrics object");
        assert!(metrics.get("uptime_ms").is_some());
        assert!(metrics.get("requests_total").is_some());
    }

    #[tokio::test]
    async fn metrics_track_tool_calls() {
        let mut state = AppState::default();

        // Make some tool calls
        let _ = call_tool(
            &mut state,
            &json!({
                "name":"stack_push",
                "arguments":{"function_name": "test.call"}
            }),
        )
        .await;

        let _ = call_tool(
            &mut state,
            &json!({
                "name":"stack_list",
                "arguments":{}
            }),
        )
        .await;

        // Check health for metrics
        let result = call_tool(
            &mut state,
            &json!({
                "name":"health_check",
                "arguments":{}
            }),
        )
        .await;

        let metrics = result.get("metrics").expect("metrics object");
        // Note: health_check hasn't recorded itself yet (recording happens after return)
        assert_eq!(
            metrics.get("requests_total").and_then(Value::as_u64),
            Some(2)
        ); // push + list

        let tools = metrics
            .get("tools")
            .and_then(Value::as_object)
            .expect("tools object");
        assert!(tools.contains_key("stack_push"));
        assert!(tools.contains_key("stack_list"));
        // health_check not yet in tools since metrics are captured before it records itself

        let push_metrics = tools.get("stack_push").expect("push metrics");
        assert_eq!(
            push_metrics.get("call_count").and_then(Value::as_u64),
            Some(1)
        );
    }

    #[tokio::test]
    async fn metrics_track_errors() {
        let mut state = AppState::default();

        // Make an invalid tool call that will error
        let _ = call_tool(
            &mut state,
            &json!({
                "name":"stack_cancel",
                "arguments":{} // missing required selector
            }),
        )
        .await;

        let result = call_tool(
            &mut state,
            &json!({
                "name":"health_check",
                "arguments":{}
            }),
        )
        .await;

        let metrics = result.get("metrics").expect("metrics object");
        let tools = metrics
            .get("tools")
            .and_then(Value::as_object)
            .expect("tools object");
        let cancel_metrics = tools.get("stack_cancel").expect("cancel metrics");
        assert_eq!(
            cancel_metrics.get("error_count").and_then(Value::as_u64),
            Some(1)
        );
    }

    #[tokio::test]
    async fn shutdown_guard_saves_stack_to_disk() {
        let tmp = tempdir().expect("temp dir");
        let path = tmp.path().join("shutdown.json");

        let mut state = AppState::default();
        state.stack.push_back(task_with_times(100, now_unix_ms()));
        state.stack.push_back(task_with_times(200, now_unix_ms()));

        let guard = ShutdownGuard::new(&path);
        guard.save(&state).await.expect("save should succeed");

        assert!(path.exists());

        // Verify the saved content can be loaded
        let content = fs::read_to_string(&path).await.expect("read saved file");
        let stack_file = stack_file_from_content(&content).expect("parse saved file");
        assert_eq!(stack_file.tasks.len(), 2);
    }

    #[tokio::test]
    async fn shutdown_guard_skips_empty_stack() {
        let tmp = tempdir().expect("temp dir");
        let path = tmp.path().join("shutdown.json");

        let state = AppState::default();
        let guard = ShutdownGuard::new(&path);

        // Should succeed without creating file for empty stack
        guard.save(&state).await.expect("save should succeed");
        assert!(!path.exists());
    }

    #[tokio::test]
    async fn shutdown_guard_creates_parent_directories() {
        let tmp = tempdir().expect("temp dir");
        let nested_path = tmp.path().join("nested").join("shutdown.json");

        let mut state = AppState::default();
        state.stack.push_back(task_with_times(100, now_unix_ms()));

        let guard = ShutdownGuard::new(&nested_path);
        guard.save(&state).await.expect("save should succeed");

        assert!(nested_path.exists());
    }
}
