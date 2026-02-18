# message-stack-claw

Rust MCP-style server for deferred stack-based function calls.

## Why this is useful for OpenClaw

For OpenClaw workflows, you often want to queue actions and execute them later in a controlled order.
This server gives you a simple **LIFO stack** with per-task delay.

Examples:

- queue follow-up notifications and execute them with spacing
- stage retries/backoff actions as deferred calls
- queue tool calls during one turn and drain them in a later turn

## Tools exposed

- `stack_push(function_name, args, delay_ms, note?)`
- `stack_push_batch(items[])` (atomic all-or-nothing batch enqueue)
- `stack_list()`
- `stack_run_next()`
- `stack_run_due(limit?, grace_ms?)`
- `stack_run_all()`
- `stack_clear()`
- `stack_save(path?)`
- `stack_load(path?, mode?, dedupe_ids?, pause_during_downtime?)`

## Protocol

JSON-RPC-like over stdio:

- `tools/list`
- `tools/call`

Each line in stdin should be one JSON request, and each response is one JSON line.

## Run locally

```bash
cargo run
```

## Example requests

List tools:

```json
{ "id": 1, "method": "tools/list" }
```

Push a task:

```json
{
  "id": 2,
  "method": "tools/call",
  "params": {
    "name": "stack_push",
    "arguments": {
      "function_name": "message.send",
      "args": { "target": "ops", "text": "Ping" },
      "delay_ms": 1500,
      "note": "retry #1"
    }
  }
}
```

Run next queued task:

```json
{ "id": 3, "method": "tools/call", "params": { "name": "stack_run_next", "arguments": {} } }
```

Batch enqueue multiple tasks atomically:

```json
{
  "id": 31,
  "method": "tools/call",
  "params": {
    "name": "stack_push_batch",
    "arguments": {
      "items": [
        { "function_name": "message.send", "args": { "text": "step 1" }, "delay_ms": 0 },
        { "function_name": "message.send", "args": { "text": "step 2" }, "delay_ms": 500 }
      ]
    }
  }
}
```

Batch-run due tasks, max 10 at a time, including tasks due within 250ms:

```json
{
  "id": 4,
  "method": "tools/call",
  "params": {
    "name": "stack_run_due",
    "arguments": { "limit": 10, "grace_ms": 250 }
  }
}
```

Append from a saved stack file while skipping duplicate task IDs:

```json
{
  "id": 5,
  "method": "tools/call",
  "params": {
    "name": "stack_load",
    "arguments": { "path": ".message-stack-claw.stack.json", "mode": "append", "dedupe_ids": true }
  }
}
```

Restore from disk while preserving remaining delay windows (ignore downtime between save and load):

```json
{
  "id": 6,
  "method": "tools/call",
  "params": {
    "name": "stack_load",
    "arguments": {
      "path": ".message-stack-claw.stack.json",
      "mode": "replace",
      "pause_during_downtime": true
    }
  }
}
```

## Notes

- Stack is in-memory by default, but you can persist/restore via `stack_save` and `stack_load`.
- Default persistence path is `.message-stack-claw.stack.json` (override with `path`).
- `stack_save` writes atomically (temp file + rename) and creates parent directories when needed.
- `stack_load` validates the persisted file format version and fails fast on incompatible versions.
- `stack_load` also validates task payloads and rejects invalid entries (for example empty `function_name`).
- `stack_load` supports `mode`:
  - `replace` (default): replace in-memory stack with file contents
  - `append`: append file contents to current stack
- In `append` mode, `dedupe_ids` defaults to `true` to avoid duplicate task IDs when reloading snapshots.
- `stack_load` now validates boolean option types (`dedupe_ids`, `pause_during_downtime`) and fails fast on invalid payloads.
- `pause_during_downtime: true` on `stack_load` preserves each task's remaining delay across process downtime (useful when restoring from disk after restarts).
- `stack_run_next` waits only the **remaining** delay (delay counted from enqueue time).
- `stack_push_batch` validates the full batch first and only enqueues when all items are valid (prevents partial queue updates).
- `stack_run_due` executes due tasks without waiting and supports optional batching controls:
  - `limit`: maximum number of tasks to execute in one call
  - `grace_ms`: include near-due tasks whose remaining delay is within this window
- `stack_list` includes `remaining_delay_ms` for each task.
