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
- `stack_list()`
- `stack_run_next()`
- `stack_run_all()`
- `stack_clear()`

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
{"id":1,"method":"tools/list"}
```

Push a task:

```json
{"id":2,"method":"tools/call","params":{"name":"stack_push","arguments":{"function_name":"message.send","args":{"target":"ops","text":"Ping"},"delay_ms":1500,"note":"retry #1"}}}
```

Run next queued task:

```json
{"id":3,"method":"tools/call","params":{"name":"stack_run_next","arguments":{}}}
```

## Notes

- The stack is in-memory for now (resets when process restarts).
- `stack_run_next` waits `delay_ms` before returning execution payload.
- If you need persistence, add a file-backed store in a follow-up.
