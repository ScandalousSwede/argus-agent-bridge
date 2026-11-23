# Argus Agent Bridge

Two independent processes can register their capabilities, send each other a message and read it later. The bridge exposes HTTP polling and WebSocket push. Its server stores registrations, queued messages and message history on disk.

## Try a disposable bridge

From the repository root on Linux, macOS or WSL, use Node.js 18+ and Python 3.9+. Only the Node server needs an installed package.

```console
npm install --ignore-scripts
python3 demo_local.py
```

The example starts the real server on an unused loopback port with temporary state. Two instances of the included Python client register as a producer and reviewer. The producer sends one message. After a graceful shutdown and restart, the reviewer reads the same message, checks its identifier in history and confirms the inbox is empty on a second read. Each run uses a fresh directory and leaves the existing bridge alone.

Expected output includes `Restored 2 registrations` and `Inbox now empty`. The message ID varies by run. The example uses HTTP polling and disables optional external relays.

## Components

- [Server](server.js) implements HTTP endpoints, WebSocket push and periodic disk flushes.
- [Python client](agent_bridge_client.py) supports synchronous HTTP calls with the standard library. Its optional asynchronous WebSocket path needs the `websockets` package.
- [Disposable example](demo_local.py) exercises registration, sending, consuming and reopening persisted state.

| Operation | Endpoint |
| --- | --- |
| Register capabilities or list agents | `POST /bridge/register` and `GET /bridge/agents` |
| Send and read a message | `POST /bridge/send` and `GET /bridge/messages` |
| Inspect retained history | `GET /bridge/history` |
| Inspect service state | `GET /health` and `GET /bridge/status` |
| Receive push messages | `/bridge/ws?agent=<id>` |

## Runtime configuration

`BRIDGE_HOST` and `BRIDGE_PORT` select the listener. `BRIDGE_STATE_DIR` selects the directory for the three state files. The standalone server defaults to port 3018 and all interfaces. The example selects loopback explicitly. The optional `BRIDGE_API_KEY` applies to HTTP requests. External Discord and OpenClaw relay routes are separately configurable and unnecessary for this demonstration.

The server flushes persisted state every 30 seconds and during graceful shutdown. A sudden process or machine failure can lose recent unflushed changes. The example verifies recovery after a graceful restart.
