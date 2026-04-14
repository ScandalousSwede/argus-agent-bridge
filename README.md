# argus-agent-bridge

A lightweight multi-agent message bus with capability registration, WebSocket push delivery, disk-backed message persistence, and human oversight relay via Discord.

Built as Layer 2 of a 3-tier agent coordination stack. Provides bidirectional text messaging between AI agents running on separate machines over LAN — designed to be always-on alongside a 24/7 desktop AI agent.

## Why This Exists

When two AI agents need to coordinate — one on a laptop (an IDE AI assistant), one on a desktop (an autonomous monitoring agent) — they need a communication channel that doesn't depend on either agent being in the same process, on the same machine, or even running at the same time. This bridge provides that channel: a persistent, human-monitorable message bus where every message is logged and optionally relayed to a human oversight channel.

## Architecture

```
Laptop (Cascade / IDE AI)          Desktop (Argus / autonomous agent)
        │                                         │
   MCP Server                            agent_bridge_client.py
        │                                         │
        └──────────── HTTP / WebSocket ───────────┘
                              │
                    ┌─────────────────┐
                    │  Bridge Server  │
                    │  (this repo)    │
                    │                 │
                    │  /bridge/send   │
                    │  /bridge/recv   │
                    │  /bridge/ws     │──── WebSocket push
                    │  /bridge/agents │
                    └────────┬────────┘
                             │
                    Discord webhook relay
                    (human oversight channel)
```

## Features

**For agents:**
- HTTP polling (`GET /bridge/messages`) and WebSocket push (`/bridge/ws`) — agents choose their delivery model
- Capability registration: agents declare what they can do when joining
- Message routing: send to a specific agent by ID, or broadcast to all
- Message history with configurable retention
- Reply threading (`reply_to` field)

**For reliability:**
- Disk-backed state: agent registrations, inboxes, and message history flushed to disk every 30s and on shutdown — survives restarts
- Graceful SIGTERM/SIGINT with state flush before exit
- 5-minute agent TTL with WebSocket keepalive; stale agents auto-deregistered
- Uncaught exception handler preserves state before crash

**For human oversight:**
- Discord webhook relay: every message mirrored to a configurable channel in real time — humans can observe inter-agent communication without intercepting it
- Bridge status endpoint shows all registered agents, queue depths, and uptime

## Components

### `server.js` — Node.js bridge server
- HTTP + WebSocket server (no external dependencies beyond `ws`)
- Runs on the desktop as 24/7 infrastructure
- Binds `0.0.0.0` for LAN access

### `agent_bridge_client.py` — Python SDK
Zero-dependency Python client (stdlib only). Drop into any Python agent codebase.

```python
from agent_bridge_client import AgentBridgeClient

client = AgentBridgeClient(
    bridge_url="http://desktop-hostname:3018",
    agent_id="my-agent",
    api_key="optional"
)
client.register(capabilities=["reasoning", "file_access"])
client.send_message(to="other-agent", content="Task complete: files processed.")
messages = client.read_messages()
```

Async WebSocket variant also included (`AgentBridgeClientAsync`) for push-based delivery.

**Poll loop helper:**
```python
def on_message(msg):
    print(f"[{msg['from']}] {msg['content']}")

client.poll_loop(handler=on_message, interval=2.0)
```

## API Reference

| Method | Path | Description |
|--------|------|-------------|
| GET | `/health` | Health check |
| GET | `/bridge/status` | All agents, queue depths, uptime |
| POST | `/bridge/register` | Register agent with capabilities |
| POST | `/bridge/send` | Send message to agent or broadcast |
| GET | `/bridge/messages` | Poll inbox (supports `since`, `mark_read`) |
| POST | `/bridge/messages/clear` | Clear inbox |
| GET | `/bridge/agents` | List registered agents |
| GET | `/bridge/history` | Message history with filters |
| WS | `/bridge/ws` | WebSocket push connection |

## Setup

**Server (Node.js):**
```bash
npm install
node server.js
```

**Client (Python):**
```bash
pip install websockets  # only needed for async WebSocket variant
```

Environment variables (server):
```
BRIDGE_PORT=3018
BRIDGE_API_KEY=<shared-secret>          # optional
BRIDGE_DISCORD_WEBHOOK=<webhook-url>    # optional: human oversight relay
BRIDGE_STATE_FILE=./bridge-state.json   # disk persistence path
```

## Part of a Larger System

This repo is Layer 2 of a 3-layer coordination stack:

- **Layer 1 ([argus-coordination-api](../argus-coordination-api)):** Mutex locking, safety interlocks, audit trail
- **Layer 2 (this repo):** Bidirectional text messaging with human oversight
- **Layer 3:** Direct tool use (MCP server, hardware API)

The full system coordinates a laptop-side AI coding assistant with a desktop-side autonomous AI agent, both of which can issue commands to a physical 6-DOF robot arm.

## Design Notes

The decision to relay all inter-agent messages to a Discord channel was deliberate — it provides a lightweight human oversight channel without requiring humans to actively monitor the system. Messages flow at the speed agents need; humans can review the log asynchronously. This is a practical implementation of the "humans in the loop without blocking the loop" pattern.

The disk-backed persistence design means agent state survives the bridge server crashing or restarting. The 30s flush interval was chosen as a balance between disk I/O and acceptable data loss on sudden crash.
