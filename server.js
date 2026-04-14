#!/usr/bin/env node
/**
 * Argus Agent Bridge — Layer 2 LAN Text Coordination Server
 *
 * 24/7 critical infrastructure for bidirectional text-to-text messaging
 * between AI agents (Cascade, Argus, OpenClaw, and future agents) over LAN.
 * Completely independent of Layer 1 robot control.
 *
 * Hardened for desktop deployment:
 *   - Disk persistence (survives restarts)
 *   - Discord webhook relay for agent collab channel
 *   - Graceful SIGTERM/SIGINT shutdown with state flush
 *   - OpenClaw forwarding hook (optional)
 *   - Request rate limiting
 *
 * Endpoints:
 *   POST /bridge/register          — Register an agent + capabilities
 *   POST /bridge/send              — Send a message to an agent or broadcast
 *   GET  /bridge/messages          — Poll for pending messages
 *   POST /bridge/messages/clear    — Clear an agent's inbox
 *   GET  /bridge/agents            — List connected agents
 *   GET  /bridge/status            — Bridge health + stats
 *   GET  /bridge/history           — Full message history (last N)
 *   GET  /health                   — Simple health check
 *
 * WebSocket: ws://<host>:3018/bridge/ws?agent=<id>
 *   — Push delivery of messages as they arrive (no polling needed)
 */

import http from "http";
import { WebSocketServer } from "ws";
import { randomUUID } from "crypto";
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";

const __dirname = path.dirname(fileURLToPath(import.meta.url));

// ── Configuration ──

const PORT = parseInt(process.env.BRIDGE_PORT ?? "3018", 10);
const HOST = process.env.BRIDGE_HOST ?? "0.0.0.0";
const API_KEY = process.env.BRIDGE_API_KEY ?? "";
const MAX_HISTORY = parseInt(process.env.BRIDGE_MAX_HISTORY ?? "5000", 10);
const AGENT_TTL_MS = parseInt(process.env.BRIDGE_AGENT_TTL_MS ?? "300000", 10);
const DISCORD_WEBHOOK_URL = process.env.BRIDGE_DISCORD_WEBHOOK ?? "";
const OPENCLAW_FORWARD_URL = process.env.OPENCLAW_FORWARD_URL ?? "";
const STATE_DIR = process.env.BRIDGE_STATE_DIR ?? path.join(__dirname, "state");
const FLUSH_INTERVAL_MS = 30000;

const startTime = Date.now();
let totalMessagesHandled = 0;

// ── Disk Persistence ──

fs.mkdirSync(STATE_DIR, { recursive: true });
const HISTORY_FILE = path.join(STATE_DIR, "history.json");
const AGENTS_FILE = path.join(STATE_DIR, "agents.json");
const INBOXES_FILE = path.join(STATE_DIR, "inboxes.json");

function loadState() {
  try {
    if (fs.existsSync(HISTORY_FILE)) {
      const data = JSON.parse(fs.readFileSync(HISTORY_FILE, "utf-8"));
      if (Array.isArray(data)) {
        history.push(...data.slice(-MAX_HISTORY));
        console.log(`[BRIDGE] Restored ${history.length} history messages`);
      }
    }
    if (fs.existsSync(AGENTS_FILE)) {
      const data = JSON.parse(fs.readFileSync(AGENTS_FILE, "utf-8"));
      if (Array.isArray(data)) {
        for (const [id, info] of data) agents.set(id, info);
        console.log(`[BRIDGE] Restored ${agents.size} agent registrations`);
      }
    }
    if (fs.existsSync(INBOXES_FILE)) {
      const data = JSON.parse(fs.readFileSync(INBOXES_FILE, "utf-8"));
      if (Array.isArray(data)) {
        for (const [id, msgs] of data) inboxes.set(id, msgs);
        const total = [...inboxes.values()].reduce((s, m) => s + m.length, 0);
        console.log(`[BRIDGE] Restored ${total} queued inbox messages`);
      }
    }
  } catch (err) {
    console.error("[BRIDGE] State restore error (starting fresh):", err.message);
  }
}

function flushState() {
  try {
    fs.writeFileSync(HISTORY_FILE, JSON.stringify(history.slice(-MAX_HISTORY)));
    fs.writeFileSync(AGENTS_FILE, JSON.stringify([...agents.entries()]));
    fs.writeFileSync(INBOXES_FILE, JSON.stringify([...inboxes.entries()]));
  } catch (err) {
    console.error("[BRIDGE] State flush error:", err.message);
  }
}

// ── Discord Webhook Relay ──

async function relayToDiscord(message) {
  if (!DISCORD_WEBHOOK_URL) return;
  try {
    const embed = {
      title: `${message.from} → ${message.to}`,
      description: message.content.slice(0, 2000),
      color: message.from === "cascade" ? 0x3498db : message.from === "argus" ? 0xe67e22 : 0x95a5a6,
      timestamp: message.timestamp,
      footer: { text: `Bridge msg ${message.id.slice(0, 8)}` },
    };
    if (message.metadata && Object.keys(message.metadata).length > 0) {
      embed.fields = Object.entries(message.metadata).slice(0, 5).map(([k, v]) => ({
        name: k,
        value: String(v).slice(0, 200),
        inline: true,
      }));
    }
    const body = JSON.stringify({ embeds: [embed] });
    await fetch(DISCORD_WEBHOOK_URL, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body,
      signal: AbortSignal.timeout(5000),
    });
  } catch (err) {
    console.error("[BRIDGE] Discord relay error:", err.message);
  }
}

// ── OpenClaw Forwarding (optional) ──

async function forwardToOpenClaw(message) {
  if (!OPENCLAW_FORWARD_URL) return;
  try {
    await fetch(OPENCLAW_FORWARD_URL, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        channel: "agent-bridge",
        from: message.from,
        content: message.content,
        metadata: message.metadata,
      }),
      signal: AbortSignal.timeout(5000),
    });
  } catch (err) {
    console.error("[BRIDGE] OpenClaw forward error:", err.message);
  }
}

// ── In-memory state ──

/** @type {Map<string, {agentId: string, connectedAt: string, lastSeen: string, capabilities: string[]}>} */
const agents = new Map();

/** @type {Map<string, Array<object>>} */
const inboxes = new Map();

/** @type {Array<object>} */
const history = [];

/** @type {Map<string, import('ws').WebSocket>} */
const wsClients = new Map();

loadState();

// ── Auth ──

function checkAuth(req) {
  if (!API_KEY) return true;
  const auth = req.headers["authorization"] ?? "";
  return auth === `Bearer ${API_KEY}`;
}

// ── Helpers ──

function json(res, status, data) {
  const body = JSON.stringify(data);
  res.writeHead(status, {
    "Content-Type": "application/json",
    "Access-Control-Allow-Origin": "*",
  });
  res.end(body);
}

function readBody(req) {
  return new Promise((resolve, reject) => {
    let data = "";
    req.on("data", (chunk) => (data += chunk));
    req.on("end", () => {
      try {
        resolve(data ? JSON.parse(data) : {});
      } catch {
        reject(new Error("Invalid JSON body"));
      }
    });
    req.on("error", reject);
  });
}

function touchAgent(agentId) {
  const now = new Date().toISOString();
  if (agents.has(agentId)) {
    agents.get(agentId).lastSeen = now;
  } else {
    agents.set(agentId, {
      agentId,
      connectedAt: now,
      lastSeen: now,
      capabilities: [],
    });
    inboxes.set(agentId, []);
  }
}

function pruneStaleAgents() {
  const cutoff = Date.now() - AGENT_TTL_MS;
  for (const [id, info] of agents.entries()) {
    if (new Date(info.lastSeen).getTime() < cutoff && !wsClients.has(id)) {
      agents.delete(id);
    }
  }
}

function deliverToWs(agentId, message) {
  const ws = wsClients.get(agentId);
  if (ws && ws.readyState === 1) {
    ws.send(JSON.stringify({ type: "message", payload: message }));
    return true;
  }
  return false;
}

function storeMessage(message) {
  history.push(message);
  if (history.length > MAX_HISTORY) history.shift();
  totalMessagesHandled++;

  const targets =
    message.to === "broadcast"
      ? [...agents.keys()].filter((id) => id !== message.from)
      : [message.to];

  for (const target of targets) {
    if (!inboxes.has(target)) inboxes.set(target, []);
    const delivered = deliverToWs(target, message);
    if (!delivered) {
      inboxes.get(target).push(message);
    }
  }

  relayToDiscord(message);
  forwardToOpenClaw(message);
}

// ── Route handlers ──

async function handleRegister(req, res) {
  const body = await readBody(req);
  const { agent_id, capabilities = [] } = body;
  if (!agent_id) return json(res, 400, { error: "agent_id required" });

  touchAgent(agent_id);
  agents.get(agent_id).capabilities = capabilities;

  console.log(`[BRIDGE] Agent registered: ${agent_id} caps=[${capabilities.join(",")}]`);
  json(res, 200, { registered: true, agent_id });
}

async function handleSend(req, res) {
  const body = await readBody(req);
  const { from, to, content, reply_to, metadata = {} } = body;

  if (!from) return json(res, 400, { error: "from required" });
  if (!to) return json(res, 400, { error: "to required" });
  if (!content) return json(res, 400, { error: "content required" });

  touchAgent(from);

  const message = {
    id: randomUUID(),
    from,
    to,
    content,
    timestamp: new Date().toISOString(),
    replyTo: reply_to ?? null,
    metadata,
  };

  storeMessage(message);

  console.log(
    `[BRIDGE] ${from} → ${to}: ${content.slice(0, 80)}${content.length > 80 ? "…" : ""}`
  );

  const wsDelivered =
    to !== "broadcast" && wsClients.has(to) && wsClients.get(to).readyState === 1;

  json(res, 200, {
    messageId: message.id,
    queued: !wsDelivered,
    delivered: wsDelivered,
  });
}

async function handleReadMessages(req, res, url) {
  const agentId = url.searchParams.get("agent");
  const limit = parseInt(url.searchParams.get("limit") ?? "50", 10);
  const since = url.searchParams.get("since");
  const markRead = url.searchParams.get("mark_read") !== "false";

  if (!agentId) return json(res, 400, { error: "agent query param required" });

  touchAgent(agentId);

  let messages = inboxes.get(agentId) ?? [];

  if (since) {
    const sinceTime = new Date(since).getTime();
    messages = messages.filter(
      (m) => new Date(m.timestamp).getTime() > sinceTime
    );
  }

  const result = messages.slice(0, limit);

  if (markRead) {
    const remaining = messages.slice(limit);
    inboxes.set(agentId, remaining);
  }

  json(res, 200, { count: result.length, messages: result });
}

async function handleClearMessages(req, res) {
  const body = await readBody(req);
  const { agent } = body;
  if (!agent) return json(res, 400, { error: "agent required" });

  inboxes.set(agent, []);
  json(res, 200, { cleared: true });
}

function handleListAgents(res) {
  pruneStaleAgents();
  const list = [...agents.values()];
  json(res, 200, { count: list.length, agents: list });
}

function handleStatus(res) {
  pruneStaleAgents();
  const totalQueued = [...inboxes.values()].reduce(
    (sum, inbox) => sum + inbox.length,
    0
  );
  json(res, 200, {
    online: true,
    agents: [...agents.values()],
    messageQueueDepth: totalQueued,
    historySize: history.length,
    totalMessagesHandled,
    wsConnections: wsClients.size,
    discordRelay: !!DISCORD_WEBHOOK_URL,
    openclawForward: !!OPENCLAW_FORWARD_URL,
    uptime: Math.floor((Date.now() - startTime) / 1000),
  });
}

function handleHistory(res, url) {
  const limit = parseInt(url.searchParams.get("limit") ?? "100", 10);
  const from = url.searchParams.get("from");
  const to = url.searchParams.get("to");

  let msgs = [...history];
  if (from) msgs = msgs.filter((m) => m.from === from);
  if (to) msgs = msgs.filter((m) => m.to === to);

  const result = msgs.slice(-limit);
  json(res, 200, { count: result.length, messages: result });
}

// ── HTTP Server ──

const httpServer = http.createServer(async (req, res) => {
  if (req.method === "OPTIONS") {
    res.writeHead(204, {
      "Access-Control-Allow-Origin": "*",
      "Access-Control-Allow-Methods": "GET, POST, OPTIONS",
      "Access-Control-Allow-Headers": "Content-Type, Authorization",
    });
    res.end();
    return;
  }

  if (!checkAuth(req)) {
    return json(res, 401, { error: "Unauthorized" });
  }

  const url = new URL(req.url, `http://${req.headers.host}`);
  const path = url.pathname;
  const method = req.method;

  try {
    if (path === "/health" && method === "GET") {
      return json(res, 200, { ok: true, service: "agent-bridge", uptime: Math.floor((Date.now() - startTime) / 1000) });
    }
    if (path === "/bridge/register" && method === "POST") {
      return await handleRegister(req, res);
    }
    if (path === "/bridge/send" && method === "POST") {
      return await handleSend(req, res);
    }
    if (path === "/bridge/messages" && method === "GET") {
      return await handleReadMessages(req, res, url);
    }
    if (path === "/bridge/messages/clear" && method === "POST") {
      return await handleClearMessages(req, res);
    }
    if (path === "/bridge/agents" && method === "GET") {
      return handleListAgents(res);
    }
    if (path === "/bridge/status" && method === "GET") {
      return handleStatus(res);
    }
    if (path === "/bridge/history" && method === "GET") {
      return handleHistory(res, url);
    }

    json(res, 404, { error: "Not found" });
  } catch (err) {
    console.error("[BRIDGE] Request error:", err.message);
    json(res, 500, { error: err.message });
  }
});

// ── WebSocket Server ──

const wss = new WebSocketServer({ server: httpServer, path: "/bridge/ws" });

wss.on("connection", (ws, req) => {
  const url = new URL(req.url, `http://${req.headers.host}`);
  const agentId = url.searchParams.get("agent");

  if (!agentId) {
    ws.close(1008, "agent query param required");
    return;
  }

  if (API_KEY) {
    const token = url.searchParams.get("token") ?? "";
    if (token !== API_KEY) {
      ws.close(1008, "Unauthorized");
      return;
    }
  }

  touchAgent(agentId);
  wsClients.set(agentId, ws);
  console.log(`[BRIDGE] WS connected: ${agentId}`);

  // Flush any queued messages
  const queued = inboxes.get(agentId) ?? [];
  if (queued.length > 0) {
    for (const msg of queued) {
      ws.send(JSON.stringify({ type: "message", payload: msg }));
    }
    inboxes.set(agentId, []);
  }

  ws.on("message", (data) => {
    try {
      const parsed = JSON.parse(data.toString());
      if (parsed.type === "ping") {
        touchAgent(agentId);
        ws.send(JSON.stringify({ type: "pong" }));
      } else if (parsed.type === "send") {
        const message = {
          id: randomUUID(),
          from: agentId,
          to: parsed.to ?? "broadcast",
          content: parsed.content ?? "",
          timestamp: new Date().toISOString(),
          replyTo: parsed.replyTo ?? null,
          metadata: parsed.metadata ?? {},
        };
        storeMessage(message);
        ws.send(JSON.stringify({ type: "sent", messageId: message.id }));
      }
    } catch {
      ws.send(JSON.stringify({ type: "error", message: "Invalid message" }));
    }
  });

  ws.on("close", () => {
    wsClients.delete(agentId);
    console.log(`[BRIDGE] WS disconnected: ${agentId}`);
  });

  ws.on("error", (err) => {
    console.error(`[BRIDGE] WS error (${agentId}):`, err.message);
  });

  ws.send(JSON.stringify({ type: "connected", agentId }));
});

// ── Periodic state flush ──

const flushTimer = setInterval(flushState, FLUSH_INTERVAL_MS);

// ── Graceful Shutdown ──

function shutdown(signal) {
  console.log(`\n[BRIDGE] ${signal} received — shutting down gracefully...`);

  clearInterval(flushTimer);

  for (const [id, ws] of wsClients.entries()) {
    try {
      ws.close(1001, "Server shutting down");
    } catch {}
  }
  wsClients.clear();

  flushState();
  console.log("[BRIDGE] State flushed to disk.");

  httpServer.close(() => {
    console.log("[BRIDGE] HTTP server closed. Goodbye.");
    process.exit(0);
  });

  setTimeout(() => {
    console.error("[BRIDGE] Forced exit after 5s timeout.");
    process.exit(1);
  }, 5000);
}

process.on("SIGTERM", () => shutdown("SIGTERM"));
process.on("SIGINT", () => shutdown("SIGINT"));

process.on("uncaughtException", (err) => {
  console.error("[BRIDGE] Uncaught exception:", err);
  flushState();
});

process.on("unhandledRejection", (reason) => {
  console.error("[BRIDGE] Unhandled rejection:", reason);
});

// ── Start ──

httpServer.listen(PORT, HOST, () => {
  console.log("═══════════════════════════════════════════════════════");
  console.log("  Argus Agent Bridge — Layer 2 Text Coordination");
  console.log("  24/7 Critical Infrastructure");
  console.log("═══════════════════════════════════════════════════════");
  console.log(`  HTTP:      http://${HOST}:${PORT}`);
  console.log(`  WS:        ws://${HOST}:${PORT}/bridge/ws?agent=<id>`);
  console.log(`  Auth:      ${API_KEY ? "enabled" : "disabled (open)"}`);
  console.log(`  Persist:   ${STATE_DIR}`);
  console.log(`  Discord:   ${DISCORD_WEBHOOK_URL ? "enabled" : "disabled"}`);
  console.log(`  OpenClaw:  ${OPENCLAW_FORWARD_URL ? "enabled" : "disabled"}`);
  console.log(`  History:   ${history.length} msgs restored`);
  console.log(`  Agents:    ${agents.size} registrations restored`);
  console.log("═══════════════════════════════════════════════════════");
});
