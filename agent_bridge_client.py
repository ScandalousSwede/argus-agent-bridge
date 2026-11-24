#!/usr/bin/env python3
"""
Argus Agent Bridge — Python Client SDK (Layer 2)

Drop this file into the Argus desktop codebase. It provides a simple
synchronous + async interface for bidirectional text messaging with
Cascade (and any other registered agents) over LAN.

Usage (synchronous — runs ON the desktop alongside the bridge):
    from agent_bridge_client import AgentBridgeClient

    client = AgentBridgeClient(
        bridge_url="http://localhost:3018",   # localhost since both on desktop
        agent_id="argus",
        api_key="your-key"                    # optional
    )
    client.register(capabilities=["vla", "robot_control", "vision"])

    # Send a message to Cascade
    client.send_message(to="cascade", content="Task complete: pick-and-place done.")

    # Read messages from Cascade
    messages = client.read_messages(limit=10)
    for msg in messages:
        print(f"[{msg['from']}] {msg['content']}")

Usage (WebSocket push, async):
    import asyncio
    from agent_bridge_client import AgentBridgeClientAsync

    async def main():
        client = AgentBridgeClientAsync(
            bridge_url="ws://localhost:3018",
            agent_id="argus",
            api_key="your-key"
        )
        async with client.connect() as ws:
            async for message in ws:
                print(f"Received: {message['content']}")
                await client.send_message(to=message['from'], content="ACK")

    asyncio.run(main())
"""

import json
import time
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime, timezone
from typing import Any, Optional


class AgentBridgeClient:
    """Synchronous HTTP client for the Argus Agent Bridge (Layer 2)."""

    def __init__(
        self,
        bridge_url: str = "http://localhost:3018",
        agent_id: str = "argus",
        api_key: str = "",
        timeout: int = 10,
    ) -> None:
        self.bridge_url = bridge_url.rstrip("/")
        self.agent_id = agent_id
        self.api_key = api_key
        self.timeout = timeout

    def _headers(self) -> dict[str, str]:
        h = {"Content-Type": "application/json"}
        if self.api_key:
            h["Authorization"] = f"Bearer {self.api_key}"
        return h

    def _get(self, path: str, params: Optional[dict[str, str]] = None) -> Any:
        url = f"{self.bridge_url}{path}"
        if params:
            url += "?" + urllib.parse.urlencode(params)
        req = urllib.request.Request(url, headers=self._headers())
        with urllib.request.urlopen(req, timeout=self.timeout) as resp:
            return json.loads(resp.read())

    def _post(self, path: str, body: dict[str, Any]) -> Any:
        url = f"{self.bridge_url}{path}"
        data = json.dumps(body).encode("utf-8")
        req = urllib.request.Request(
            url, data=data, headers=self._headers(), method="POST"
        )
        with urllib.request.urlopen(req, timeout=self.timeout) as resp:
            return json.loads(resp.read())

    def health(self) -> dict[str, Any]:
        """Check bridge health."""
        return self._get("/health")

    def register(self, capabilities: Optional[list[str]] = None) -> dict[str, Any]:
        """Register this agent with the bridge."""
        return self._post(
            "/bridge/register",
            {"agent_id": self.agent_id, "capabilities": capabilities or []},
        )

    def send_message(
        self,
        to: str,
        content: str,
        reply_to: Optional[str] = None,
        metadata: Optional[dict[str, Any]] = None,
    ) -> dict[str, Any]:
        """
        Send a text message to another agent.

        Args:
            to: Target agent ID (e.g. 'cascade') or 'broadcast'
            content: Message body — plain text or markdown
            reply_to: Optional message ID this replies to
            metadata: Optional key-value metadata

        Returns:
            {'messageId': str, 'queued': bool, 'delivered': bool}
        """
        body: dict[str, Any] = {
            "from": self.agent_id,
            "to": to,
            "content": content,
        }
        if reply_to:
            body["reply_to"] = reply_to
        if metadata:
            body["metadata"] = metadata
        return self._post("/bridge/send", body)

    def read_messages(
        self,
        limit: int = 50,
        since: Optional[str] = None,
        mark_read: bool = True,
    ) -> list[dict[str, Any]]:
        """
        Read pending messages from this agent's inbox.

        Args:
            limit: Max messages to return
            since: ISO timestamp — only return messages after this time
            mark_read: Remove returned messages from inbox

        Returns:
            List of message dicts with keys: id, from, to, content, timestamp, replyTo, metadata
        """
        params: dict[str, str] = {
            "agent": self.agent_id,
            "limit": str(limit),
            "mark_read": str(mark_read).lower(),
        }
        if since:
            params["since"] = since
        result = self._get("/bridge/messages", params)
        return result.get("messages", [])

    def clear_messages(self) -> dict[str, Any]:
        """Clear all pending messages in this agent's inbox."""
        return self._post("/bridge/messages/clear", {"agent": self.agent_id})

    def list_agents(self) -> list[dict[str, Any]]:
        """List all agents currently registered with the bridge."""
        result = self._get("/bridge/agents")
        return result.get("agents", [])

    def status(self) -> dict[str, Any]:
        """Get bridge status: agents, queue depth, uptime."""
        return self._get("/bridge/status")

    def history(
        self,
        limit: int = 100,
        from_agent: Optional[str] = None,
        to_agent: Optional[str] = None,
    ) -> list[dict[str, Any]]:
        """Get message history from the bridge."""
        params: dict[str, str] = {"limit": str(limit)}
        if from_agent:
            params["from"] = from_agent
        if to_agent:
            params["to"] = to_agent
        result = self._get("/bridge/history", params)
        return result.get("messages", [])

    def poll_loop(
        self,
        handler,
        interval: float = 2.0,
        limit: int = 20,
    ) -> None:
        """
        Blocking poll loop — calls handler(message) for each new message.

        Args:
            handler: Callable[[dict], None] — called for each incoming message
            interval: Seconds between polls
            limit: Max messages per poll
        """
        print(f"[BRIDGE] Polling for messages as '{self.agent_id}' every {interval}s...")
        while True:
            try:
                messages = self.read_messages(limit=limit)
                for msg in messages:
                    try:
                        handler(msg)
                    except Exception as e:
                        print(f"[BRIDGE] Handler error: {e}")
            except urllib.error.URLError as e:
                print(f"[BRIDGE] Poll error (bridge offline?): {e}")
            except Exception as e:
                print(f"[BRIDGE] Unexpected poll error: {e}")
            time.sleep(interval)


class AgentBridgeClientAsync:
    """
    Async WebSocket client for push-based message delivery.
    Requires: pip install websockets
    """

    def __init__(
        self,
        bridge_url: str = "ws://localhost:3018",
        agent_id: str = "argus",
        api_key: str = "",
    ) -> None:
        self.bridge_url = bridge_url.rstrip("/")
        self.agent_id = agent_id
        self.api_key = api_key
        self._ws = None

    def _ws_url(self) -> str:
        params = f"agent={urllib.parse.quote(self.agent_id)}"
        if self.api_key:
            params += f"&token={urllib.parse.quote(self.api_key)}"
        return f"{self.bridge_url}/bridge/ws?{params}"

    async def connect(self):
        """
        Async context manager. Yields self with active WebSocket connection.

        Usage:
            async with client.connect() as c:
                async for msg in c.messages():
                    print(msg)
        """
        try:
            import websockets
        except ImportError:
            raise ImportError(
                "websockets package required: pip install websockets"
            )

        async with websockets.connect(self._ws_url()) as ws:
            self._ws = ws
            yield self

    async def messages(self):
        """Async generator yielding incoming message dicts."""
        if not self._ws:
            raise RuntimeError("Not connected. Use async with client.connect()")
        async for raw in self._ws:
            envelope = json.loads(raw)
            if envelope.get("type") == "message":
                yield envelope["payload"]

    async def send_message(
        self,
        to: str,
        content: str,
        reply_to: Optional[str] = None,
        metadata: Optional[dict[str, Any]] = None,
    ) -> None:
        """Send a message over the active WebSocket connection."""
        if not self._ws:
            raise RuntimeError("Not connected. Use async with client.connect()")
        payload: dict[str, Any] = {"type": "send", "to": to, "content": content}
        if reply_to:
            payload["replyTo"] = reply_to
        if metadata:
            payload["metadata"] = metadata
        await self._ws.send(json.dumps(payload))

    async def ping(self) -> None:
        """Send a keepalive ping."""
        if self._ws:
            await self._ws.send(json.dumps({"type": "ping"}))


# ── CLI quick-test ──

if __name__ == "__main__":
    import sys

    bridge_url = "http://localhost:3018"
    agent_id = "argus"

    client = AgentBridgeClient(bridge_url=bridge_url, agent_id=agent_id)

    cmd = sys.argv[1] if len(sys.argv) > 1 else "status"

    if cmd == "status":
        print(json.dumps(client.status(), indent=2))
    elif cmd == "register":
        print(json.dumps(client.register(["vla", "robot_control", "vision"]), indent=2))
    elif cmd == "agents":
        print(json.dumps(client.list_agents(), indent=2))
    elif cmd == "send":
        to = sys.argv[2] if len(sys.argv) > 2 else "cascade"
        content = sys.argv[3] if len(sys.argv) > 3 else "Hello from Argus!"
        print(json.dumps(client.send_message(to=to, content=content), indent=2))
    elif cmd == "read":
        msgs = client.read_messages()
        print(json.dumps({"count": len(msgs), "messages": msgs}, indent=2))
    elif cmd == "poll":
        def on_message(msg):
            ts = msg.get("timestamp", "")
            print(f"[{ts}] {msg['from']} → {msg['to']}: {msg['content']}")
        client.poll_loop(on_message)
    else:
        print(f"Unknown command: {cmd}")
        print("Usage: python agent_bridge_client.py [status|register|agents|send|read|poll]")
        sys.exit(1)
