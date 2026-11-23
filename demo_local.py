"""Exercise two clients across a graceful restart of a disposable local bridge."""
import os
import secrets
import shutil
import socket
import subprocess
import tempfile
import time
import urllib.error
import urllib.request
from contextlib import contextmanager
from pathlib import Path

from agent_bridge_client import AgentBridgeClient

ROOT = Path(__file__).resolve().parent


@contextmanager
def local_server(state, key):
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        port = sock.getsockname()[1]
    node = shutil.which("node")
    if not node:
        raise RuntimeError("Install Node.js and run npm install in this repository.")
    env = {"PATH": os.defpath, "BRIDGE_HOST": "127.0.0.1", "BRIDGE_PORT": str(port),
           "BRIDGE_STATE_DIR": state, "BRIDGE_API_KEY": key}
    url = f"http://127.0.0.1:{port}"
    producer = AgentBridgeClient(url, "producer", key, timeout=2)
    reader = AgentBridgeClient(url, "reviewer", key, timeout=2)
    process = subprocess.Popen([node, "server.js"], cwd=ROOT, env=env,
                               stdout=subprocess.DEVNULL)
    try:
        deadline = time.monotonic() + 8
        while time.monotonic() < deadline:
            if process.poll() is not None:
                raise RuntimeError("Bridge exited during startup. Check npm install.")
            try:
                if producer.health().get("service") == "agent-bridge":
                    break
            except urllib.error.HTTPError:
                raise
            except (urllib.error.URLError, TimeoutError):
                time.sleep(0.05)
        else:
            raise RuntimeError("Disposable bridge did not start.")
        yield producer, reader
    finally:
        process.terminate()
        try:
            if process.wait(timeout=7) != 0:
                raise RuntimeError("Bridge exited without a successful graceful shutdown.")
        except subprocess.TimeoutExpired:
            process.kill()
            process.wait()
            raise RuntimeError("Graceful shutdown timed out.")


def main():
    if os.name != "posix":
        raise SystemExit("Use Linux, macOS or WSL for this SIGTERM restart example.")
    urllib.request.install_opener(urllib.request.build_opener(urllib.request.ProxyHandler({})))
    key = secrets.token_urlsafe(32)
    message = "The example report is ready."
    with tempfile.TemporaryDirectory(prefix="agent-bridge-demo-") as state:
        with local_server(state, key) as (writer, reader):
            writer.register(["publish"])
            reader.register(["review"])
            sent = writer.send_message("reviewer", message)
            message_id = sent["messageId"]
            if not sent["queued"]:
                raise RuntimeError("Expected one queued message.")
        with local_server(state, key) as (_, reader):
            agents = {entry["agentId"]: entry["capabilities"] for entry in reader.list_agents()}
            if agents != {"producer": ["publish"], "reviewer": ["review"]}:
                raise RuntimeError("Registrations did not survive restart.")
            received = reader.read_messages()
            if len(received) != 1 or received[0]["id"] != message_id or received[0]["content"] != message:
                raise RuntimeError("The reopened inbox did not match the original send.")
            if reader.read_messages() or [r["id"] for r in reader.history()] != [message_id]:
                raise RuntimeError("Inbox consumption or retained history did not match.")
            print(f"Restored 2 registrations. Received once: {message}")
            print(f"Reopened history entry {message_id}. Inbox now empty.")


if __name__ == "__main__":
    main()
