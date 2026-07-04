"""JSON-RPC 2.0 client over Unix Domain Sockets.

Provides both a real UDS client and a mock client for testing.
Set ``EO_MOCK_MODE=true`` to use the mock (no Rust node required).
"""

from __future__ import annotations

import asyncio
import json
import os
import uuid
from pathlib import Path
from typing import Any, Optional


class JsonRpcError(Exception):
    """Raised when the JSON-RPC server returns an error response."""

    def __init__(self, code: int, message: str) -> None:
        self.code = code
        self.message = message
        super().__init__(f"JSON-RPC error {code}: {message}")


# ── Abstract interface ────────────────────────────────────────────────


class RpcClient:
    """Abstract interface for JSON-RPC clients (real + mock)."""

    async def call(self, method: str, params: dict[str, Any]) -> dict[str, Any]:
        raise NotImplementedError


# ── Real UDS client ───────────────────────────────────────────────────


class JsonRpcClient(RpcClient):
    """JSON-RPC 2.0 client that communicates over a Unix Domain Socket.

    Uses newline-delimited JSON framing: one JSON object per line.
    A fresh connection is opened for each ``call()`` (stateless).
    """

    def __init__(self, socket_path: str | Path) -> None:
        self.socket_path = str(Path(socket_path).expanduser())

    async def call(self, method: str, params: dict[str, Any]) -> dict[str, Any]:
        """Send a JSON-RPC request and return the ``result`` field.

        Raises :class:`JsonRpcError` if the server returns an error.
        """
        request = {
            "jsonrpc": "2.0",
            "method": method,
            "params": params,
            "id": uuid.uuid4().hex,
        }

        payload = json.dumps(request, ensure_ascii=False) + "\n"

        reader, writer = await asyncio.open_unix_connection(self.socket_path)

        try:
            writer.write(payload.encode("utf-8"))
            await writer.drain()

            # Read the response line
            line = await asyncio.wait_for(reader.readline(), timeout=30.0)
            if not line:
                raise JsonRpcError(-32000, "Server closed connection without response")

            response = json.loads(line.decode("utf-8"))
        finally:
            writer.close()
            await writer.wait_closed()

        if "error" in response:
            err = response["error"]
            raise JsonRpcError(err.get("code", -1), err.get("message", "Unknown error"))

        return response.get("result", {})


# ── Mock client (no Rust node required) ────────────────────────────────


class MockJsonRpcClient(RpcClient):
    """Mock JSON-RPC client that returns canned responses.

    Useful for testing the LangGraph workflow without a running Rust node.
    Responses are keyed by method name. Set ``EO_MOCK_MODE=true`` to activate
    via the :func:`get_client` factory.
    """

    def __init__(self, responses: Optional[dict[str, dict[str, Any]]] = None) -> None:
        self.responses = responses or self._default_responses()
        self.calls: list[tuple[str, dict[str, Any]]] = []

    async def call(self, method: str, params: dict[str, Any]) -> dict[str, Any]:
        self.calls.append((method, params))

        if method == "submit_to_cas_and_raft":
            # Generate a deterministic code_hash from the code content
            import hashlib

            code = params.get("code", "")
            code_hash = hashlib.sha256(code.encode()).hexdigest()[:16]
            task_id = str(uuid.uuid4())
            # Simulate inline execution: store a mock execution result
            # and return its hash so the evaluator can find it
            mock_result = {
                "exit_code": 0,
                "stdout": "SGVsbG8gZnJvbSB0aGUgc2FuZGJveCE=\n",  # "Hello from the sandbox!"
                "stderr": "",
                "execution_time_ms": 15,
                "peak_memory_bytes": 1048576,
            }
            result_hash = hashlib.sha256(
                json.dumps(mock_result).encode()
            ).hexdigest()[:16]
            # Store in mock responses so fetch_execution_result can find it
            self.responses[result_hash] = mock_result
            return {
                "code_hash": code_hash,
                "task_id": task_id,
                "result_hash": result_hash,
            }

        if method == "fetch_execution_result":
            # Look up by result_hash (stored during inline execution simulation)
            result_hash = params.get("result_hash", "")
            if result_hash and result_hash in self.responses:
                return self.responses[result_hash]

        return self.responses.get(method, {})

    @staticmethod
    def _default_responses() -> dict[str, dict[str, Any]]:
        return {
            "get_cluster_topology": {
                "nodes": [
                    {
                        "node_id": "a1b2c3d4-e29b-41d4-a716-446655440000",
                        "node_type": "Heavy",
                        "os": "MacOS",
                        "capabilities": {
                            "storage": True,
                            "gpu_acceleration": False,
                            "runtimes": ["Wasm"],
                            "max_memory_mb": 8192,
                            "cpu_cores": 8,
                        },
                        "advertised_addresses": [
                            "/ip4/127.0.0.1/tcp/9000"
                        ],
                        "current_assigned_roles": ["Execution", "Storage"],
                        "started_at": "2026-06-11T00:00:00Z",
                    },
                    {
                        "node_id": "b2c3d4e5-e29b-41d4-a716-446655440001",
                        "node_type": "Light",
                        "os": "Linux",
                        "capabilities": {
                            "storage": False,
                            "gpu_acceleration": True,
                            "runtimes": ["Wasm", "Container"],
                            "max_memory_mb": 4096,
                            "cpu_cores": 4,
                        },
                        "advertised_addresses": [
                            "/ip4/10.0.0.2/tcp/9000"
                        ],
                        "current_assigned_roles": ["Inference"],
                        "started_at": "2026-06-11T00:00:00Z",
                    },
                ],
                "role_assignments": {
                    "a1b2c3d4-e29b-41d4-a716-446655440000": ["Execution", "Storage"],
                    "b2c3d4e5-e29b-41d4-a716-446655440001": ["Inference"],
                },
                "tasks_pending": 0,
                "tasks_completed": 42,
            },
            "fetch_execution_result": {
                "exit_code": 0,
                "stdout": "SGVsbG8gZnJvbSB0aGUgc2FuZGJveCE=\n",  # "Hello from the sandbox!"
                "stderr": "",
                "execution_time_ms": 15,
                "peak_memory_bytes": 1048576,
            },
        }


# ── Factory ────────────────────────────────────────────────────────────


def _is_mock_mode() -> bool:
    """Check mock mode at call time (allows tests to set env var after import)."""
    return os.environ.get("EO_MOCK_MODE", "").lower() in ("1", "true", "yes")


def _default_socket_path() -> str:
    """Return the default UDS socket path."""
    return os.environ.get("EO_IPC_SOCKET", "~/.edge-orchestrator/ipc.sock")


def get_client(socket_path: str | None = None) -> RpcClient:
    """Return a JSON-RPC client (real or mock depending on ``EO_MOCK_MODE``).

    Args:
        socket_path: Override the UDS path (ignored in mock mode).

    Returns:
        An :class:`RpcClient` implementation.
    """
    if _is_mock_mode():
        return MockJsonRpcClient()

    path = socket_path or _default_socket_path()
    return JsonRpcClient(path)
