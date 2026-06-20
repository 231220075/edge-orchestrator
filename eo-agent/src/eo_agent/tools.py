"""LangChain Tools bridging to the Rust Edge-Cloud Orchestrator.

Three tools expose the core Rust capabilities via Unix Domain Socket
JSON-RPC. In mock mode (``EO_MOCK_MODE=true``) they return canned
responses — no running Rust node required.
"""

from __future__ import annotations

import base64
from typing import Any

from langchain_core.tools import tool

from eo_agent.client import JsonRpcError, get_client


def _decode_output(value: str) -> str:
    """Decode a base64-encoded field from the JSON-RPC response."""
    if not value:
        return ""
    try:
        return base64.b64decode(value).decode("utf-8", errors="replace")
    except Exception:
        return value


@tool
async def get_cluster_topology() -> dict[str, Any]:
    """Query the cluster for active nodes, their OS types, capabilities, and
    assigned roles.

    Returns a dictionary with:
        - nodes: list of NodeDescriptor objects
        - role_assignments: dict mapping node_id -> list of role strings
        - tasks_pending: number of tasks waiting in the queue
        - tasks_completed: number of tasks completed so far
    """
    client = get_client()
    try:
        return await client.call("get_cluster_topology", {})
    except JsonRpcError as e:
        return {"error": str(e), "code": e.code}


@tool
async def submit_to_cas_and_raft(
    code: str,
    code_language: str = "python",
    required_runtime: str = "Wasm",
    routing: str = "AnyExecutor",
    timeout_ms: int = 30000,
) -> dict[str, Any]:
    """Submit generated code to the cluster for execution.

    The code is base64-encoded and sent to the Rust node, which:
    1. Computes the SHA-256 content hash
    2. Stores the blob in the content-addressed store (CAS)
    3. Proposes a SubmitTask entry to the Raft consensus cluster

    Args:
        code: The source code or bytecode to execute.
        code_language: One of "wasm", "python", or "posix".
        required_runtime: Runtime to use — "Wasm", "NativePosix", or "Container".
        routing: Routing strategy — "AnyExecutor", "PreferWasm", "PreferNative",
            or "Pinned:<node_id>".
        timeout_ms: Execution timeout in milliseconds (default 30000).

    Returns:
        dict with ``code_hash`` (str) and ``task_id`` (str).
    """
    code_b64 = base64.b64encode(code.encode("utf-8")).decode("ascii")

    client = get_client()
    try:
        return await client.call("submit_to_cas_and_raft", {
            "code": code_b64,
            "code_language": code_language,
            "required_runtime": required_runtime,
            "routing": routing,
            "timeout_ms": timeout_ms,
        })
    except JsonRpcError as e:
        return {"error": str(e), "code": e.code}


@tool
async def fetch_execution_result(result_hash: str) -> dict[str, Any]:
    """Retrieve the execution result of a completed task from CAS.

    Args:
        result_hash: The content hash of the execution result blob.

    Returns:
        dict with ``exit_code``, ``stdout`` (decoded string), ``stderr``
        (decoded string), ``execution_time_ms``, and ``peak_memory_bytes``.
    """
    client = get_client()
    try:
        result = await client.call("fetch_execution_result", {
            "result_hash": result_hash,
        })
    except JsonRpcError as e:
        return {"error": str(e), "code": e.code}

    # Decode stdout/stderr from base64 for the agent's consumption
    result["stdout"] = _decode_output(result.get("stdout", ""))
    result["stderr"] = _decode_output(result.get("stderr", ""))
    return result


# ── Tool registry ──────────────────────────────────────────────────────

ALL_TOOLS = [get_cluster_topology, submit_to_cas_and_raft, fetch_execution_result]
"""All tools available to the agent graph."""
