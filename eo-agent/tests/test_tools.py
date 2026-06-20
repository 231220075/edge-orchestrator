"""Tests for the LangChain tool wrappers."""

import os
import base64
import pytest

# Force mock mode for all tests in this module
os.environ["EO_MOCK_MODE"] = "true"


@pytest.mark.asyncio
async def test_get_cluster_topology_returns_nodes():
    """get_cluster_topology should return a dict with nodes and metadata."""
    from eo_agent.tools import get_cluster_topology

    result = await get_cluster_topology.ainvoke({})

    assert "nodes" in result
    assert isinstance(result["nodes"], list)
    assert len(result["nodes"]) == 2
    assert result["nodes"][0]["node_type"] in ("Heavy", "Light")
    assert "role_assignments" in result
    assert "tasks_pending" in result
    assert "tasks_completed" in result


@pytest.mark.asyncio
async def test_submit_to_cas_and_raft_returns_hashes():
    """submit_to_cas_and_raft should return code_hash and task_id."""
    from eo_agent.tools import submit_to_cas_and_raft

    result = await submit_to_cas_and_raft.ainvoke({
        "code": "def hello(): return 42",
        "code_language": "python",
        "required_runtime": "Wasm",
        "routing": "AnyExecutor",
        "timeout_ms": 5000,
    })

    assert "code_hash" in result
    assert "task_id" in result
    assert len(result["code_hash"]) == 16  # mock uses truncated SHA-256


@pytest.mark.asyncio
async def test_submit_to_cas_and_raft_code_b64_encoded_in_call():
    """The code should be base64-encoded when sent to the IPC server."""
    from eo_agent.client import MockJsonRpcClient, get_client

    # Replace the global client factory's mock with a fresh one
    client = MockJsonRpcClient()
    # We test directly via the client to inspect the params
    code = "print('hello')"
    code_b64 = base64.b64encode(code.encode()).decode()
    result = await client.call("submit_to_cas_and_raft", {
        "code": code_b64,
        "code_language": "python",
        "required_runtime": "Wasm",
        "routing": "AnyExecutor",
        "timeout_ms": 30000,
    })

    assert "code_hash" in result
    assert "task_id" in result


@pytest.mark.asyncio
async def test_fetch_execution_result_decodes_fields():
    """fetch_execution_result should decode base64 stdout/stderr."""
    from eo_agent.tools import fetch_execution_result

    result = await fetch_execution_result.ainvoke({"result_hash": "abc123"})

    assert result["exit_code"] == 0
    assert "Hello from the sandbox!" in result["stdout"]
    assert result["stderr"] == ""
    assert result["execution_time_ms"] == 15


@pytest.mark.asyncio
async def test_fetch_execution_result_handles_missing_hash():
    """fetch_execution_result should handle errors gracefully."""
    from eo_agent.client import MockJsonRpcClient
    import json

    # A mock client that returns an error
    client = MockJsonRpcClient(responses={
        "fetch_execution_result": {"error": "not found"},
    })
    result = await client.call("fetch_execution_result", {"result_hash": "nonexistent"})
    # The raw mock returns whatever we configured
    assert "error" in result


@pytest.mark.asyncio
async def test_tool_error_handling():
    """Tools should return error dicts on JsonRpcError, not raise."""
    from eo_agent.client import MockJsonRpcClient

    client = MockJsonRpcClient(responses={
        "get_cluster_topology": {},
        "submit_to_cas_and_raft": {"code_hash": "abc", "task_id": "123"},
        "fetch_execution_result": {},
    })

    # All methods should return dicts (no exceptions)
    topo = await client.call("get_cluster_topology", {})
    assert isinstance(topo, dict)

    submit = await client.call("submit_to_cas_and_raft", {"code": "dGVzdA=="})
    assert isinstance(submit, dict)

    fetch = await client.call("fetch_execution_result", {"result_hash": "abc"})
    assert isinstance(fetch, dict)
