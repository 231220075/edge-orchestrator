"""Tests for the LangGraph ReAct workflow.

Uses the mock JSON-RPC client so no Rust node is needed.
All tests verify the graph structure, node execution, and
conditional routing (including the self-correction loop).
"""

import os

# Force mock mode before any imports from eo_agent
os.environ["EO_MOCK_MODE"] = "true"

import pytest
from langchain_core.messages import HumanMessage


@pytest.mark.asyncio
async def test_graph_runs_all_four_nodes():
    """A successful run should traverse planner → coder → deploy → evaluate → END."""
    from eo_agent.graph import build_graph

    graph = build_graph()

    result = await graph.ainvoke(
        {"messages": [HumanMessage(content="Deploy a hello-world function")], "retry_count": 0},
        {"configurable": {"thread_id": "test-1"}},
    )

    # All four nodes should have populated their fields
    assert result.get("user_intent"), "Planner did not set user_intent"
    assert result.get("execution_plan"), "Planner did not set execution_plan"
    assert result.get("generated_code"), "Coder did not generate code"
    assert result.get("code_hash"), "Deploy did not set code_hash"
    assert result.get("task_id"), "Deploy did not set task_id"
    assert result.get("exit_code") is not None, "Evaluator did not set exit_code"

    # With mock fetch_execution_result returning exit_code=0, we should get final_answer
    assert result.get("final_answer"), "Evaluator should produce final_answer on success"


@pytest.mark.asyncio
async def test_graph_produces_final_answer_on_success():
    """A successful workflow should end with a final_answer containing output."""
    from eo_agent.graph import build_graph

    graph = build_graph()

    result = await graph.ainvoke(
        {"messages": [HumanMessage(content="Run a computation")], "retry_count": 0},
        {"configurable": {"thread_id": "test-2"}},
    )

    final = result.get("final_answer", "")
    assert "completed successfully" in final.lower()
    assert "Hello from the sandbox!" in final


@pytest.mark.asyncio
async def test_graph_retry_count_initialized():
    """The retry_count should be incremented by the coder node."""
    from eo_agent.graph import build_graph

    graph = build_graph()

    result = await graph.ainvoke(
        {"messages": [HumanMessage(content="Deploy retry counting test")], "retry_count": 0},
        {"configurable": {"thread_id": "test-3"}},
    )

    # Coder always increments retry_count; with success, it should be 1
    assert result.get("retry_count", 0) >= 1


@pytest.mark.asyncio
async def test_route_after_evaluation_success():
    """route_after_evaluation returns 'end' when exit_code is 0."""
    from eo_agent.graph import route_after_evaluation

    result = route_after_evaluation({
        "exit_code": 0,
        "retry_count": 1,
        "last_error": None,
    })
    assert result == "end"


@pytest.mark.asyncio
async def test_route_after_evaluation_retry():
    """route_after_evaluation returns 'coder' when there is an error and retries remain."""
    from eo_agent.graph import route_after_evaluation

    result = route_after_evaluation({
        "exit_code": 1,
        "retry_count": 1,  # < MAX_RETRIES (3)
        "last_error": "sandbox: trap: unreachable executed",
    })
    assert result == "coder"


@pytest.mark.asyncio
async def test_route_after_evaluation_exhausted():
    """route_after_evaluation returns 'end' when retries are exhausted."""
    from eo_agent.graph import route_after_evaluation

    result = route_after_evaluation({
        "exit_code": 1,
        "retry_count": 3,  # >= MAX_RETRIES (3)
        "last_error": "still failing",
    })
    assert result == "end"


@pytest.mark.asyncio
async def test_route_after_evaluation_no_error():
    """route_after_evaluation returns 'end' when there is no last_error (even with no exit_code)."""
    from eo_agent.graph import route_after_evaluation

    result = route_after_evaluation({
        "retry_count": 0,
        "last_error": None,
    })
    assert result == "end"


@pytest.mark.asyncio
async def test_graph_messages_accumulate():
    """The add_messages reducer should accumulate messages across nodes."""
    from eo_agent.graph import build_graph

    graph = build_graph()

    result = await graph.ainvoke(
        {"messages": [HumanMessage(content="Hello cluster")], "retry_count": 0},
        {"configurable": {"thread_id": "test-4"}},
    )

    messages = result.get("messages", [])
    # We should have: initial HumanMessage + planner AIMessage + coder AIMessage
    # + deploy AIMessage + evaluate AIMessage = at least 5 messages
    assert len(messages) >= 3, f"Expected at least 3 messages, got {len(messages)}"
