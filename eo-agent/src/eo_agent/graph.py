"""LangGraph StateGraph for the Edge-Cloud Orchestrator cognitive agent.

The graph starts with a **router** that classifies every user request
into one of three paths:

    router ── "deploy" ──→ planner → coder → deploy → evaluate
                           ↑              │            │
                           └── retry ─────┘            │
                                                       → END
    router ── "question" → direct_answer → END

    router ── "tool" ──→ direct_answer → END
"""

from __future__ import annotations

from langgraph.graph import END, StateGraph
from langgraph.checkpoint.memory import MemorySaver

from eo_agent.state import AgentState
from eo_agent.nodes import (
    router_node,
    direct_answer_node,
    planner_node,
    coder_node,
    deployment_node,
    evaluator_node,
)

# Maximum number of Coder→Deploy→Evaluate retries before giving up.
MAX_RETRIES = 3


def route_after_router(state: AgentState) -> str:
    """Route based on the ``task_type`` set by the router node."""
    task_type = state.get("task_type", "question")
    if task_type == "deploy":
        return "planner"
    else:
        return "direct_answer"


def route_after_evaluation(state: AgentState) -> str:
    """Conditional routing after the evaluator node.

    Returns:
        ``"coder"`` to retry with self-correction,
        ``"end"`` if the task succeeded or retries are exhausted.
    """
    exit_code = state.get("exit_code")
    retry_count = state.get("retry_count", 0)
    last_error = state.get("last_error")

    # Success — we're done
    if exit_code == 0:
        return "end"

    # No error set and no exit code — nothing to retry
    if not last_error:
        return "end"

    # Still have retries — feed error back to coder
    if retry_count < MAX_RETRIES:
        return "coder"

    # Exhausted retries — give up
    return "end"


def build_graph(checkpointer: MemorySaver | None = None) -> StateGraph:
    """Build and compile the cognitive agent workflow.

    Args:
        checkpointer: Optional memory checkpointer for conversation persistence.
            Defaults to an in-memory :class:`MemorySaver`.

    Returns:
        A compiled :class:`StateGraph` ready for ``ainvoke``.
    """
    if checkpointer is None:
        checkpointer = MemorySaver()

    workflow = StateGraph(AgentState)

    # ── Register nodes ──────────────────────────────────────────────
    workflow.add_node("router", router_node)
    workflow.add_node("direct_answer", direct_answer_node)
    workflow.add_node("planner", planner_node)
    workflow.add_node("coder", coder_node)
    workflow.add_node("deploy", deployment_node)
    workflow.add_node("evaluate", evaluator_node)

    # ── Edges ───────────────────────────────────────────────────────
    workflow.set_entry_point("router")

    # Router branches to planner (deploy) or direct_answer (question/tool)
    workflow.add_conditional_edges(
        "router",
        route_after_router,
        {
            "planner": "planner",
            "direct_answer": "direct_answer",
        },
    )

    # Deploy pipeline: planner → coder → deploy → evaluate
    workflow.add_edge("planner", "coder")
    workflow.add_edge("coder", "deploy")
    workflow.add_edge("deploy", "evaluate")

    # Evaluator: retry or end
    workflow.add_conditional_edges(
        "evaluate",
        route_after_evaluation,
        {
            "coder": "coder",
            "end": END,
        },
    )

    # Direct answer always ends
    workflow.add_edge("direct_answer", END)

    return workflow.compile(checkpointer=checkpointer)
