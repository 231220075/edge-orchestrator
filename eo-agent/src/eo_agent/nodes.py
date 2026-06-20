"""LangGraph node functions for the ReAct (Reasoning + Action) workflow.

Each node is an async function that reads from ``AgentState`` and returns
a partial state update. Nodes are designed to work with or without a
real LLM — in test/mock mode they produce deterministic output.
"""

from __future__ import annotations

import json
import os
from typing import Any

from langchain_core.messages import AIMessage, HumanMessage, SystemMessage

from eo_agent.state import AgentState
from eo_agent.tools import (
    fetch_execution_result,
    get_cluster_topology,
    submit_to_cas_and_raft,
)

# ── Helpers ────────────────────────────────────────────────────────────

_MAX_RETRIES = 3


def _get_chat_model():
    """Return a LangChain ChatModel, or None if no LLM is configured.

    Supports ``langchain-openai`` and ``langchain-anthropic`` out of the box.
    Set ``EO_LLM_MODEL`` to override the default.
    """
    model_name = os.environ.get("EO_LLM_MODEL", "gpt-4o")

    # Try OpenAI first
    try:
        from langchain_openai import ChatOpenAI
        return ChatOpenAI(model=model_name, temperature=0.1)
    except ImportError:
        pass

    # Try Anthropic
    try:
        from langchain_anthropic import ChatAnthropic
        return ChatAnthropic(model=model_name, temperature=0.1)
    except ImportError:
        pass

    return None


async def _call_llm(
    system_prompt: str,
    user_message: str,
    state: AgentState,
) -> str:
    """Call the LLM with system + user prompts. Returns the text response.

    In mock mode (no LLM available), returns a canned deterministic response
    so the graph can be tested without an API key.
    """
    model = _get_chat_model()

    if model is None:
        # Mock mode — return deterministic placeholder based on the prompt
        return _mock_llm_response(system_prompt, user_message, state)

    messages = [
        SystemMessage(content=system_prompt),
        HumanMessage(content=user_message),
    ]
    response = await model.ainvoke(messages)
    return response.content  # type: ignore[return-value]


def _mock_llm_response(system_prompt: str, user_message: str, state: AgentState) -> str:
    """Deterministic mock for testing without a real LLM."""
    last_error = state.get("last_error", "")
    retry_count = state.get("retry_count", 0)

    if "planner" in system_prompt.lower():
        return json.dumps({
            "intent": "User wants to deploy and execute code on the cluster",
            "plan": [
                {"step": 1, "action": "inspect_cluster", "target": "Get available nodes"},
                {"step": 2, "action": "generate_code", "target": "Write the requested function"},
                {"step": 3, "action": "deploy_code", "target": "Submit to CAS and schedule"},
                {"step": 4, "action": "evaluate_result", "target": "Check execution output"},
            ],
        })

    if "coder" in system_prompt.lower():
        if last_error and retry_count < _MAX_RETRIES:
            return (
                f"# Fixed code (attempt {retry_count + 1}) — previous error: {last_error}\n"
                "def compute():\n"
                "    return 42\n"
            )
        return (
            "# Generated code\n"
            "def compute():\n"
            "    return 42\n"
        )

    return "{}"


# ── Node 1: Planner ────────────────────────────────────────────────────


async def planner_node(state: AgentState) -> dict[str, Any]:
    """Parse the user's natural-language request into a structured plan.

    Reads the last user message from ``state["messages"]``, calls
    ``get_cluster_topology`` to inspect available nodes, and produces
    an ``execution_plan``.

    Returns a partial state update with ``user_intent``, ``execution_plan``,
    and an ``AIMessage`` appended to the conversation.
    """
    # Extract the user's most recent message
    messages = state.get("messages", [])
    user_text = ""
    for msg in reversed(messages):
        if hasattr(msg, "content") and isinstance(msg.content, str):
            user_text = msg.content
            break

    topology = await get_cluster_topology.ainvoke({})  # type: ignore[attr-defined]

    system_prompt = (
        "You are the Planner agent in an edge-cloud orchestration system. "
        "Your job is to:\n"
        "1. Parse the user's natural language intent.\n"
        "2. Review the available cluster topology (nodes, capabilities, roles).\n"
        "3. Produce a JSON execution plan with numbered steps.\n\n"
        "Respond ONLY with a JSON object: "
        '{"intent": "...", "plan": [{"step": 1, "action": "...", "target": "..."}]}'
    )

    user_message = (
        f"User request: {user_text}\n\n"
        f"Current cluster topology:\n{json.dumps(topology, indent=2)}"
    )

    raw = await _call_llm(system_prompt, user_message, state)

    try:
        parsed = json.loads(raw)
        intent = parsed.get("intent", user_text)
        plan = parsed.get("plan", [])
    except json.JSONDecodeError:
        intent = user_text
        plan = [{"step": 1, "action": "execute", "target": user_text}]

    return {
        "user_intent": intent,
        "execution_plan": plan,
        "messages": [AIMessage(content=f"Plan created: {json.dumps(plan)}")],
    }


# ── Node 2: Coder ──────────────────────────────────────────────────────


async def coder_node(state: AgentState) -> dict[str, Any]:
    """Generate (or regenerate) code to fulfill the execution plan.

    On first pass, writes code from the plan. On retry (when
    ``last_error`` is set), rewrites the code to fix the reported error.

    Returns a partial state update with ``generated_code``, ``code_language``,
    and incremented ``retry_count``.
    """
    plan = state.get("execution_plan", [])
    last_error = state.get("last_error", "")
    retry_count = state.get("retry_count", 0) + 1
    previous_code = state.get("generated_code", "")

    correction_context = ""
    if last_error and previous_code:
        correction_context = (
            f"\n\nPREVIOUS ATTEMPT FAILED with error:\n{last_error}\n\n"
            f"Previous code was:\n```\n{previous_code}\n```\n\n"
            f"Please FIX the code to address the error above."
        )

    system_prompt = (
        "You are the Coder agent in an edge-cloud orchestration system. "
        "Your job is to generate executable code that fulfills the plan. "
        "Output ONLY the code, no explanation. "
        "Default to Python unless the plan specifies Wasm."
        + correction_context
    )

    plan_text = json.dumps(plan, indent=2)
    user_message = f"Execution plan:\n{plan_text}\n\nWrite the code to implement this plan."

    code = await _call_llm(system_prompt, user_message, state)

    # Detect language from plan hints
    code_language = "python"
    plan_str = json.dumps(plan).lower()
    if "wasm" in plan_str or "wat" in plan_str:
        code_language = "wasm"
    elif "container" in plan_str or "native" in plan_str:
        code_language = "posix"

    return {
        "generated_code": code,
        "code_language": code_language,
        "retry_count": retry_count,
        "last_error": None,  # clear previous error on new attempt
        "messages": [
            AIMessage(
                content=f"Code generated (attempt {retry_count}):\n```{code_language}\n{code[:500]}\n```"
            )
        ],
    }


# ── Node 3: Deployment / Router ────────────────────────────────────────


async def deployment_node(state: AgentState) -> dict[str, Any]:
    """Submit generated code to the cluster via CAS + Raft.

    Calls ``submit_to_cas_and_raft`` with the code and plan metadata.
    Stores the returned ``code_hash`` and ``task_id`` in state.

    Returns a partial state update with ``code_hash``, ``task_id``,
    and a tool-result message.
    """
    code = state.get("generated_code", "")
    code_language = state.get("code_language", "python")
    plan = state.get("execution_plan", [])

    # Determine runtime and routing from plan
    plan_str = json.dumps(plan).lower()
    if "wasm" in plan_str:
        required_runtime = "Wasm"
    elif "container" in plan_str:
        required_runtime = "Container"
    else:
        required_runtime = "Wasm"  # default to Wasm sandbox

    if "prefer wasm" in plan_str:
        routing = "PreferWasm"
    elif "prefer native" in plan_str:
        routing = "PreferNative"
    else:
        routing = "AnyExecutor"

    result = await submit_to_cas_and_raft.ainvoke({  # type: ignore[attr-defined]
        "code": code,
        "code_language": code_language,
        "required_runtime": required_runtime,
        "routing": routing,
        "timeout_ms": 30000,
    })

    if "error" in result:
        return {
            "last_error": f"Deployment failed: {result['error']}",
            "messages": [AIMessage(content=f"Deployment error: {result['error']}")],
        }

    code_hash = result.get("code_hash", "")
    task_id = result.get("task_id", "")

    return {
        "code_hash": code_hash,
        "task_id": task_id,
        "messages": [
            AIMessage(
                content=f"Task submitted: code_hash={code_hash}, task_id={task_id}"
            )
        ],
    }


# ── Node 4: Evaluator ──────────────────────────────────────────────────


async def evaluator_node(state: AgentState) -> dict[str, Any]:
    """Fetch the execution result and evaluate success or failure.

    If the task succeeded (exit_code == 0), writes ``final_answer``.
    If the task failed, writes ``last_error`` so the Coder can retry.

    Returns a partial state update with ``exit_code``, ``stdout``, ``stderr``,
    ``execution_time_ms``, and either ``final_answer`` or ``last_error``.
    """
    code_hash = state.get("code_hash", "")
    task_id = state.get("task_id", "")

    if not code_hash:
        return {
            "last_error": "No code_hash available — deployment may have failed",
            "final_answer": "Workflow error: nothing to evaluate.",
        }

    # In a production system we would look up the result_hash from the
    # completed task. For now, the code_hash doubles as a result locator
    # (the mock maps fetch_execution_result by hash).
    result = await fetch_execution_result.ainvoke({  # type: ignore[attr-defined]
        "result_hash": code_hash,
    })

    if "error" in result:
        return {
            "last_error": f"Result fetch failed: {result['error']}",
            "messages": [AIMessage(content=f"Fetch error: {result['error']}")],
        }

    exit_code = result.get("exit_code", -1)
    stdout = result.get("stdout", "")
    stderr = result.get("stderr", "")
    execution_time_ms = result.get("execution_time_ms", 0)

    update: dict[str, Any] = {
        "exit_code": exit_code,
        "stdout": stdout,
        "stderr": stderr,
        "execution_time_ms": execution_time_ms,
    }

    if exit_code == 0:
        update["final_answer"] = (
            f"Task {task_id} completed successfully in {execution_time_ms}ms.\n"
            f"Output:\n{stdout}"
        )
        update["messages"] = [
            AIMessage(content=f"Execution succeeded: {stdout[:500]}"),
        ]
    else:
        update["last_error"] = stderr or f"Non-zero exit code: {exit_code}"
        update["messages"] = [
            AIMessage(content=f"Execution failed (exit {exit_code}): {stderr[:500]}"),
        ]

    return update
