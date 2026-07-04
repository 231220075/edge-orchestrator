"""LangGraph node functions for the ReAct (Reasoning + Action) workflow.

Each node is an async function that reads from ``AgentState`` and returns
a partial state update. Nodes are designed to work with or without a
real LLM — in test/mock mode they produce deterministic output.

The routing decision (deploy vs question vs tool) is made by the LLM
in the router node — NOT by keyword matching. This gives the agent a
proper "thinking" step before it commits to a code-execution pipeline
or a direct answer.
"""

from __future__ import annotations

import json
import os
import re
import sys
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

# Track whether we've already warned about mock mode (avoid spam)
_MOCK_WARNED = False


def _strip_markdown_fences(text: str) -> str:
    """Remove markdown code fences from LLM-generated code.

    Many LLMs wrap code in ``` fences even when told not to.
    This strips the outermost ``` fences and any language tag.
    Also handles `` ``` `` fences (triple-backtick).
    """
    text = text.strip()
    # Pattern: optional opening ```<lang>, content, closing ```
    # Use regex to be robust against variations
    md_pattern = re.compile(
        r'^```(?:\w+)?\s*\n(.*?)\n```\s*$',
        re.DOTALL,
    )
    m = md_pattern.match(text)
    if m:
        return m.group(1).strip()
    # Also try without the newline variant (compact)
    if text.startswith("```") and text.endswith("```"):
        inner = text[3:-3].strip()
        # Remove language tag on first line if present
        if "\n" in inner:
            first_line, rest = inner.split("\n", 1)
            if first_line and not any(c in first_line for c in "()[]{}\"'= "):
                # Looks like a language tag (e.g., "python")
                return rest.strip()
            return inner
        return inner
    return text


def _is_ipc_error(error_msg: str) -> bool:
    """Check if an error message indicates an IPC/Socket connectivity problem."""
    ipc_markers = [
        "Connection refused",
        "No such file or directory",
        "Socket",
        "IPC",
        "connection",
        "connect",
        "Channel closed",
        "channel closed",
        "transport",
    ]
    error_lower = error_msg.lower()
    return any(m.lower() in error_lower for m in ipc_markers)


def _ensure_api_keys() -> bool:
    """Try to load API keys from the secure keyring.

    Returns ``True`` if at least one key is available (env var or keyring).
    """
    # Check if keys are already in environment
    for var in ("OPENAI_API_KEY", "ANTHROPIC_API_KEY", "DEEPSEEK_API_KEY"):
        if os.environ.get(var):
            return True

    # Try loading from keyring
    try:
        from eo_agent.keyring import get_keyring
        kr = get_keyring()
        loaded = kr.load_to_env()
        if loaded:
            return True
    except Exception:
        pass

    return False


def _get_chat_model():
    """Return a LangChain ChatModel, or None if no LLM is configured.

    Reads the active provider from :class:`LLMConfig` (``~/.eo_llm_config.yaml``).
    If no config exists, attempts to auto-generate one from existing keyring keys.

    Supports OpenAI-compatible APIs (ChatOpenAI) and Anthropic (ChatAnthropic).
    Provider-specific settings (``base_url``, ``model``, ``api_key_source``)
    are read from the config file — nothing is hardcoded.

    Returns ``None`` if no LLM is configured or the API key is missing,
    which triggers mock mode.
    """
    global _MOCK_WARNED

    # Ensure keys are loaded from keyring into environment
    _ensure_api_keys()

    # ── Read config ─────────────────────────────────────────────────────
    try:
        from eo_agent.llm_config import get_llm_config
        cfg = get_llm_config()
    except ImportError:
        if not _MOCK_WARNED:
            _warn_mock("llm_config module not available — falling back to mock mode")
            _MOCK_WARNED = True
        return None

    # Auto-generate if no config exists yet (backward compat)
    if not cfg.get_active():
        if cfg.auto_generate():
            # Reload after auto-generation
            pass
        else:
            if not _MOCK_WARNED:
                _warn_mock(
                    "No LLM provider configured. Set one up:\n"
                    "  eo-agent --llm-add <name> <base_url> <model>\n"
                    "  eo-agent --set-key <provider> <api_key>\n"
                    "Or export OPENAI_API_KEY / DEEPSEEK_API_KEY in your shell.\n"
                    "Falling back to mock mode."
                )
                _MOCK_WARNED = True
            return None

    active = cfg.get_active()
    if not active:
        return None

    provider_name = cfg.get_active_name() or "unknown"
    model_name = active.get("model") or ""
    base_url = active.get("base_url", "")
    api_key_source = active.get("api_key_source", "keyring")

    # ── Resolve API key ─────────────────────────────────────────────────
    api_key: str | None = None

    if api_key_source == "none":
        # No auth needed (local LLM, Ollama)
        pass
    elif api_key_source.startswith("env:"):
        var_name = api_key_source[len("env:"):]
        api_key = os.environ.get(var_name)
        if not api_key:
            if not _MOCK_WARNED:
                _warn_mock(
                    f"Environment variable {var_name} (for provider '{provider_name}') "
                    f"is not set — falling back to mock mode"
                )
                _MOCK_WARNED = True
            return None
    elif api_key_source.startswith("plaintext:"):
        api_key = api_key_source[len("plaintext:"):]
    else:
        # "keyring" or unknown — resolve via LLMConfig
        api_key = cfg.resolve_api_key(provider_name)
        if not api_key:
            # Also try env vars as fallback
            env_map = {
                "openai": "OPENAI_API_KEY",
                "deepseek": "DEEPSEEK_API_KEY",
                "anthropic": "ANTHROPIC_API_KEY",
            }
            env_var = env_map.get(provider_name)
            if env_var:
                api_key = os.environ.get(env_var)
        if not api_key:
            if not _MOCK_WARNED:
                _warn_mock(
                    f"No API key found for provider '{provider_name}'.\n"
                    f"  Run: eo-agent --set-key {provider_name} <your-key>\n"
                    f"  Or:  export {env_map.get(provider_name, provider_name.upper() + '_API_KEY')}=<your-key>\n"
                    f"Falling back to mock mode."
                )
                _MOCK_WARNED = True
            return None

    # ── Detect model family: Anthropic vs OpenAI-compatible ──────────────
    model_lower = model_name.lower()

    # Anthropic models
    if "claude" in model_lower or "anthropic" in model_lower:
        try:
            from langchain_anthropic import ChatAnthropic
        except ImportError:
            if not _MOCK_WARNED:
                _warn_mock(
                    "langchain-anthropic not installed — falling back to mock mode.\n"
                    "  Install: pip install langchain-anthropic"
                )
                _MOCK_WARNED = True
            return None

        if api_key:
            os.environ.setdefault("ANTHROPIC_API_KEY", api_key)
        kwargs: dict[str, Any] = {"model": model_name, "temperature": 0.1}
        if base_url:
            kwargs["base_url"] = base_url
        return ChatAnthropic(**kwargs)

    # OpenAI / DeepSeek / Ollama / any OpenAI-compatible API
    try:
        from langchain_openai import ChatOpenAI
    except ImportError:
        if not _MOCK_WARNED:
            _warn_mock("langchain-openai not installed — falling back to mock mode")
            _MOCK_WARNED = True
        return None

    if api_key:
        os.environ.setdefault("OPENAI_API_KEY", api_key)

    kwargs = {"model": model_name, "temperature": 0.1}
    if base_url:
        kwargs["base_url"] = base_url

    return ChatOpenAI(**kwargs)


def _warn_mock(reason: str) -> None:
    """Print a one-time warning about mock mode to stderr."""
    print(
        f"\n  \033[1;33m⚠ {reason}\033[0m\n",
        file=sys.stderr,
    )


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

    try:
        response = await model.ainvoke(messages)
        return response.content  # type: ignore[return-value]
    except Exception as exc:
        # Catch authentication errors, rate limits, network issues, etc.
        error_msg = str(exc)
        print(
            f"\n  \033[1;31m✗ LLM call failed:\033[0m {error_msg[:200]}\n"
            f"  \033[2mFalling back to mock response for this node.\033[0m\n",
            file=sys.stderr,
        )
        return _mock_llm_response(system_prompt, user_message, state)


def _mock_llm_response(system_prompt: str, user_message: str, state: AgentState) -> str:
    """Deterministic mock for testing without a real LLM."""
    last_error = state.get("last_error", "")
    retry_count = state.get("retry_count", 0)

    # Router — classify intent (replaces keyword matching)
    if "router" in system_prompt.lower():
        # For mock mode, do basic keyword detection on the user message
        user_lower = user_message.lower()
        deploy_hints = ["deploy", "部署", "run ", "运行", "计算", "compute",
                        "fibonacci", "斐波那契", "execute", "执行"]
        if any(h in user_lower for h in deploy_hints):
            return json.dumps({"task_type": "deploy", "reasoning": "mock: code execution task detected"})
        return json.dumps({"task_type": "question", "reasoning": "mock: defaulting to direct answer"})

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
                "\n"
                "print(compute())\n"
            )
        return (
            "# Generated code\n"
            "def compute():\n"
            "    return 42\n"
            "\n"
            "print(compute())\n"
        )

    return "{}"


# ── Node 0: Router (LLM-powered classification) ────────────────────────


async def router_node(state: AgentState) -> dict[str, Any]:
    """Entry node: use the LLM to classify the user request.

    The LLM is given the user's message and a description of the available
    actions. It decides whether to:

    - ``deploy`` → the request needs code generation + execution
      (e.g. "部署一个计算斐波那契的程序", "write and run a hello-world server")
    - ``question`` → the request can be answered directly with knowledge
      (e.g. "什么是Raft共识算法?", "how does Docker networking work?")
    - ``tool`` → the request is about the cluster itself
      (e.g. "topology", "show me the nodes", "cluster status")

    This replaces brittle keyword matching with genuine LLM reasoning —
    the "thinking" step in the ReAct loop.
    """
    messages = state.get("messages", [])
    user_text = ""
    for msg in reversed(messages):
        if hasattr(msg, "content") and isinstance(msg.content, str):
            user_text = msg.content
            break

    if not user_text.strip():
        return {
            "task_type": "question",
            "user_intent": "",
            "messages": [AIMessage(content="[router] empty input — defaulting to question")],
        }

    system_prompt = (
        "You are the Router agent in an edge-cloud orchestration system. "
        "Your job is to classify the user's request into exactly one of "
        "three types. Think carefully about what the user truly needs.\n\n"
        "**deploy**: The user wants CODE to be generated and executed. "
        "This includes: writing programs, running computations, deploying "
        "services, creating scripts, generating algorithms. Any request "
        "that requires producing a concrete software artifact or running "
        "code on the cluster is a deploy task.\n\n"
        "**question**: The user wants KNOWLEDGE or an EXPLANATION. "
        "This includes: asking about concepts, definitions, how-to "
        "questions, comparisons, weather queries, general chat. No code "
        "execution is needed — just a direct text answer.\n\n"
        "**tool**: The user wants to inspect or manage the CLUSTER itself. "
        "This includes: checking topology, listing nodes, viewing cluster "
        "status or health. Usually short commands (1-3 words).\n\n"
        "Respond ONLY with a JSON object (no markdown, no explanation):\n"
        '{"task_type": "<deploy|question|tool>", "reasoning": "<one short sentence>"}'
    )

    raw = await _call_llm(system_prompt, user_text, state)

    # Parse the LLM's classification
    task_type = "question"  # safe default
    reasoning = ""
    try:
        # Strip any markdown fences that might have sneaked in
        clean = _strip_markdown_fences(raw)
        parsed = json.loads(clean)
        tt = parsed.get("task_type", "").lower().strip()
        if tt in ("deploy", "question", "tool"):
            task_type = tt
        reasoning = parsed.get("reasoning", "")
    except (json.JSONDecodeError, AttributeError):
        # If the LLM returned malformed JSON, use heuristics as fallback
        raw_lower = raw.lower()
        if "deploy" in raw_lower:
            task_type = "deploy"
        elif "tool" in raw_lower:
            task_type = "tool"
        reasoning = "fallback: parsed from unstructured LLM response"

    return {
        "task_type": task_type,
        "user_intent": user_text,
        "messages": [AIMessage(
            content=f"[router] → {task_type}" +
                    (f" ({reasoning})" if reasoning else "")
        )],
    }


# ── Node 1: Direct Answer ───────────────────────────────────────────────


async def direct_answer_node(state: AgentState) -> dict[str, Any]:
    """Answer a general knowledge question directly via LLM, no code execution.

    For questions like "what's the weather?" or "explain how Raft works",
    this node calls the LLM with a conversational prompt and returns the
    response as ``final_answer``.
    """
    user_text = state.get("user_intent", "")
    if not user_text:
        user_text = ""
        for msg in reversed(state.get("messages", [])):
            if hasattr(msg, "content") and isinstance(msg.content, str):
                user_text = msg.content
                break

    system_prompt = (
        "You are the Edge-Cloud Orchestrator assistant. "
        "Answer the user's question directly and concisely. "
        "Respond in the same language as the user's question. "
        "When asked about your identity, say you are the Edge-Cloud Orchestrator "
        "cognitive agent — do NOT claim to be GPT, Llama, Claude, or any "
        "specific model; your backend provider is configured by the operator."
    )

    raw = await _call_llm(system_prompt, user_text, state)

    return {
        "final_answer": raw,
        "messages": [AIMessage(content=raw)],
    }


# ── Node 2: Planner ────────────────────────────────────────────────────


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
        "1. Parse the user's natural language intent — identify the SPECIFIC "
        "computation or task they want (e.g. 'compute fibonacci(20)', "
        "'run a web server on port 8080', 'sort this array').\n"
        "2. Review the available cluster topology (nodes, capabilities, roles).\n"
        "3. Produce a JSON execution plan with numbered steps that describe "
        "the ACTUAL computation, not generic placeholders.\n\n"
        "CRITICAL: The plan steps must be SPECIFIC and COMPUTATIONALLY MEANINGFUL. "
        "For 'compute fibonacci(20)', write steps like: "
        "'Implement iterative fibonacci function', 'Compute fib(20)', "
        "'Print the result'. NOT generic steps like 'execute step 1'.\n\n"
        "For computational tasks (math, algorithms, data processing), "
        "always prefer Python — it runs natively and is most reliable.\n\n"
        "Respond ONLY with a JSON object:\n"
        '{"intent": "<restate the user specific request>", '
        '"plan": [{"step": 1, "action": "<specific action>", "target": "<specific target>"}], '
        '"language": "python"}'
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
    user_intent = state.get("user_intent", "")
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
        "Your job is to generate executable code that solves the user's request. "
        "Output ONLY the code, no explanation, no markdown fences. "
        "Default to Python unless the plan specifies Wasm.\n\n"
        "CRITICAL RULES:\n"
        "1. IMPLEMENT THE ACTUAL ALGORITHM — do NOT write placeholder code "
        "that just prints 'step completed'. Write the real computation.\n"
        "2. Your code MUST print results to stdout using print().\n"
        "3. Define a main() function AND call it at the bottom:\n"
        "   if __name__ == '__main__': main()\n"
        "4. NEVER install packages (no pip, pip3, apt, brew, etc.).\n"
        "5. Use ONLY the Python standard library — no external dependencies.\n"
        "6. Keep the code self-contained and simple.\n"
        "7. Do NOT use subprocess or os.system to run external commands."
        + correction_context
    )

    plan_text = json.dumps(plan, indent=2)
    user_message = (
        f"USER'S ORIGINAL REQUEST:\n{user_intent}\n\n"
        f"Execution plan (for reference):\n{plan_text}\n\n"
        f"Write the code to SOLVE THE USER'S REQUEST. "
        f"The plan is guidance — the code must do the REAL computation."
    )

    raw_code = await _call_llm(system_prompt, user_message, state)

    # Strip markdown fences — LLMs often wrap code in ``` fences
    code = _strip_markdown_fences(raw_code)

    # Detect language from the actual generated code (not plan keywords)
    code_lower = code.strip().lower()
    if code_lower.startswith("(module") or code_lower.startswith("(;"):
        code_language = "wasm"      # WAT / WAT comment
    elif code_lower.startswith("def ") or code_lower.startswith("import ") or \
         code_lower.startswith("print(") or code_lower.startswith("#") or \
         code_lower.startswith("from ") or "def " in code_lower[:100]:
        code_language = "python"
    elif "fn main" in code_lower[:100] or "use " in code_lower[:100]:
        code_language = "posix"     # Rust / shell
    else:
        # Fallback: use plan hints
        plan_str = json.dumps(plan).lower()
        if "wasm" in plan_str or "wat" in plan_str:
            code_language = "wasm"
        elif "container" in plan_str or "native" in plan_str:
            code_language = "posix"
        else:
            code_language = "python"

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

    if not code.strip():
        return {
            "last_error": "No code was generated — the Coder node produced empty output",
            "messages": [AIMessage(content="Deployment skipped: no code to deploy")],
        }

    # Determine runtime from code language (not plan keywords).
    # IMPORTANT: required_runtime must be one of "Wasm", "NativePosix", or
    # "Container" — the Rust handler rejects anything else (error -32602).
    # For Python code, we use NativePosix since it runs as a native subprocess.
    # The inline executor dispatches on code_language, not required_runtime.
    if code_language in ("wasm", "wat"):
        required_runtime = "Wasm"
    elif code_language == "container":
        required_runtime = "Container"
    else:
        # python, posix, shell, or unknown → run as native process
        required_runtime = "NativePosix"

    # Routing strategy from plan hints
    plan_str = json.dumps(plan).lower()
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
        error_str = result.get("error", "unknown")
        # IPC connectivity failures → stop immediately (coder can't fix these)
        if _is_ipc_error(error_str):
            return {
                "last_error": None,  # suppress retry — coder can't fix IPC
                "exit_code": -1,
                "final_answer": (
                    f"Deployment failed: Cannot connect to the Rust node.\n"
                    f"  Error: {error_str}\n"
                    f"  Socket: {os.environ.get('EO_IPC_SOCKET', '~/.edge-orchestrator/ipc.sock')}\n\n"
                    f"Make sure the Rust node is running:\n"
                    f"  ./scripts/run_node_mac.sh --node-only\n"
                    f"Or use mock mode for testing:\n"
                    f"  eo-agent --mock --interactive"
                ),
                "messages": [AIMessage(content=f"IPC error: {error_str}")],
            }
        return {
            "last_error": f"Deployment failed: {error_str}",
            "messages": [AIMessage(content=f"Deployment error: {error_str}")],
        }

    code_hash = result.get("code_hash", "")
    task_id = result.get("task_id", "")
    result_hash = result.get("result_hash", "")  # set when inline execution ran

    return {
        "code_hash": code_hash,
        "task_id": task_id,
        "result_hash": result_hash,
        "messages": [
            AIMessage(
                content=f"Task submitted: code_hash={code_hash}, task_id={task_id}"
                + (f", result_hash={result_hash}" if result_hash else "")
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
    # If a previous node already set final_answer (e.g., IPC failure in
    # deployment_node), pass through without overwriting.
    if state.get("final_answer"):
        return {}

    code_hash = state.get("code_hash", "")
    task_id = state.get("task_id", "")
    # Prefer result_hash (set by inline executor) over code_hash (code blob)
    lookup_hash = state.get("result_hash", "") or code_hash

    if not lookup_hash:
        return {
            "last_error": "No code_hash or result_hash available — deployment may have failed",
            "final_answer": "Workflow error: nothing to evaluate. The deployment step did not produce a hash. "
                            "This may indicate that the Rust node is not running or the IPC connection failed.",
        }

    # Attempt to fetch the execution result.
    result = await fetch_execution_result.ainvoke({  # type: ignore[attr-defined]
        "result_hash": lookup_hash,
    })

    if "error" in result:
        error_str = result.get("error", "")
        # If the task was just submitted, the result might not be ready yet.
        # This is expected in a real deployment — the executor may still be
        # processing. Give a clear message and let the retry loop handle it.
        return {
            "last_error": f"Result fetch failed: {error_str}",
            "exit_code": -1,
            "messages": [AIMessage(content=f"Fetch error for {code_hash}: {error_str}")],
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
