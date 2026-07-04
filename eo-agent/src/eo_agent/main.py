"""CLI entrypoint for the eo-agent cognitive control plane.

Usage:
    eo-agent "Deploy a fibonacci calculator to the nearest Wasm node"
    eo-agent --socket /tmp/ipc.sock "What nodes are in the cluster?"

Key management:
    eo-agent --set-key openai sk-proj-abc123...
    eo-agent --set-key deepseek sk-abc123...
    eo-agent --set-key anthropic sk-ant-abc123...
    eo-agent --key-status
    eo-agent --remove-key openai

Environment variables:
    EO_IPC_SOCKET   — path to the Rust node's UDS socket
    EO_MOCK_MODE    — set to ``true`` to use mock responses (no Rust node needed)
    EO_LLM_MODEL    — override the LLM model (default: from active provider config)
    EO_LLM_BASE_URL — custom API base URL (for DeepSeek / Ollama / local models)
    OPENAI_API_KEY  — OpenAI API key (also checked as fallback)
    DEEPSEEK_API_KEY— DeepSeek API key (also checked as fallback)
    ANTHROPIC_API_KEY — Anthropic API key (also checked as fallback)
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import sys
from pathlib import Path

from langchain_core.messages import HumanMessage


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        prog="eo-agent",
        description="Cognitive Control Plane for Edge-Cloud Orchestrator",
    )
    parser.add_argument(
        "prompt",
        nargs="?",
        help="Natural language instruction for the agent",
    )
    parser.add_argument(
        "--socket",
        default=os.environ.get("EO_IPC_SOCKET", "~/.edge-orchestrator/ipc.sock"),
        help="Path to the Rust node UDS socket (default: ~/.edge-orchestrator/ipc.sock)",
    )
    parser.add_argument(
        "--mock",
        action="store_true",
        default=os.environ.get("EO_MOCK_MODE", "").lower() in ("1", "true", "yes"),
        help="Run with mock IPC client (no Rust node required)",
    )
    parser.add_argument(
        "--model",
        default=None,
        help="LLM model override (default: from active provider config)",
    )
    parser.add_argument(
        "--interactive", "-i",
        action="store_true",
        help="Run in interactive REPL mode",
    )
    # ── Key management ──────────────────────────────────────────────
    parser.add_argument(
        "--set-key",
        nargs=2,
        metavar=("PROVIDER", "KEY"),
        help="Securely store an API key (e.g., --set-key openai sk-...)",
    )
    parser.add_argument(
        "--key-status",
        action="store_true",
        help="Show which API keys are configured and where they are stored",
    )
    parser.add_argument(
        "--remove-key",
        metavar="PROVIDER",
        help="Remove a stored API key (e.g., --remove-key openai)",
    )
    # ── LLM provider management ────────────────────────────────────
    parser.add_argument(
        "--llm-status",
        action="store_true",
        help="Show current LLM provider, model, and base URL",
    )
    parser.add_argument(
        "--llm-list",
        action="store_true",
        help="List all configured LLM providers",
    )
    parser.add_argument(
        "--llm-add",
        nargs=3,
        metavar=("NAME", "BASE_URL", "MODEL"),
        help="Add an LLM provider (e.g., --llm-add ollama http://localhost:11434/v1 llama3.1)",
    )
    parser.add_argument(
        "--key-source",
        default="keyring",
        metavar="SOURCE",
        help="API key source for --llm-add: keyring (default), env:VAR, none, plaintext:KEY",
    )
    parser.add_argument(
        "--llm-use",
        metavar="NAME",
        help="Switch the active LLM provider",
    )
    parser.add_argument(
        "--llm-remove",
        metavar="NAME",
        help="Remove an LLM provider configuration",
    )
    parser.add_argument(
        "--llm-edit",
        metavar="NAME",
        help="Edit an existing LLM provider (use with --base-url, --model, --key-source)",
    )
    parser.add_argument(
        "--base-url",
        default=None,
        metavar="URL",
        help="API base URL (used with --llm-add or --llm-edit)",
    )
    return parser.parse_args(argv)


async def run_once(prompt: str, socket_path: str, mock_mode: bool, model: str) -> None:
    """Run a single prompt through the agent graph and print the result."""
    # Set environment for submodules
    if mock_mode:
        os.environ["EO_MOCK_MODE"] = "true"
    os.environ["EO_IPC_SOCKET"] = socket_path
    os.environ["EO_LLM_MODEL"] = model

    # Load keys from keyring
    _load_keys_from_keyring()

    from eo_agent.graph import build_graph

    graph = build_graph()

    initial_state = {
        "messages": [HumanMessage(content=prompt)],
        "retry_count": 0,
    }

    print(f"\n{'─' * 60}")
    print(f"  eo-agent: {prompt[:50]}{'...' if len(prompt) > 50 else ''}")
    print(f"{'─' * 60}\n")

    config = {"configurable": {"thread_id": "cli-single"}}

    result = await graph.ainvoke(initial_state, config)

    # Print the final answer
    final = result.get("final_answer", "No final answer produced.")
    print(final)

    # If there were retries, summarise
    retry_count = result.get("retry_count", 0)
    if retry_count > 0:
        print(f"\n(Completed after {retry_count} retry attempt(s))")

    print()


async def run_interactive(socket_path: str, mock_mode: bool, model: str) -> None:
    """Run an interactive REPL session."""
    if mock_mode:
        os.environ["EO_MOCK_MODE"] = "true"
    os.environ["EO_IPC_SOCKET"] = socket_path
    os.environ["EO_LLM_MODEL"] = model

    # Load keys from keyring
    keyring_keys = _load_keys_from_keyring()

    from eo_agent.graph import build_graph

    graph = build_graph()

    print("\n" + "=" * 60)
    print("  Edge-Cloud Orchestrator — Cognitive Control Plane")
    if keyring_keys:
        print(f"  Keys loaded: {', '.join(keyring_keys)}")
    print("  Type 'help' for commands, 'quit' to exit")
    print("=" * 60 + "\n")

    while True:
        try:
            prompt = input("eo> ").strip()
        except (EOFError, KeyboardInterrupt):
            print("\nGoodbye.")
            break

        if not prompt:
            continue

        cmd = prompt.lower()
        if cmd in ("quit", "exit", "q"):
            print("Goodbye.")
            break
        if cmd == "help":
            print("Commands:")
            print("  <natural language>  — ask a question OR deploy a task")
            print("    Deploy: 部署计算斐波那契数列的程序，返回第20项")
            print("    Ask:    什么是Raft共识算法？")
            print("  topology            — show cluster topology")
            print("  llm                 — show current LLM provider status")
            print("  llm list            — list all configured LLM providers")
            print("  llm use <name>      — switch LLM provider")
            print("  help                — show this help")
            print("  quit/exit/q         — exit")
            continue
        if cmd == "topology":
            from eo_agent.tools import get_cluster_topology
            topo = await get_cluster_topology.ainvoke({})
            print(json.dumps(topo, indent=2))
            continue
        if cmd == "llm" or cmd.startswith("llm "):
            _handle_llm_repl(cmd)
            continue

        initial_state = {
            "messages": [HumanMessage(content=prompt)],
            "retry_count": 0,
        }

        print("Processing...")
        config = {"configurable": {"thread_id": "cli-interactive"}}
        result = await graph.ainvoke(initial_state, config)

        final = result.get("final_answer", "No final answer produced.")
        print(f"\n{final}\n")


# ── Key management commands ─────────────────────────────────────────────


def _load_keys_from_keyring() -> list[str]:
    """Load API keys from secure keyring into environment.

    Returns a list of provider names that were loaded.
    """
    try:
        from eo_agent.keyring import get_keyring
        kr = get_keyring()
        loaded = kr.load_to_env()
        return list(loaded.keys())
    except Exception:
        return []


def cmd_set_key(provider: str, api_key: str) -> None:
    """Handle ``--set-key`` command."""
    try:
        from eo_agent.keyring import get_keyring
    except ImportError:
        print("Error: keyring module not available.", file=sys.stderr)
        sys.exit(1)

    provider = provider.lower().strip()
    valid_providers = {"openai", "anthropic", "deepseek"}
    if provider not in valid_providers:
        print(
            f"Error: Unknown provider '{provider}'. "
            f"Valid providers: {', '.join(sorted(valid_providers))}",
            file=sys.stderr,
        )
        sys.exit(1)

    kr = get_keyring()
    success = kr.set_key(provider, api_key)

    if success:
        print(f"✓ API key for '{provider}' stored securely in {kr.backend_name}.")
    else:
        print(
            f"✗ Failed to store key for '{provider}' in {kr.backend_name}.",
            file=sys.stderr,
        )
        sys.exit(1)


def cmd_key_status() -> None:
    """Handle ``--key-status`` command."""
    try:
        from eo_agent.keyring import get_keyring
    except ImportError:
        print("Error: keyring module not available.", file=sys.stderr)
        sys.exit(1)

    kr = get_keyring()
    status = kr.get_key_status()

    if not status:
        print("No API keys configured.")
        print(f"\nBackend: {kr.backend_name}")
        print("\nSet a key with:")
        print("  eo-agent --set-key openai sk-proj-...")
        print("  eo-agent --set-key deepseek sk-...")
        print("  eo-agent --set-key anthropic sk-ant-...")
        return

    print(f"API Key Status (backend: {kr.backend_name}):\n")
    print(f"  {'Provider':<12} {'Stored':<8} {'Masked Key':<24} {'Backend':<16}")
    print(f"  {'─'*12} {'─'*8} {'─'*24} {'─'*16}")
    for provider, info in sorted(status.items()):
        stored = "✓" if info["stored"] else "✗"
        masked = info.get("masked") or "—"
        backend = info.get("backend", "—")
        print(f"  {provider:<12} {stored:<8} {masked:<24} {backend:<16}")

    # Also check env vars
    env_keys = []
    for var in ("OPENAI_API_KEY", "ANTHROPIC_API_KEY", "DEEPSEEK_API_KEY"):
        if os.environ.get(var):
            env_keys.append(var)
    if env_keys:
        print(f"\n  Additional keys in environment: {', '.join(env_keys)}")


def cmd_remove_key(provider: str) -> None:
    """Handle ``--remove-key`` command."""
    try:
        from eo_agent.keyring import get_keyring
    except ImportError:
        print("Error: keyring module not available.", file=sys.stderr)
        sys.exit(1)

    provider = provider.lower().strip()
    kr = get_keyring()

    if kr.delete_key(provider):
        print(f"✓ API key for '{provider}' removed from {kr.backend_name}.")
    else:
        print(f"✗ No key found for '{provider}'.", file=sys.stderr)
        sys.exit(1)


def cmd_llm_status() -> None:
    """Handle ``--llm-status`` command."""
    try:
        from eo_agent.llm_config import get_llm_config
    except ImportError:
        print("Error: llm_config module not available.", file=sys.stderr)
        sys.exit(1)

    cfg = get_llm_config()
    active = cfg.get_active()
    active_name = cfg.get_active_name()

    if not active_name or not active:
        print("No LLM provider configured.")
        print(f"\nConfig file: {cfg.config_path}")
        print("\nAdd a provider:")
        print("  eo-agent --llm-add <name> <base_url> <model> [--key-source SOURCE]")
        print("\nExamples:")
        print("  eo-agent --llm-add deepseek https://api.deepseek.com/v1 deepseek-chat")
        print("  eo-agent --llm-add ollama http://localhost:11434/v1 llama3.1 --key-source none")
        print("  eo-agent --llm-add openai https://api.openai.com/v1 gpt-4o")
        return

    print(f"Active LLM Provider: {active_name}")
    print(f"  Model:    {active.get('model', '—')}")
    print(f"  Base URL: {active.get('base_url', '—')}")
    print(f"  Key:      {active.get('api_key_source', '—')}")

    # Resolve actual key status
    api_key = cfg.resolve_api_key(active_name)
    if api_key:
        masked = api_key[:7] + "..." + api_key[-4:] if len(api_key) > 12 else "****"
        print(f"  Key mask: {masked}")
    else:
        source = active.get("api_key_source", "keyring")
        if source == "none":
            print(f"  Key mask: (no auth)")
        else:
            print(f"  Key mask: NOT FOUND")

    print(f"\nConfig file: {cfg.config_path}")
    providers = cfg.list_providers()
    if len(providers) > 1:
        print(f"Other providers: {', '.join(p for p in providers if p != active_name)}")


def cmd_llm_list() -> None:
    """Handle ``--llm-list`` command."""
    try:
        from eo_agent.llm_config import get_llm_config
    except ImportError:
        print("Error: llm_config module not available.", file=sys.stderr)
        sys.exit(1)

    cfg = get_llm_config()
    providers = cfg.list_providers()
    active_name = cfg.get_active_name()

    if not providers:
        print("No LLM providers configured.")
        print(f"\nConfig file: {cfg.config_path}")
        print("\nAdd one with:")
        print("  eo-agent --llm-add <name> <base_url> <model> [--key-source SOURCE]")
        return

    print(f"LLM Providers (active: {active_name or 'none'}):\n")
    print(f"  {'Name':<14} {'Model':<22} {'Base URL':<42} {'Key Source':<16}")
    print(f"  {'─'*14} {'─'*22} {'─'*42} {'─'*16}")
    for name, info in sorted(providers.items()):
        marker = " *" if name == active_name else "  "
        model = info.get("model", "—")[:20]
        base = info.get("base_url", "—")[:40]
        source = info.get("api_key_source", "—")[:14]
        print(f"  {marker} {name:<12} {model:<22} {base:<42} {source:<16}")


def cmd_llm_add(name: str, base_url: str, model: str, key_source: str) -> None:
    """Handle ``--llm-add`` command."""
    try:
        from eo_agent.llm_config import get_llm_config
    except ImportError:
        print("Error: llm_config module not available.", file=sys.stderr)
        sys.exit(1)

    cfg = get_llm_config()
    try:
        cfg.add_provider(name, base_url, model, key_source)
    except ValueError as exc:
        print(f"Error: {exc}", file=sys.stderr)
        sys.exit(1)

    active_marker = ""
    if cfg.get_active_name() == name.lower().strip():
        active_marker = " (active)"
    print(f"✓ Provider '{name}' added{active_marker}.")
    print(f"  Model: {model}  |  Base URL: {base_url}  |  Key: {key_source}")
    print(f"  Config: {cfg.config_path}")


def cmd_llm_use(name: str) -> None:
    """Handle ``--llm-use`` command."""
    try:
        from eo_agent.llm_config import get_llm_config
    except ImportError:
        print("Error: llm_config module not available.", file=sys.stderr)
        sys.exit(1)

    cfg = get_llm_config()
    if cfg.set_active(name):
        active = cfg.get_active()
        print(f"✓ Switched to provider '{name}'.")
        if active:
            print(f"  Model: {active.get('model')}  |  Base URL: {active.get('base_url')}")
    else:
        print(f"✗ Provider '{name}' not found.", file=sys.stderr)
        providers = cfg.list_providers()
        if providers:
            print(f"  Available: {', '.join(providers.keys())}")
        else:
            print("  No providers configured. Add one: eo-agent --llm-add")
        sys.exit(1)


def cmd_llm_remove(name: str) -> None:
    """Handle ``--llm-remove`` command."""
    try:
        from eo_agent.llm_config import get_llm_config
    except ImportError:
        print("Error: llm_config module not available.", file=sys.stderr)
        sys.exit(1)

    cfg = get_llm_config()
    if cfg.remove_provider(name):
        new_active = cfg.get_active_name()
        suffix = f" (active is now: {new_active})" if new_active else " (no active provider)"
        print(f"✓ Provider '{name}' removed.{suffix}")
    else:
        print(f"✗ Provider '{name}' not found.", file=sys.stderr)
        sys.exit(1)


def cmd_llm_edit(
    name: str,
    base_url: str | None = None,
    model: str | None = None,
    key_source: str | None = None,
) -> None:
    """Handle ``--llm-edit`` command."""
    try:
        from eo_agent.llm_config import get_llm_config
    except ImportError:
        print("Error: llm_config module not available.", file=sys.stderr)
        sys.exit(1)

    if not base_url and not model and not key_source:
        print("Error: --llm-edit requires at least one of --base-url, --model, --key-source",
              file=sys.stderr)
        sys.exit(1)

    cfg = get_llm_config()

    if not cfg.get_provider(name):
        print(f"✗ Provider '{name}' not found.", file=sys.stderr)
        providers = cfg.list_providers()
        if providers:
            print(f"  Available: {', '.join(providers.keys())}")
        sys.exit(1)

    changed = []
    if base_url:
        changed.append(f"base_url → {base_url}")
    if model:
        changed.append(f"model → {model}")
    if key_source:
        changed.append(f"key_source → {key_source}")

    if cfg.edit_provider(name, base_url=base_url, model=model, api_key_source=key_source):
        print(f"✓ Provider '{name}' updated: {', '.join(changed)}")

        # Show updated config
        updated = cfg.get_provider(name)
        if updated:
            print(f"  Model: {updated.get('model')}  |  Base URL: {updated.get('base_url')}  |  Key: {updated.get('api_key_source')}")
    else:
        print(f"✗ Failed to update '{name}'.", file=sys.stderr)
        sys.exit(1)


def _handle_llm_repl(cmd: str) -> None:
    """Handle ``llm`` commands inside the interactive REPL."""
    try:
        from eo_agent.llm_config import get_llm_config
    except ImportError:
        print("LLM config module not available.")
        return

    cfg = get_llm_config()
    parts = cmd.split()

    if len(parts) == 1 or (len(parts) == 2 and parts[1] == "status"):
        # llm / llm status
        active_name = cfg.get_active_name()
        active = cfg.get_active()
        if not active_name or not active:
            print("No LLM provider configured.")
            print("Set one up from the shell:")
            print("  eo-agent --llm-add <name> <base_url> <model> [--key-source SOURCE]")
            return
        print(f"Active provider: {active_name}")
        print(f"  Model:    {active.get('model', '—')}")
        print(f"  Base URL: {active.get('base_url', '—')}")
        print(f"  Key:      {active.get('api_key_source', '—')}")
        api_key = cfg.resolve_api_key(active_name)
        if api_key and len(api_key) > 12:
            print(f"  Key mask: {api_key[:7]}...{api_key[-4:]}")
        elif api_key:
            print(f"  Key mask: ****")
        else:
            source = active.get("api_key_source", "keyring")
            if source == "none":
                print(f"  Key mask: (no auth)")
            else:
                print(f"  Key mask: NOT FOUND")
        return

    if len(parts) == 2 and parts[1] == "list":
        # llm list
        providers = cfg.list_providers()
        active_name = cfg.get_active_name()
        if not providers:
            print("No LLM providers configured.")
            return
        print(f"LLM Providers (active: {active_name or 'none'}):")
        for name, info in sorted(providers.items()):
            marker = "*" if name == active_name else " "
            print(f"  [{marker}] {name}")
            print(f"      model={info.get('model')}  base_url={info.get('base_url')}  key={info.get('api_key_source')}")
        return

    if len(parts) >= 3 and parts[1] == "use":
        # llm use <name>
        name = parts[2]
        if cfg.set_active(name):
            active = cfg.get_active()
            print(f"✓ Switched to provider '{name}'.")
            if active:
                print(f"  Model: {active.get('model')}  |  Base URL: {active.get('base_url')}")
        else:
            print(f"✗ Provider '{name}' not found.")
            providers = cfg.list_providers()
            if providers:
                print(f"  Available: {', '.join(providers.keys())}")
        return

    print(f"Unknown llm command: {cmd}")
    print("Usage: llm | llm status | llm list | llm use <name>")


def main() -> None:
    """Entry point for the ``eo-agent`` console script."""
    args = parse_args()

    # ── Key management commands (exit after handling) ─────────────────
    if args.set_key:
        cmd_set_key(args.set_key[0], args.set_key[1])
        return

    if args.key_status:
        cmd_key_status()
        return

    if args.remove_key:
        cmd_remove_key(args.remove_key)
        return

    # ── LLM provider management commands (exit after handling) ────────
    if args.llm_status:
        cmd_llm_status()
        return

    if args.llm_list:
        cmd_llm_list()
        return

    if args.llm_add:
        cmd_llm_add(args.llm_add[0], args.llm_add[1], args.llm_add[2], args.key_source)
        return

    if args.llm_use:
        cmd_llm_use(args.llm_use)
        return

    if args.llm_remove:
        cmd_llm_remove(args.llm_remove)
        return

    if args.llm_edit:
        # Pass all three — edit_provider is idempotent for unchanged values
        cmd_llm_edit(
            args.llm_edit,
            base_url=args.base_url,
            model=args.model,
            key_source=args.key_source,
        )
        return

    # ── Normal operation ──────────────────────────────────────────────
    # Resolve model: CLI flag > env var > default
    model = args.model or os.environ.get("EO_LLM_MODEL") or ""
    if args.interactive:
        asyncio.run(run_interactive(args.socket, args.mock, model))
    elif args.prompt:
        asyncio.run(run_once(args.prompt, args.socket, args.mock, model))
    else:
        # No prompt, enter interactive mode
        asyncio.run(run_interactive(args.socket, args.mock, model))


if __name__ == "__main__":
    main()
