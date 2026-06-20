"""CLI entrypoint for the eo-agent cognitive control plane.

Usage:
    eo-agent "Deploy a fibonacci calculator to the nearest Wasm node"
    eo-agent --socket /tmp/ipc.sock "What nodes are in the cluster?"

Environment variables:
    EO_IPC_SOCKET   — path to the Rust node's UDS socket
    EO_MOCK_MODE    — set to ``true`` to use mock responses (no Rust node needed)
    EO_LLM_MODEL    — override the default LLM model (e.g., ``gpt-4o``, ``claude-sonnet-4-6``)
"""

from __future__ import annotations

import argparse
import asyncio
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
        default=os.environ.get("EO_LLM_MODEL", "gpt-4o"),
        help="LLM model to use (default: gpt-4o)",
    )
    parser.add_argument(
        "--interactive", "-i",
        action="store_true",
        help="Run in interactive REPL mode",
    )
    return parser.parse_args(argv)


async def run_once(prompt: str, socket_path: str, mock_mode: bool, model: str) -> None:
    """Run a single prompt through the agent graph and print the result."""
    # Set environment for submodules
    if mock_mode:
        os.environ["EO_MOCK_MODE"] = "true"
    os.environ["EO_IPC_SOCKET"] = socket_path
    os.environ["EO_LLM_MODEL"] = model

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

    from eo_agent.graph import build_graph

    graph = build_graph()

    print("\n" + "=" * 60)
    print("  Edge-Cloud Orchestrator — Cognitive Control Plane")
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
            print("  <natural language>  — submit a task description")
            print("  topology            — show cluster topology")
            print("  help                — show this help")
            print("  quit/exit/q         — exit")
            continue
        if cmd == "topology":
            from eo_agent.tools import get_cluster_topology
            import json
            topo = await get_cluster_topology.ainvoke({})
            print(json.dumps(topo, indent=2))
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


def main() -> None:
    """Entry point for the ``eo-agent`` console script."""
    args = parse_args()

    if args.interactive:
        asyncio.run(run_interactive(args.socket, args.mock, args.model))
    elif args.prompt:
        asyncio.run(run_once(args.prompt, args.socket, args.mock, args.model))
    else:
        # No prompt, enter interactive mode
        asyncio.run(run_interactive(args.socket, args.mock, args.model))


if __name__ == "__main__":
    main()
