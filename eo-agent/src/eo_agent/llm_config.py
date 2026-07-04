"""LLM provider configuration manager.

Reads and writes ``~/.eo_llm_config.yaml`` — the user's personal LLM provider
registry. Each provider specifies a **base_url**, **model**, and an
**api_key_source** reference (the key itself is NEVER stored in this file).

Schema::

    active_provider: "deepseek"
    providers:
      deepseek:
        base_url: "https://api.deepseek.com/v1"
        model: "deepseek-chat"
        api_key_source: "keyring"       # "keyring" | "env:VARNAME" | "none" | "plaintext:KEY"
      ollama:
        base_url: "http://localhost:11434/v1"
        model: "llama3.1"
        api_key_source: "none"

Usage::

    from eo_agent.llm_config import LLMConfig

    cfg = LLMConfig()
    active = cfg.get_active()          # → {"base_url": "...", "model": "...", ...}
    cfg.set_active("ollama")
    cfg.add_provider("local", "http://10.0.0.5:8080/v1", "mistral", "none")
"""

from __future__ import annotations

import os
import sys
from pathlib import Path
from typing import Any

import yaml

# ── Constants ──────────────────────────────────────────────────────────────

_CONFIG_PATH = Path.home() / ".eo_llm_config.yaml"

# Sensible defaults for well-known providers (used by auto_generate).
_KNOWN_PROVIDERS: dict[str, dict[str, str]] = {
    "openai": {
        "base_url": "https://api.openai.com/v1",
        "model": "gpt-4o",
        "api_key_source": "keyring",
    },
    "deepseek": {
        "base_url": "https://api.deepseek.com/v1",
        "model": "deepseek-chat",
        "api_key_source": "keyring",
    },
    "anthropic": {
        "base_url": "https://api.anthropic.com",
        "model": "claude-sonnet-4-6",
        "api_key_source": "keyring",
    },
}

# Valid api_key_source prefixes for validation
_VALID_KEY_SOURCES = ("keyring", "env:", "none", "plaintext:")


# ── Public API ─────────────────────────────────────────────────────────────


class LLMConfig:
    """Read/write the user's LLM provider configuration.

    The config file lives at ``~/.eo_llm_config.yaml`` with ``0o600``
    permissions.  API keys are never written into this file — only a
    reference describing where to find them (``keyring``, ``env:VAR``,
    ``none``, or ``plaintext:KEY``).
    """

    def __init__(self, path: Path | None = None) -> None:
        self._path = path or _CONFIG_PATH
        self._data: dict[str, Any] = self._load()

    # ── File I/O ──────────────────────────────────────────────────────────

    def _load(self) -> dict[str, Any]:
        """Load config from disk. Returns empty dict if file doesn't exist."""
        if not self._path.exists():
            return {}
        try:
            with open(self._path, "r", encoding="utf-8") as fh:
                data = yaml.safe_load(fh) or {}
            return data
        except Exception:
            return {}

    def _save(self) -> None:
        """Persist current config to disk with restrictive permissions."""
        self._path.parent.mkdir(parents=True, exist_ok=True)
        with open(self._path, "w", encoding="utf-8") as fh:
            yaml.safe_dump(self._data, fh, default_flow_style=False, allow_unicode=True)
        self._path.chmod(0o600)

    @property
    def config_path(self) -> Path:
        """Path to the config file (read-only)."""
        return self._path

    # ── Provider queries ──────────────────────────────────────────────────

    def get_active(self) -> dict[str, str] | None:
        """Return the currently active provider config dict, or *None*.

        Returns a dict with keys ``base_url``, ``model``, ``api_key_source``.
        """
        active_name = self._data.get("active_provider")
        if not active_name:
            return None
        providers = self._data.get("providers", {})
        return providers.get(active_name)

    def get_active_name(self) -> str | None:
        """Return the name of the currently active provider."""
        return self._data.get("active_provider")

    def list_providers(self) -> dict[str, dict[str, str]]:
        """Return all configured providers as ``{name: {base_url, model, ...}}``."""
        return dict(self._data.get("providers", {}))

    def get_provider(self, name: str) -> dict[str, str] | None:
        """Return a single provider config by name, or *None*."""
        return self._data.get("providers", {}).get(name)

    # ── Provider management ───────────────────────────────────────────────

    def set_active(self, name: str) -> bool:
        """Switch the active provider to *name*.

        Returns ``True`` on success, ``False`` if the provider doesn't exist.
        """
        if name not in self._data.get("providers", {}):
            return False
        self._data["active_provider"] = name
        self._save()
        return True

    def add_provider(
        self,
        name: str,
        base_url: str,
        model: str,
        api_key_source: str = "keyring",
    ) -> None:
        """Add (or overwrite) a provider configuration.

        Args:
            name: Short name for the provider (e.g. ``"deepseek"``, ``"ollama"``).
            base_url: API base URL (must include the ``/v1`` path if applicable).
            model: Model identifier string.
            api_key_source: Where to find the API key.  One of:
                - ``"keyring"`` — fetch from secure Keyring under *name*
                - ``"env:VARNAME"`` — read from environment variable
                - ``"none"`` — no authentication (local LLM, Ollama)
                - ``"plaintext:sk-..."`` — inline key (NOT recommended;
                  ``keyring`` is preferred)
        """
        name = name.lower().strip()
        self._validate_key_source(api_key_source)

        providers = self._data.setdefault("providers", {})
        providers[name] = {
            "base_url": base_url.rstrip("/"),
            "model": model,
            "api_key_source": api_key_source,
        }

        # Auto-select as active if this is the first provider
        if not self._data.get("active_provider"):
            self._data["active_provider"] = name

        self._save()

    def remove_provider(self, name: str) -> bool:
        """Remove a provider.  Returns ``False`` if it didn't exist."""
        name = name.lower().strip()
        providers = self._data.get("providers", {})
        if name not in providers:
            return False

        del providers[name]

        # If we removed the active provider, pick another (or clear)
        if self._data.get("active_provider") == name:
            remaining = list(providers.keys())
            self._data["active_provider"] = remaining[0] if remaining else None

        self._save()
        return True

    def edit_provider(
        self,
        name: str,
        base_url: str | None = None,
        model: str | None = None,
        api_key_source: str | None = None,
    ) -> bool:
        """Edit an existing provider's settings.  Only provided fields are updated.

        Returns ``False`` if the provider doesn't exist.
        """
        name = name.lower().strip()
        providers = self._data.get("providers", {})
        if name not in providers:
            return False

        if api_key_source is not None:
            self._validate_key_source(api_key_source)
            providers[name]["api_key_source"] = api_key_source
        if base_url is not None:
            providers[name]["base_url"] = base_url.rstrip("/")
        if model is not None:
            providers[name]["model"] = model

        self._save()
        return True

    # ── API key resolution ────────────────────────────────────────────────

    def resolve_api_key(self, provider_name: str | None = None) -> str | None:
        """Resolve the actual API key for a provider.

        Looks up the ``api_key_source`` and fetches the real key from
        the appropriate location.  Returns *None* if no key is configured
        or the provider uses ``"none"`` auth.

        Args:
            provider_name: Provider to resolve.  Defaults to the active provider.
        """
        name = provider_name or self._data.get("active_provider")
        if not name:
            return None

        provider = self._data.get("providers", {}).get(name)
        if not provider:
            return None

        source = provider.get("api_key_source", "keyring")

        if source == "none":
            return None

        if source == "keyring":
            try:
                from eo_agent.keyring import get_keyring
                kr = get_keyring()
                return kr.get_key(name)
            except Exception:
                return None

        if source.startswith("plaintext:"):
            return source[len("plaintext:"):]

        if source.startswith("env:"):
            var_name = source[len("env:"):]
            return os.environ.get(var_name)

        # Auto-detect: if source looks like a raw API key (e.g. "sk-..." or "sk-ant-..."),
        # treat it as inline plaintext for convenience.
        if source.startswith("sk-") or source.startswith("sk-ant-"):
            return source

        # Unknown source — warn once and return None
        _warn_once_source(source, name)
        return None

    # ── Auto-generation ───────────────────────────────────────────────────

    def auto_generate(self) -> bool:
        """Generate an initial config from existing keyring keys.

        Scans the keyring for known providers (openai, deepseek, anthropic)
        and creates entries with sensible defaults.  Sets the first found
        provider as active.

        Returns ``True`` if any providers were auto-configured.
        """
        try:
            from eo_agent.keyring import get_keyring
            kr = get_keyring()
            stored = kr.list_providers()
        except Exception:
            stored = []

        if not stored:
            return False

        created = 0
        for provider_name in stored:
            if provider_name in _KNOWN_PROVIDERS:
                defaults = dict(_KNOWN_PROVIDERS[provider_name])
                providers = self._data.setdefault("providers", {})
                if provider_name not in providers:
                    providers[provider_name] = defaults
                    created += 1

        if created > 0:
            if not self._data.get("active_provider"):
                self._data["active_provider"] = stored[0]
            self._save()

            print(
                f"\n  \033[1;32m✓ Auto-generated LLM config from keyring "
                f"({created} provider(s))\033[0m\n"
                f"  \033[2mConfig: {self._path}\033[0m\n"
                f"  \033[2mActive: {self._data['active_provider']}\033[0m\n"
                f"  \033[2mManage: eo-agent --llm-status | --llm-list | --llm-add | --llm-use\033[0m\n",
                file=sys.stderr,
            )
            return True

        return False

    @staticmethod
    def _validate_key_source(source: str) -> None:
        """Raise ``ValueError`` if *source* is not a recognised format."""
        if any(source.startswith(prefix) for prefix in _VALID_KEY_SOURCES):
            return
        valid = ", ".join(f'"{p}"' for p in _VALID_KEY_SOURCES)
        raise ValueError(
            f"Invalid api_key_source '{source}'. "
            f"Must be one of: {valid} (or plaintext:KEY, env:VARNAME)"
        )


# ── One-shot warning gate ──────────────────────────────────────────────────

_WARNED_SOURCES: set[str] = set()


def _warn_once_source(source: str, provider: str) -> None:
    """Print a warning about an unknown key source at most once per process."""
    key = f"{provider}:{source}"
    if key in _WARNED_SOURCES:
        return
    _WARNED_SOURCES.add(key)
    print(
        f"  \033[1;33m⚠ Unknown api_key_source '{source}' "
        f"for provider '{provider}'\033[0m",
        file=sys.stderr,
    )


# ── Module-level convenience ───────────────────────────────────────────────

_config: LLMConfig | None = None


def get_llm_config() -> LLMConfig:
    """Return the singleton LLMConfig instance."""
    global _config
    if _config is None:
        _config = LLMConfig()
    return _config
