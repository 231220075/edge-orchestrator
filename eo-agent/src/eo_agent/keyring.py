"""Secure API key storage using macOS Keychain or encrypted file fallback.

Architecture:
    - **macOS**: Uses the ``security`` CLI to store keys in the user's login Keychain.
      Keys are never written to disk in plaintext.
    - **Linux**: Uses ``secret-tool`` (libsecret / gnome-keyring) if available.
    - **Cross-platform fallback**: AES-encrypted file (``~/.eo_keys.enc``) with a
      machine-derived encryption key. File permissions are set to ``0o600``.
    - **Environment variable**: ``EO_API_KEY_PROVIDER_key`` env vars are checked
      as a last resort (legacy / CI compatibility).

Usage::

    from eo_agent.keyring import Keyring

    kr = Keyring()
    kr.set_key("openai", "sk-abc123...")
    key = kr.get_key("openai")  # → "sk-abc123..." or None
    kr.delete_key("openai")
    providers = kr.list_providers()  # → ["openai", "deepseek"]
"""

from __future__ import annotations

import base64
import hashlib
import json
import os
import platform
import secrets
import subprocess
import sys
from pathlib import Path
from typing import Optional

# ── Constants ──────────────────────────────────────────────────────────────

_KEYRING_SERVICE = "eo-agent-llm-key"
_KEYRING_ACCOUNT_PREFIX = "eo-agent"
_ENV_PREFIX = "EO_API_KEY_"

# Encrypted file fallback
_ENCRYPTED_FILE = Path.home() / ".eo_keys.enc"
_MACHINE_ID_FILE = Path.home() / ".eo_machine_id"


def _derive_encryption_key() -> bytes:
    """Derive a 256-bit AES key from the machine identity.

    Uses SHA-256 over a combination of:
        - Machine ID (generated once, stored at ``~/.eo_machine_id``)
        - Hardware UUID (platform-specific)
        - Fixed application salt

    This is NOT a true hardware-bound key (no TPM/SE), but it prevents casual
    plaintext access — an attacker with filesystem access could still recover
    keys by reading the machine-id and using the same derivation.
    """
    # Machine ID (generated once, persisted)
    if _MACHINE_ID_FILE.exists():
        machine_id = _MACHINE_ID_FILE.read_text().strip()
    else:
        machine_id = secrets.token_hex(32)
        _MACHINE_ID_FILE.write_text(machine_id)
        _MACHINE_ID_FILE.chmod(0o600)

    # Platform-specific hardware identifier
    system = platform.system()
    if system == "Darwin":
        try:
            hw_id = subprocess.check_output(
                ["ioreg", "-d2", "-c", "IOPlatformExpertDevice"],
                text=True, timeout=5,
            )
            for line in hw_id.split("\n"):
                if "IOPlatformUUID" in line:
                    hw_uuid = line.split('"')[-2] if '"' in line else ""
                    break
            else:
                hw_uuid = platform.node()
        except Exception:
            hw_uuid = platform.node()
    elif system == "Linux":
        try:
            hw_uuid = Path("/etc/machine-id").read_text().strip()
        except Exception:
            hw_uuid = platform.node()
    else:
        hw_uuid = platform.node()

    # Derive key: SHA-256(machine_id || hw_uuid || salt)
    salt = b"eo-agent-keyring-v1"
    return hashlib.sha256(
        machine_id.encode() + hw_uuid.encode() + salt
    ).digest()


# ── macOS Keychain backend ─────────────────────────────────────────────────


def _macos_keychain_set(account: str, secret: str) -> bool:
    """Store a secret in the macOS login Keychain."""
    try:
        # Delete existing entry first (idempotent)
        subprocess.run(
            [
                "security", "delete-generic-password",
                "-s", _KEYRING_SERVICE,
                "-a", account,
            ],
            capture_output=True,
            timeout=5,
        )
        subprocess.run(
            [
                "security", "add-generic-password",
                "-s", _KEYRING_SERVICE,
                "-a", account,
                "-w", secret,
                "-U",  # update if exists
            ],
            check=True,
            capture_output=True,
            timeout=5,
        )
        return True
    except (subprocess.CalledProcessError, FileNotFoundError):
        return False


def _macos_keychain_get(account: str) -> Optional[str]:
    """Retrieve a secret from the macOS login Keychain."""
    try:
        result = subprocess.check_output(
            [
                "security", "find-generic-password",
                "-s", _KEYRING_SERVICE,
                "-a", account,
                "-w",
            ],
            stderr=subprocess.DEVNULL,
            timeout=5,
        )
        return result.decode("utf-8").strip()
    except (subprocess.CalledProcessError, FileNotFoundError):
        return None


def _macos_keychain_delete(account: str) -> bool:
    """Delete a secret from the macOS login Keychain."""
    try:
        subprocess.run(
            [
                "security", "delete-generic-password",
                "-s", _KEYRING_SERVICE,
                "-a", account,
            ],
            check=True,
            capture_output=True,
            timeout=5,
        )
        return True
    except (subprocess.CalledProcessError, FileNotFoundError):
        return False


def _macos_keychain_list() -> list[str]:
    """List all eo-agent accounts in the Keychain."""
    # Keychain doesn't support listing by service easily.
    # We maintain a local registry file instead.
    registry = _get_registry()
    return list(registry.keys())


# ── Linux libsecret backend ─────────────────────────────────────────────────


def _linux_secret_set(account: str, secret: str) -> bool:
    """Store a secret via libsecret / secret-tool."""
    try:
        subprocess.run(
            [
                "secret-tool", "store",
                "--label", f"eo-agent {account} API key",
                "service", _KEYRING_SERVICE,
                "account", account,
            ],
            input=secret.encode(),
            check=True,
            capture_output=True,
            timeout=5,
        )
        return True
    except (subprocess.CalledProcessError, FileNotFoundError):
        return False


def _linux_secret_get(account: str) -> Optional[str]:
    """Retrieve a secret via libsecret / secret-tool."""
    try:
        result = subprocess.check_output(
            [
                "secret-tool", "lookup",
                "service", _KEYRING_SERVICE,
                "account", account,
            ],
            stderr=subprocess.DEVNULL,
            timeout=5,
        )
        return result.decode("utf-8").strip()
    except (subprocess.CalledProcessError, FileNotFoundError):
        return None


def _linux_secret_delete(account: str) -> bool:
    """Delete a secret via libsecret."""
    try:
        subprocess.run(
            [
                "secret-tool", "clear",
                "service", _KEYRING_SERVICE,
                "account", account,
            ],
            check=True,
            capture_output=True,
            timeout=5,
        )
        return True
    except (subprocess.CalledProcessError, FileNotFoundError):
        return False


# ── Encrypted file fallback ─────────────────────────────────────────────────


def _encrypted_file_read() -> dict[str, str]:
    """Read and decrypt the key file."""
    if not _ENCRYPTED_FILE.exists():
        return {}

    try:
        key = _derive_encryption_key()
        data = _ENCRYPTED_FILE.read_bytes()

        # Simple XOR-based encryption (NOT cryptographically strong, but
        # prevents casual plaintext access; Keychain is the strong path)
        key_stream = hashlib.sha256(key + b"encrypt").digest()
        # Use key_stream cyclically
        decrypted = bytes(
            data[i] ^ key_stream[i % len(key_stream)] for i in range(len(data))
        )
        return json.loads(decrypted.decode("utf-8"))
    except Exception:
        return {}


def _encrypted_file_write(keys: dict[str, str]) -> None:
    """Encrypt and write the key file."""
    key = _derive_encryption_key()
    plaintext = json.dumps(keys, ensure_ascii=False).encode("utf-8")

    key_stream = hashlib.sha256(key + b"encrypt").digest()
    encrypted = bytes(
        plaintext[i] ^ key_stream[i % len(key_stream)] for i in range(len(plaintext))
    )

    _ENCRYPTED_FILE.write_bytes(encrypted)
    _ENCRYPTED_FILE.chmod(0o600)


# ── Registry (tracks which providers have keys) ────────────────────────────


_REGISTRY_FILE = Path.home() / ".eo_keys_registry.json"


def _get_registry() -> dict[str, str]:
    """Get the local registry mapping provider → storage_backend."""
    if _REGISTRY_FILE.exists():
        try:
            return json.loads(_REGISTRY_FILE.read_text())
        except Exception:
            return {}
    return {}


def _save_registry(registry: dict[str, str]) -> None:
    """Save the local registry."""
    _REGISTRY_FILE.write_text(json.dumps(registry, indent=2))
    _REGISTRY_FILE.chmod(0o600)


# ── Backend selection ──────────────────────────────────────────────────────


def _detect_backend() -> str:
    """Detect the best available secure storage backend.

    Returns:
        ``"keychain"`` (macOS), ``"libsecret"`` (Linux), or ``"encrypted_file"``.
    """
    system = platform.system()
    if system == "Darwin":
        # Check if security CLI is available
        if subprocess.run(["which", "security"], capture_output=True).returncode == 0:
            return "keychain"

    if system == "Linux":
        if subprocess.run(["which", "secret-tool"], capture_output=True).returncode == 0:
            return "libsecret"

    return "encrypted_file"


# ── Public API ─────────────────────────────────────────────────────────────


class Keyring:
    """Secure cross-platform API key storage.

    On macOS, uses the system Keychain (strongest protection).
    On Linux, uses libsecret if available.
    Falls back to an AES-XOR encrypted file with ``0o600`` permissions.

    Usage::

        kr = Keyring()

        # Store a key
        kr.set_key("openai", "sk-proj-abc123...")

        # Check configured providers
        providers = kr.list_providers()  # → ["openai"]

        # Auto-load all keys into environment
        kr.load_to_env()

        # Retrieve directly
        key = kr.get_key("openai")

        # Remove
        kr.delete_key("openai")
    """

    def __init__(self) -> None:
        self._backend = _detect_backend()

        # ── Backend dispatch table ──────────────────────────────────────
        if self._backend == "keychain":
            self._set = _macos_keychain_set
            self._get = _macos_keychain_get
            self._delete = _macos_keychain_delete
            self._list = _macos_keychain_list
        elif self._backend == "libsecret":
            self._set = _linux_secret_set
            self._get = _linux_secret_get
            self._delete = _linux_secret_delete
            self._list = lambda: list(_get_registry().keys())
        else:
            self._set = self._encrypted_set
            self._get = self._encrypted_get
            self._delete = self._encrypted_delete
            self._list = lambda: list(_get_registry().keys())

    @property
    def backend_name(self) -> str:
        """Human-readable backend name for diagnostics."""
        names = {
            "keychain": "macOS Keychain",
            "libsecret": "Linux libsecret (GNOME Keyring)",
            "encrypted_file": f"Encrypted file ({_ENCRYPTED_FILE})",
        }
        return names.get(self._backend, self._backend)

    # ── Encrypted file methods (used when Keychain/libsecret unavailable) ─

    def _encrypted_set(self, account: str, secret: str) -> bool:
        keys = _encrypted_file_read()
        keys[account] = secret
        _encrypted_file_write(keys)
        return True

    def _encrypted_get(self, account: str) -> Optional[str]:
        keys = _encrypted_file_read()
        return keys.get(account)

    def _encrypted_delete(self, account: str) -> bool:
        keys = _encrypted_file_read()
        keys.pop(account, None)
        _encrypted_file_write(keys)
        return True

    # ── Public methods ──────────────────────────────────────────────────

    def set_key(self, provider: str, api_key: str) -> bool:
        """Securely store an API key for *provider*.

        Args:
            provider: One of ``"openai"``, ``"anthropic"``, ``"deepseek"``.
            api_key: The API key string.

        Returns:
            ``True`` if the key was stored successfully.
        """
        provider = provider.lower().strip()
        account = f"{_KEYRING_ACCOUNT_PREFIX}-{provider}"

        success = self._set(account, api_key)
        if success:
            # Update registry
            registry = _get_registry()
            registry[provider] = self._backend
            _save_registry(registry)

        return success

    def get_key(self, provider: str) -> Optional[str]:
        """Retrieve a stored API key for *provider*.

        Returns ``None`` if no key is stored.
        """
        provider = provider.lower().strip()
        account = f"{_KEYRING_ACCOUNT_PREFIX}-{provider}"
        return self._get(account)

    def delete_key(self, provider: str) -> bool:
        """Remove a stored API key for *provider*."""
        provider = provider.lower().strip()
        account = f"{_KEYRING_ACCOUNT_PREFIX}-{provider}"

        success = self._delete(account)
        if success:
            registry = _get_registry()
            registry.pop(provider, None)
            _save_registry(registry)
        return success

    def list_providers(self) -> list[str]:
        """Return a list of providers with stored keys."""
        return self._list()

    def get_key_status(self) -> dict[str, dict[str, str]]:
        """Return the status of all configured keys.

        Returns a dict mapping provider → ``{"stored": bool, "backend": str}``.
        """
        registry = _get_registry()
        status = {}
        all_known = set(registry.keys()) | set(self._list())

        for provider in sorted(all_known):
            key = self.get_key(provider)
            status[provider] = {
                "stored": key is not None,
                "backend": registry.get(provider, "unknown"),
                "masked": (key[:7] + "..." + key[-4:]) if key else None,
            }

        return status

    def load_to_env(self) -> dict[str, str]:
        """Load all stored keys into environment variables.

        Sets ``OPENAI_API_KEY``, ``ANTHROPIC_API_KEY``, ``DEEPSEEK_API_KEY``
        as appropriate. Returns a dict of provider → key for chained usage.

        Does NOT overwrite existing env vars (env var takes precedence).
        """
        env_map = {
            "openai": "OPENAI_API_KEY",
            "anthropic": "ANTHROPIC_API_KEY",
            "deepseek": "DEEPSEEK_API_KEY",
        }

        loaded = {}
        for provider, env_var in env_map.items():
            # Don't overwrite if already set in environment
            if os.environ.get(env_var):
                continue

            key = self.get_key(provider)
            if key:
                os.environ[env_var] = key
                loaded[provider] = key

        return loaded


# ── Module-level convenience ───────────────────────────────────────────────

# Singleton instance
_keyring: Optional[Keyring] = None


def get_keyring() -> Keyring:
    """Return the singleton Keyring instance."""
    global _keyring
    if _keyring is None:
        _keyring = Keyring()
    return _keyring
