"""Configuration helpers shared by GetCases front ends."""

from __future__ import annotations

import json
import os
from pathlib import Path


CONFIG_PATH = Path.home() / ".config" / "courtlistener" / "config.json"
TOKEN_ENV_VAR = "COURTLISTENER_TOKEN"


def load_saved_token() -> str:
    """Return the persisted CourtListener token, or an empty string."""
    try:
        data = json.loads(CONFIG_PATH.read_text(encoding="utf-8"))
    except Exception:
        return ""
    return str(data.get("api_token") or "")


def load_token() -> str:
    """Return the environment token first, then the saved token."""
    return os.environ.get(TOKEN_ENV_VAR, "").strip() or load_saved_token()


def save_token(token: str) -> None:
    """Persist the CourtListener token for later launches."""
    try:
        CONFIG_PATH.parent.mkdir(parents=True, exist_ok=True)
        CONFIG_PATH.write_text(
            json.dumps({"api_token": token.strip()}, indent=2),
            encoding="utf-8",
        )
    except Exception:
        return
