"""Validation and selection of the Prom browser session supplied to CI.

``PROM_STORAGE_STATE`` is preferred because Playwright storage state preserves
both cookies and origin localStorage.  ``PROM_COOKIES`` remains supported so
existing GitHub Actions secrets keep working during migration.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any


class BrowserStateError(ValueError):
    """Raised when a browser-session secret has an invalid JSON structure."""


@dataclass(frozen=True)
class PromBrowserState:
    """A validated session that can be applied to a new Playwright context."""

    source: str
    storage_state: dict[str, Any] | None
    cookies: list[dict[str, Any]] | None
    cookie_count: int
    origin_count: int


def load_prom_browser_state(
    storage_state_json: str,
    cookies_json: str,
) -> PromBrowserState | None:
    """Load a CI session without ever exposing its values in logs.

    ``PROM_STORAGE_STATE`` has priority over the legacy ``PROM_COOKIES``
    secret.  The returned object contains either a Playwright storage-state
    mapping or a legacy list of cookies, never both.
    """

    if storage_state_json.strip():
        payload = _parse_json(storage_state_json, "PROM_STORAGE_STATE")
        if not isinstance(payload, dict):
            raise BrowserStateError("PROM_STORAGE_STATE must be a JSON object")

        cookies = _validate_cookies(payload.get("cookies", []), "PROM_STORAGE_STATE.cookies")
        origins = payload.get("origins", [])
        if not isinstance(origins, list):
            raise BrowserStateError("PROM_STORAGE_STATE.origins must be a JSON list")

        return PromBrowserState(
            source="PROM_STORAGE_STATE",
            storage_state=payload,
            cookies=None,
            cookie_count=len(cookies),
            origin_count=len(origins),
        )

    if cookies_json.strip():
        payload = _parse_json(cookies_json, "PROM_COOKIES")
        cookies = _validate_cookies(payload, "PROM_COOKIES")
        return PromBrowserState(
            source="PROM_COOKIES",
            storage_state=None,
            cookies=cookies,
            cookie_count=len(cookies),
            origin_count=0,
        )

    return None


def storage_state_summary(storage_state: dict[str, Any]) -> dict[str, Any]:
    """Return a safe summary suitable for terminal output or diagnostics.

    Local-storage *values* and cookies are deliberately omitted: both can
    contain authentication material.  The keys are enough to compare the
    local export with CI diagnostics.
    """

    cookies = _validate_cookies(storage_state.get("cookies", []), "storage_state.cookies")
    origins = storage_state.get("origins", [])
    if not isinstance(origins, list):
        raise BrowserStateError("storage_state.origins must be a JSON list")

    origin_summaries: list[dict[str, Any]] = []
    for origin in origins:
        if not isinstance(origin, dict):
            continue
        local_storage = origin.get("localStorage", [])
        if not isinstance(local_storage, list):
            local_storage = []
        keys = sorted(
            item["name"]
            for item in local_storage
            if isinstance(item, dict) and isinstance(item.get("name"), str)
        )
        origin_summaries.append(
            {
                "origin": origin.get("origin", "<unknown>"),
                "local_storage_keys": keys,
            }
        )

    return {
        "cookie_count": len(cookies),
        "origin_count": len(origins),
        "origins": origin_summaries,
    }


def _parse_json(raw_value: str, secret_name: str) -> Any:
    try:
        return json.loads(raw_value)
    except json.JSONDecodeError as error:
        raise BrowserStateError(f"{secret_name} contains invalid JSON") from error


def _validate_cookies(value: Any, field_name: str) -> list[dict[str, Any]]:
    if not isinstance(value, list):
        raise BrowserStateError(f"{field_name} must be a JSON list")
    if not all(isinstance(cookie, dict) for cookie in value):
        raise BrowserStateError(f"{field_name} must contain only JSON objects")
    return value
