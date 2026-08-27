"""
prom_export_cookies.py
----------------------
Запускати ЛОКАЛЬНО після ручного входу в Prom.

Відкриває збережений профіль (pw-profile), витягує Playwright storage state
(cookies + localStorage для Prom) і зберігає його в prom_storage_state.json.

Вміст prom_storage_state.json треба додати в GitHub Secret PROM_STORAGE_STATE.
Для сумісності також створюється legacy prom_cookies.json, але він не містить
localStorage та має використовуватись лише як fallback.

Запуск:
    python scripts/prom_export_cookies.py
"""

import json
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

from playwright.sync_api import sync_playwright

try:
    from services.prom_browser_state import storage_state_summary
except ModuleNotFoundError:  # Supports both `python scripts/...` and `python -m scripts...`.
    from scripts.services.prom_browser_state import storage_state_summary

PROFILE_DIR = "./pw-profile"
START_URL = "https://my.prom.ua/cms/product"
STORAGE_STATE_FILE = Path("prom_storage_state.json")
LEGACY_COOKIES_FILE = Path("prom_cookies.json")
SUMMARY_FILE = Path("prom_storage_state_summary.json")
PROM_ROOT_DOMAIN = "prom.ua"
GITHUB_SECRET_MAX_BYTES = 48 * 1024


def _is_prom_host(value: str) -> bool:
    host = value.lstrip(".").lower()
    return host == PROM_ROOT_DOMAIN or host.endswith(f".{PROM_ROOT_DOMAIN}")


def _prom_only_storage_state(storage_state: dict[str, Any]) -> dict[str, Any]:
    """Keep only Prom data, excluding unrelated cookies from the local profile."""
    cookies = [
        cookie
        for cookie in storage_state.get("cookies", [])
        if isinstance(cookie, dict) and _is_prom_host(str(cookie.get("domain", "")))
    ]
    origins = [
        origin
        for origin in storage_state.get("origins", [])
        if isinstance(origin, dict)
        and _is_prom_host(urlparse(str(origin.get("origin", ""))).hostname or "")
    ]
    return {"cookies": cookies, "origins": origins}


def main() -> None:
    print("🔓 Відкриваємо збережений профіль...")

    with sync_playwright() as p:
        context = p.chromium.launch_persistent_context(
            PROFILE_DIR,
            headless=False,
            channel="chrome",
        )
        try:
            # Visiting the target origin makes its localStorage available to
            # Playwright storage_state even when the profile was last closed on
            # another Prom page.
            page = context.pages[0] if context.pages else context.new_page()
            page.goto(START_URL, wait_until="domcontentloaded", timeout=45_000)
            if any(part in page.url.lower() for part in ("login", "auth", "signin")):
                print("❌ Prom просить авторизацію. Увійди в кабінет і запусти експорт ще раз.")
                return
            page.wait_for_timeout(1_500)
            prom_storage_state = _prom_only_storage_state(context.storage_state())
        finally:
            context.close()

    if not prom_storage_state["cookies"]:
        print("❌ Cookies Prom не знайдено. Переконайся що ти залогінений.")
        return

    STORAGE_STATE_FILE.write_text(
        json.dumps(prom_storage_state, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    LEGACY_COOKIES_FILE.write_text(
        json.dumps(prom_storage_state["cookies"], ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    summary = storage_state_summary(prom_storage_state)
    SUMMARY_FILE.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")

    size_bytes = STORAGE_STATE_FILE.stat().st_size
    size_kb = size_bytes / 1024
    print(
        f"✅ Збережено {summary['cookie_count']} cookies і {summary['origin_count']} origin(s) "
        f"→ {STORAGE_STATE_FILE} ({size_kb:.1f} KB)"
    )
    if size_bytes > GITHUB_SECRET_MAX_BYTES:
        print(
            "⚠️ Файл більший за ліміт GitHub Secret (48 KiB). "
            "Не вставляйте його: потрібно окремо зменшити state."
        )
        return
    print()
    print("Наступний крок:")
    print(f"  Весь вміст {STORAGE_STATE_FILE} → GitHub → Settings → Secrets → PROM_STORAGE_STATE")
    print(f"  {SUMMARY_FILE} можна використати для порівняння storage keys, він не містить секретів.")
    print(f"  {LEGACY_COOKIES_FILE} створено лише для сумісності зі старими workflow.")


if __name__ == "__main__":
    main()
