"""
prom_export_cookies.py
----------------------
Запускати ЛОКАЛЬНО після ручного входу в Prom.

Відкриває збережений профіль (pw-profile), витягує cookies сесії Prom
і зберігає їх у prom_cookies.json.

Вміст prom_cookies.json треба додати в GitHub Secret PROM_COOKIES.

Запуск:
    python scripts/prom_export_cookies.py
"""

import json
from pathlib import Path
from playwright.sync_api import sync_playwright

PROFILE_DIR = "./pw-profile"
OUTPUT_FILE = Path("prom_cookies.json")
PROM_DOMAINS = ("prom.ua", "my.prom.ua")


def main() -> None:
    print("🔓 Відкриваємо збережений профіль...")

    with sync_playwright() as p:
        context = p.chromium.launch_persistent_context(
            PROFILE_DIR,
            headless=False,
            channel="chrome",
        )

        # Отримуємо всі cookies і фільтруємо тільки Prom
        all_cookies = context.cookies()
        prom_cookies = [
            c for c in all_cookies
            if any(domain in c.get("domain", "") for domain in PROM_DOMAINS)
        ]

        context.close()

    if not prom_cookies:
        print("❌ Cookies Prom не знайдено. Переконайся що ти залогінений.")
        return

    OUTPUT_FILE.write_text(
        json.dumps(prom_cookies, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )

    size_kb = OUTPUT_FILE.stat().st_size / 1024
    print(f"✅ Збережено {len(prom_cookies)} cookies → {OUTPUT_FILE} ({size_kb:.1f} KB)")
    print()
    print("Наступний крок:")
    print(f"  cat {OUTPUT_FILE}  (або відкрий у VS Code)")
    print("  Весь вміст → GitHub → Settings → Secrets → PROM_COOKIES")


if __name__ == "__main__":
    main()
