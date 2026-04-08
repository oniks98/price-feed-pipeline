#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Тригер імпорту товарів в Prom.ua через API після git push.

Навіщо: Пром читає CSV по розкладу раз на 4 год, нові товари створює тільки опівночі.
API дозволяє запустити повний імпорт (включаючи створення нових товарів) одразу.

Використання:
  python scripts/prom_api_trigger.py

Змінні середовища:
  PROM_API_TOKEN      — токен API Prom.ua (обов'язково, GitHub Secret)
  GITHUB_REPOSITORY   — автоматично в GitHub Actions (наприклад, user/repo)
  PROM_MERGED_CSV_URL — опційно, перевизначити URL файлу (для тестів)

Документація API:
  https://public-api.docs.prom.ua/#/Products/postProductsImportURL

Логіка updated_fields (згідно документації Prom):
  - Для нових товарів: імпортуються ВСІ поля з CSV незалежно від updated_fields
  - Для існуючих товарів: оновлюються тільки поля з updated_fields
  - "price"             → також оновлює валюту, одиницю виміру, мін. обсяг
  - "presence"          → також оновлює кількість, готовність до відправки
  - "quantity_in_stock" → явно для залишків
"""

import json
import os
import sys
import time
import urllib.error
import urllib.request

# ─────────────────────────────────────────────────────────────
# Конфігурація
# ─────────────────────────────────────────────────────────────

PROM_API_BASE = "https://my.prom.ua/api/v1"

# Поля для оновлення існуючих товарів.
# Нові товари завжди імпортуються повністю незалежно від цього списку.
UPDATED_FIELDS = [
    "price",
    "presence",
    "quantity_in_stock",
    "attributes",
]

REQUEST_TIMEOUT = 30   # сек на HTTP запит
RETRY_DELAY     = 180  # сек між retry (3 хв) — Prom може тримати імпорт 5-10 хв
RETRY_MAX       = 4    # максимум спроб (~12 хв очікування)


# ─────────────────────────────────────────────────────────────
# Допоміжні функції
# ─────────────────────────────────────────────────────────────

def get_merged_csv_url() -> str:
    """Формує raw-посилання на merged.csv в GitHub."""
    override = os.environ.get("PROM_MERGED_CSV_URL", "").strip()
    if override:
        return override

    repo = os.environ.get("GITHUB_REPOSITORY", "").strip()
    if not repo:
        repo = "oniks98/scrapy-suppliers"
        print(f"⚠️  GITHUB_REPOSITORY не задано, використовую: {repo}")

    return f"https://raw.githubusercontent.com/{repo}/main/data/merged.csv"


def prom_request(token: str, payload: dict) -> dict:
    """
    Виконує POST до /products/import_url.

    Завжди повертає dict:
      {"ok": True,  "status": 200, "json": {...}}
      {"ok": False, "status": 400, "json": {...}}

    Кидає RuntimeError тільки при мережевій помилці (URLError).
    """
    url = f"{PROM_API_BASE}/products/import_url"
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
        "Accept": "application/json",
    }
    body = json.dumps(payload).encode("utf-8")
    req  = urllib.request.Request(url, data=body, headers=headers, method="POST")

    try:
        with urllib.request.urlopen(req, timeout=REQUEST_TIMEOUT) as resp:
            return {
                "ok":     True,
                "status": resp.status,
                "json":   json.loads(resp.read().decode("utf-8")),
            }

    except urllib.error.HTTPError as e:
        raw = e.read().decode("utf-8")
        try:
            data = json.loads(raw)
        except Exception:
            data = {"raw": raw}
        return {
            "ok":     False,
            "status": e.code,
            "json":   data,
        }

    except urllib.error.URLError as e:
        raise RuntimeError(f"Мережева помилка: {e.reason}")


def is_busy_import_error(resp: dict) -> bool:
    """
    Повертає True тільки якщо Prom відповів 400 з повідомленням
    про обмеження одночасних імпортів.

    Перевіряємо JSON а не рядок — щоб не ловити зайві retry
    на інших 400 (неправильний payload, токен, CSV тощо).
    """
    if resp["status"] != 400:
        return False

    data = resp["json"]
    if not isinstance(data, dict):
        return False

    message = data.get("error", {}).get("message", "").lower()
    return "импорт" in message and "огранич" in message


# ─────────────────────────────────────────────────────────────
# Основна логіка
# ─────────────────────────────────────────────────────────────

def main() -> None:
    print("=" * 60)
    print("🚀 PROM API TRIGGER — запуск імпорту товарів")
    print("=" * 60)

    token = os.environ.get("PROM_API_TOKEN", "").strip()
    if not token:
        print("❌ PROM_API_TOKEN не задано!")
        print("   GitHub Actions: Settings → Secrets → PROM_API_TOKEN")
        sys.exit(1)

    csv_url = get_merged_csv_url()
    print(f"\n📄 CSV URL: {csv_url}")

    payload = {
        "url":            csv_url,
        "force_update":   True,   # не чекаємо розкладу Прому
        "only_available": False,  # імпортуємо всі товари
        "only_update":    False,  # дозволяємо створення нових товарів
        "updated_fields": UPDATED_FIELDS,
    }

    print(f"\n📤 POST {PROM_API_BASE}/products/import_url")
    print(f"   only_update:    False  ← нові товари будуть створені")
    print(f"   force_update:   True   ← не чекаємо розкладу")
    print(f"   updated_fields: {UPDATED_FIELDS}")

    last_error = None

    for attempt in range(1, RETRY_MAX + 1):
        resp = prom_request(token, payload)

        if resp["ok"]:
            result     = resp["json"]
            last_error = None
            break

        # Prom зайнятий попереднім імпортом — чекаємо і повторюємо
        if is_busy_import_error(resp) and attempt < RETRY_MAX:
            last_error = f"HTTP {resp['status']}: {resp['json']}"
            print(
                f"  ⚠️  [{attempt}/{RETRY_MAX}] Prom зайнятий імпортом, "
                f"чекаємо {RETRY_DELAY} сек..."
            )
            time.sleep(RETRY_DELAY)
            continue

        # Будь-яка інша помилка (401, 403, 500, невірний payload тощо) — одразу виходимо
        last_error = f"HTTP {resp['status']}: {resp['json']}"
        break

    if last_error:
        print(f"\n❌ Помилка запуску імпорту ({attempt} спроб):\n{last_error}")
        sys.exit(1)

    import_id = result.get("id") or result.get("import_id") or result.get("processId")
    print(f"\n✅ Імпорт запущено успішно!")
    if import_id:
        print(f"   import_id: {import_id}")
    print(f"   Результат: https://my.prom.ua/cms/products/import-history/")


if __name__ == "__main__":
    main()
