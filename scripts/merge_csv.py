#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Об'єднання import_products.csv всіх активних постачальників в один merged.csv.

Використання:
  python scripts/merge_csv.py

Читає:  data/{supplier}/import_products.csv  — для кожного з SUPPLIER_CONFIG
Пише:   data/merged.csv                      — фінальний файл для Прому (raw link)

Логіка:
  - Збирає всі import_products.csv з активних постачальників
  - Перевіряє що заголовки однакові (інакше попереджає)
  - Склеює в один файл з utf-8-sig (BOM) — потрібен для Прому
  - Виводить статистику по кожному постачальнику

При додаванні нового постачальника:
  Достатньо додати його в SUPPLIER_CONFIG — merge підхопить автоматично.
"""

import csv
import os
import sys
from pathlib import Path
from typing import List, Tuple

# Підтягуємо SUPPLIER_CONFIG з update_products.py — єдине місце реєстрації
sys.path.insert(0, str(Path(__file__).parent))
sys.path.insert(0, str(Path(__file__).parent.parent))
from update_products import SUPPLIER_CONFIG
from suppliers.services.prom_csv_schema import PromCsvSchema

BASE_PATH = Path(os.environ.get("PROJECT_ROOT", r"C:\FullStack\PriceFeedPipeline"))
OUTPUT_FILE = BASE_PATH / "data" / "merged.csv"

# Еталонний заголовок з єдиного джерела правди
# specs_limit=160 повинен бути достатнім для всіх постачальників
CANONICAL_HEADERS: List[str] = PromCsvSchema.get_header(specs_limit=160)


def read_import_csv(supplier: str) -> Tuple[List[str], List[List[str]]]:
    """Читає data/{supplier}/import_products.csv. Повертає (headers, rows)."""
    file_path = BASE_PATH / "data" / supplier / "import_products.csv"

    if not file_path.exists():
        print(f"  ⚠️  {supplier}: import_products.csv не знайдено — пропускаємо")
        return [], []

    try:
        # Визначаємо кодування
        with open(file_path, "rb") as f:
            raw = f.read(4)
        encoding = "utf-8-sig" if raw.startswith(b"\xef\xbb\xbf") else "utf-8"

        with open(file_path, "r", encoding=encoding, errors="replace") as f:
            reader = csv.reader(f, delimiter=";")
            headers = next(reader, [])
            rows = list(reader)

        print(f"  ✅ {supplier}: {len(rows)} рядків")
        return headers, rows

    except Exception as e:
        print(f"  ❌ {supplier}: помилка читання — {e}")
        return [], []


def merge() -> None:
    print("\n" + "=" * 60)
    print("🔀 MERGE: об'єднання import_products.csv")
    print("=" * 60)

    all_rows: List[List[str]] = []
    stats: dict[str, int] = {}
    canonical_len = len(CANONICAL_HEADERS)

    for supplier in SUPPLIER_CONFIG:
        headers, rows = read_import_csv(supplier)

        if not headers or not rows:
            stats[supplier] = 0
            continue

        if len(headers) != canonical_len:
            print(
                f"  ⚠️  {supplier}: {len(headers)} колонок vs еталон {canonical_len} — "
                f"рядки будуть доповнені/обрізані автоматично"
            )

        for row in rows:
            # Доповнюємо короткі рядки пустими комірками, довгі — обрізаємо
            if len(row) < canonical_len:
                row = row + [""] * (canonical_len - len(row))
            all_rows.append(row[:canonical_len])

        stats[supplier] = len(rows)

    if not all_rows:
        print("\n❌ Немає даних для злиття — жоден файл не знайдено")
        sys.exit(1)

    # Записуємо merged.csv з канонічним заголовком
    # utf-8 без BOM — Пром вимагає UTF-8 без BOM для коректного імпорту
    OUTPUT_FILE.parent.mkdir(parents=True, exist_ok=True)
    with open(OUTPUT_FILE, "w", encoding="utf-8", newline="") as f:
        writer = csv.writer(f, delimiter=";")
        writer.writerow(CANONICAL_HEADERS)
        for row in all_rows:
            writer.writerow(row)

    print(f"\n{'=' * 60}")
    print("📊 СТАТИСТИКА:")
    print(f"{'=' * 60}")
    for supplier, count in stats.items():
        status = f"{count} рядків" if count > 0 else "— пропущено"
        print(f"  {supplier:<12} {status}")
    print(f"{'-' * 60}")
    print(f"  ВСЬОГО:      {len(all_rows)} рядків")
    print(f"  Файл:        {OUTPUT_FILE}")
    print(f"{'=' * 60}")


if __name__ == "__main__":
    merge()
