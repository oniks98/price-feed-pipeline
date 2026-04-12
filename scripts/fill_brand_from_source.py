#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Заповнює порожній Виробник у "без бренд.csv" значенням з "бренд.csv"
за збігом Ідентифікатор_товару.

Читає:  data/viatec/без бренд.csv   — товари без Виробника
        data/viatec/бренд.csv       — джерело брендів
Пише:   data/viatec/бренд_new.csv  — тільки ті рядки, яким знайшовся бренд
"""

import csv
import sys
from pathlib import Path

BASE = Path(__file__).parent.parent / "data" / "viatec"
NO_BRAND_FILE = BASE / "без бренд.csv"
BRAND_FILE    = BASE / "бренд.csv"
OUTPUT_FILE   = BASE / "бренд_new.csv"

COL_ID     = "Ідентифікатор_товару"
COL_BRAND  = "Виробник"
DELIMITER  = ";"


def detect_encoding(path: Path) -> str:
    with open(path, "rb") as f:
        return "utf-8-sig" if f.read(3) == b"\xef\xbb\xbf" else "utf-8"


def read_csv(path: Path) -> tuple[list[str], list[dict]]:
    enc = detect_encoding(path)
    with open(path, encoding=enc, errors="replace", newline="") as f:
        reader = csv.DictReader(f, delimiter=DELIMITER)
        headers = reader.fieldnames or []
        rows = list(reader)
    return list(headers), rows


def main() -> None:
    print(f"📂 Читаємо: {NO_BRAND_FILE.name}")
    headers, no_brand_rows = read_csv(NO_BRAND_FILE)

    print(f"📂 Читаємо: {BRAND_FILE.name}")
    _, brand_rows = read_csv(BRAND_FILE)

    # Перевірка наявності потрібних колонок
    for col in (COL_ID, COL_BRAND):
        if col not in headers:
            print(f"❌ Колонка '{col}' не знайдена в {NO_BRAND_FILE.name}")
            sys.exit(1)

    # Індекс: Ідентифікатор_товару → Виробник з бренд.csv
    # Якщо один ID зустрічається кілька разів — беремо перший непорожній
    brand_map: dict[str, str] = {}
    for row in brand_rows:
        pid   = row.get(COL_ID, "").strip()
        brand = row.get(COL_BRAND, "").strip()
        if pid and brand and pid not in brand_map:
            brand_map[pid] = brand

    print(f"✅ Брендів у довіднику: {len(brand_map)}")

    # Відбираємо рядки без Виробника і підставляємо бренд
    result: list[dict] = []
    not_found: list[str] = []

    for row in no_brand_rows:
        if row.get(COL_BRAND, "").strip():
            continue  # є бренд — пропускаємо

        pid = row.get(COL_ID, "").strip()
        brand = brand_map.get(pid)

        if brand:
            row[COL_BRAND] = brand
            result.append(row)
        else:
            not_found.append(pid)

    print(f"✅ Знайдено брендів для заповнення: {len(result)}")
    if not_found:
        print(f"⚠️  Не знайдено бренду для {len(not_found)} товарів: {not_found[:10]}{'...' if len(not_found) > 10 else ''}")

    if not result:
        print("❌ Немає рядків для запису — бренд_new.csv не створено")
        sys.exit(0)

    # Записуємо результат із тим самим заголовком
    with open(OUTPUT_FILE, "w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=headers, delimiter=DELIMITER)
        writer.writeheader()
        writer.writerows(result)

    print(f"✅ Записано: {OUTPUT_FILE} ({len(result)} рядків)")


if __name__ == "__main__":
    main()
