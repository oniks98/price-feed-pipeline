#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Мерж pending merged.csv з новим merged.csv перед відправкою в Prom.

Запускається в pipeline ПІСЛЯ prom_merge_csv.py і ДО prom_api_trigger.py.

Логіка:
  1. Читає prom_import_status.json через prom_import_status.
  2. Якщо status == "failed" і є pending_hashes → є дані які Prom не отримав.
  3. Завантажує merged_prev.csv (це останній merged.csv що Prom не прийняв).
  4. Мержить: merged_prev.csv як база + новий merged.csv поверх по ключу
     Ідентифікатор_товару. Новіші дані перемагають.
  5. Перезаписує merged.csv результатом мержу.
  6. Якщо status == "success" або pending порожній → нічого не робить.

Чому merged_prev.csv є базою, а не навпаки:
  merged_prev.csv містить товари які Prom реально має у своїй базі
  (останній успішний імпорт). Новий merged.csv містить свіжі зміни.
  Мержимо: беремо всі рядки prev, перезаписуємо тими що змінились у new.
  Так Prom отримає повну актуальну картину за один прийом.

Fallback:
  Якщо merged_prev.csv не знайдено — новий merged.csv залишається без змін.
  Pipeline продовжується, не падає.
"""

import csv
import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))
from prom_import_status import load_status, has_pending_imports

BASE_PATH = Path(os.environ.get("PROJECT_ROOT", r"C:\FullStack\PriceFeedPipeline"))

MERGED_CSV     = BASE_PATH / "data" / "merged.csv"
MERGED_PREV    = BASE_PATH / "data" / "merged_prev.csv"

# Також перевіряємо publish-dir (клон data-latest) — там може бути свіжіший prev
PUBLISH_PREV   = BASE_PATH.parent / "publish-dir" / "data" / "merged_prev.csv"

IDENTIFIER_COL = "Ідентифікатор_товару"
ENCODING       = "utf-8"
DELIMITER      = ";"


def _find_prev_csv() -> Path | None:
    """Повертає шлях до merged_prev.csv або None якщо не знайдено."""
    for candidate in (PUBLISH_PREV, MERGED_PREV):
        if candidate.exists():
            print(f"📂 merged_prev.csv знайдено: {candidate}")
            return candidate
    return None


def _read_csv(path: Path) -> tuple[list[str], list[dict[str, str]]]:
    """
    Читає CSV з автовизначенням BOM.
    Повертає (headers, rows_as_dicts).
    """
    with open(path, "rb") as f:
        raw = f.read(4)
    enc = "utf-8-sig" if raw.startswith(b"\xef\xbb\xbf") else ENCODING

    with open(path, newline="", encoding=enc, errors="replace") as f:
        reader = csv.DictReader(f, delimiter=DELIMITER)
        headers = reader.fieldnames or []
        rows = [dict(row) for row in reader]
    return list(headers), rows


def _write_csv(path: Path, headers: list[str], rows: list[dict[str, str]]) -> None:
    """Записує CSV в utf-8 без BOM (вимога Прому)."""
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", newline="", encoding=ENCODING) as f:
        writer = csv.DictWriter(f, fieldnames=headers, delimiter=DELIMITER, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)


def merge_with_prev(
    prev_rows: list[dict[str, str]],
    new_rows: list[dict[str, str]],
    key: str,
) -> list[dict[str, str]]:
    """
    Мерж по ключу key:
      - Базою є prev_rows (те що реально у Промі).
      - new_rows перезаписують по ключу (свіжі зміни перемагають).
      - Рядки які є в prev але відсутні в new — залишаються (вони вже є у Промі,
        їх видалення/зміна потрапила в new_rows через update_products.py).
      - Рядки яких немає в prev але є в new — додаються в кінець (нові товари).
    Порядок: спочатку всі prev (оновлені якщо є в new), потім нові яких не було в prev.
    """
    prev_index: dict[str, dict[str, str]] = {}
    for row in prev_rows:
        k = row.get(key, "").strip()
        if k:
            prev_index[k] = row

    new_index: dict[str, dict[str, str]] = {}
    for row in new_rows:
        k = row.get(key, "").strip()
        if k:
            new_index[k] = row

    result: list[dict[str, str]] = []

    # Пройшли по prev — оновлюємо якщо є в new
    for k, row in prev_index.items():
        if k in new_index:
            result.append(new_index[k])
        else:
            result.append(row)

    # Додаємо нові яких не було в prev
    for k, row in new_index.items():
        if k not in prev_index:
            result.append(row)

    return result


def main() -> None:
    print("=" * 60)
    print("🔀 MERGE PENDING — перевірка незастосованих даних")
    print("=" * 60)

    status = load_status()
    print(f"\n📋 Статус Prom: {status['status']}")
    print(f"   consecutive_failures: {status['consecutive_failures']}")
    print(f"   pending_hashes count: {len(status.get('pending_hashes') or [])}")

    if not has_pending_imports(status):
        print("\n✅ Pending відсутній — merged.csv залишається без змін")
        return

    print(f"\n⚠️  Знайдено {len(status['pending_hashes'])} незастосованих версій:")
    for entry in status["pending_hashes"]:
        print(f"   hash={entry['hash']}  run={entry['run_utc']}")

    prev_path = _find_prev_csv()
    if not prev_path:
        print("\n⚠️  merged_prev.csv не знайдено — мерж неможливий")
        print("   merged.csv залишається без змін, pipeline продовжується")
        return

    if not MERGED_CSV.exists():
        print(f"\n❌ merged.csv не знайдено: {MERGED_CSV}")
        print("   Щось пішло не так в prom_merge_csv.py")
        sys.exit(1)

    print(f"\n📂 Читаємо merged_prev.csv ({prev_path.stat().st_size // 1024} KB)...")
    prev_headers, prev_rows = _read_csv(prev_path)
    print(f"   рядків: {len(prev_rows)}")

    print(f"📂 Читаємо merged.csv ({MERGED_CSV.stat().st_size // 1024} KB)...")
    new_headers, new_rows = _read_csv(MERGED_CSV)
    print(f"   рядків: {len(new_rows)}")

    if IDENTIFIER_COL not in prev_headers:
        print(f"\n⚠️  Колонка '{IDENTIFIER_COL}' відсутня в merged_prev.csv — мерж пропущено")
        return

    print(f"\n🔀 Мержимо {len(prev_rows)} prev + {len(new_rows)} new по '{IDENTIFIER_COL}'...")
    merged_rows = merge_with_prev(prev_rows, new_rows, IDENTIFIER_COL)
    print(f"   результат: {len(merged_rows)} рядків")

    # Заголовки: використовуємо з нового merged.csv як еталону схеми
    _write_csv(MERGED_CSV, new_headers, merged_rows)
    print(f"\n✅ merged.csv перезаписано ({MERGED_CSV.stat().st_size // 1024} KB)")
    print(f"   prev: {len(prev_rows)} | new: {len(new_rows)} | result: {len(merged_rows)}")


if __name__ == "__main__":
    main()
