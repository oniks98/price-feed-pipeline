"""
kasta_export_coef.py
──────────────────
Заполняет столбик coef_kasta в markets_coefficients.csv
на основе данных из mappings.xlsx (лист «Категорія+») и royalty.xlsx.

Алгоритм для каждой строки CSV:
  1. По category_id → берём Вид*:21 из mappings.xlsx
  2. В royalty.xlsx ищем строгое совпадение (case-insensitive) по столбику Вид
     → берём max(Відсоток роялті) = X
  3. Y = round(110 / (100 - (8.5 + X)), 2)
  4. Записываем Y в coef_kasta нужной строки CSV

Запуск:
    python scripts/kasta_export_coef.py
"""

from __future__ import annotations

import csv
import io
import logging
import sys
from dataclasses import dataclass
from pathlib import Path

import openpyxl

# ─────────────────────────────── config ───────────────────────────────────────

BASE_DIR = Path(r"C:\FullStack\PriceFeedPipeline\data\markets")

MAPPINGS_PATH   = BASE_DIR / "mappings.xlsx"
ROYALTY_PATH    = BASE_DIR / "royalty.xlsx"
CSV_PATH        = BASE_DIR / "markets_coefficients.csv"

MAPPINGS_SHEET  = "Категорія+"
ROYALTY_SHEET   = "Роялті"

# Индексы столбцов (0-based)
MAPPINGS_COL_ID   = 0   # ІD категорії фіду
MAPPINGS_COL_VID  = 5   # Вид*:21

ROYALTY_COL_VID        = 2   # Вид
ROYALTY_COL_PERCENT    = 3   # Відсоток роялті

CSV_COL_CAT_ID    = "category_id"
CSV_COL_COEF_KASTA = "coef_kasta"

KASTA_FEE_PERCENT  = 8.5    # фиксированная комиссия Kasta, %
FORMULA_NUMERATOR  = 110.0  # числитель формулы

CSV_DELIMITER = ";"
CSV_ENCODING  = "utf-8-sig"   # обрабатывает BOM автоматически

# ─────────────────────────────── logging ──────────────────────────────────────

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)


# ─────────────────────────────── models ───────────────────────────────────────

@dataclass(frozen=True)
class MappingEntry:
    category_id: int
    vid: str          # Вид*:21, lower-stripped


# ─────────────────────────────── loaders ──────────────────────────────────────

def load_mappings(path: Path, sheet: str) -> dict[int, str]:
    """
    Возвращает {category_id: vid_lower} из листа «Категорія+».
    Пропускает строку заголовка и строки с пустым ID или Вид.
    """
    wb = openpyxl.load_workbook(path, data_only=True)

    if sheet not in wb.sheetnames:
        raise ValueError(f"Лист '{sheet}' не найден в {path}. Доступны: {wb.sheetnames}")

    ws = wb[sheet]
    result: dict[int, str] = {}

    for row_idx, row in enumerate(ws.iter_rows(values_only=True)):
        if row_idx == 0:   # header
            continue

        raw_id  = row[MAPPINGS_COL_ID]
        raw_vid = row[MAPPINGS_COL_VID]

        if raw_id is None or raw_vid is None:
            continue

        try:
            cat_id = int(raw_id)
        except (ValueError, TypeError):
            log.warning("mappings row %d: невалидный ID '%s' — пропускаем", row_idx, raw_id)
            continue

        vid = str(raw_vid).strip().lower()
        if not vid:
            log.warning("mappings row %d: пустой Вид для ID %d — пропускаем", row_idx, cat_id)
            continue

        result[cat_id] = vid

    wb.close()
    log.info("mappings: загружено %d записей", len(result))
    return result


def load_royalty_max(path: Path, sheet: str) -> dict[str, float]:
    """
    Возвращает {vid_lower: max_royalty_percent}.
    Строгое (case-insensitive) совпадение по столбику Вид.
    """
    wb = openpyxl.load_workbook(path, data_only=True)

    if sheet not in wb.sheetnames:
        # fallback: берём активный лист
        log.warning("Лист '%s' не найден, используем активный", sheet)
        ws = wb.active
    else:
        ws = wb[sheet]

    royalty_map: dict[str, list[float]] = {}

    for row_idx, row in enumerate(ws.iter_rows(values_only=True)):
        if row_idx == 0:   # header
            continue

        raw_vid     = row[ROYALTY_COL_VID]
        raw_percent = row[ROYALTY_COL_PERCENT]

        if raw_vid is None or raw_percent is None:
            continue

        vid = str(raw_vid).strip().lower()
        try:
            percent = float(raw_percent)
        except (ValueError, TypeError):
            continue

        royalty_map.setdefault(vid, []).append(percent)

    wb.close()

    result = {vid: max(vals) for vid, vals in royalty_map.items()}
    log.info("royalty: загружено %d уникальных видов", len(result))
    return result


# ─────────────────────────────── formula ──────────────────────────────────────

def calc_coef(max_royalty: float) -> float:
    """Y = round(110 / (100 - (8 + X)), 2)"""
    denominator = 100.0 - (KASTA_FEE_PERCENT + max_royalty)
    if denominator <= 0:
        raise ValueError(
            f"Знаменатель ≤ 0 при роялті={max_royalty}: "
            f"100 - ({KASTA_FEE_PERCENT} + {max_royalty}) = {denominator}"
        )
    return round(FORMULA_NUMERATOR / denominator, 2)


# ─────────────────────────────── CSV processing ───────────────────────────────

def process_csv(
    csv_path: Path,
    mappings: dict[int, str],
    royalty_max: dict[str, float],
) -> tuple[int, int, int]:
    """
    Читает CSV, обновляет coef_kasta в памяти, перезаписывает файл.
    Возвращает (updated, skipped_no_vid, skipped_no_royalty).
    """
    # --- читаем весь файл ---
    raw = csv_path.read_text(encoding=CSV_ENCODING)
    reader = csv.DictReader(io.StringIO(raw), delimiter=CSV_DELIMITER)
    fieldnames = reader.fieldnames

    if fieldnames is None:
        raise RuntimeError(f"CSV {csv_path} пуст или не читается")

    if CSV_COL_CAT_ID not in fieldnames:
        raise RuntimeError(f"Столбик '{CSV_COL_CAT_ID}' не найден в {csv_path}")
    if CSV_COL_COEF_KASTA not in fieldnames:
        raise RuntimeError(f"Столбик '{CSV_COL_COEF_KASTA}' не найден в {csv_path}")

    rows = list(reader)

    updated             = 0
    skipped_no_vid      = 0
    skipped_no_royalty  = 0

    for row in rows:
        raw_id = row.get(CSV_COL_CAT_ID, "").strip()

        try:
            cat_id = int(raw_id)
        except (ValueError, TypeError):
            log.warning("CSV: невалидный category_id '%s' — пропускаем", raw_id)
            skipped_no_vid += 1
            continue

        vid = mappings.get(cat_id)
        if vid is None:
            log.warning("category_id=%d: Вид не найден в mappings — пропускаем", cat_id)
            skipped_no_vid += 1
            continue

        max_royalty = royalty_max.get(vid)
        if max_royalty is None:
            log.warning(
                "category_id=%d vid='%s': не найден в royalty — пропускаем", cat_id, vid
            )
            skipped_no_royalty += 1
            continue

        try:
            coef = calc_coef(max_royalty)
        except ValueError as exc:
            log.error("category_id=%d vid='%s': %s — пропускаем", cat_id, vid, exc)
            skipped_no_royalty += 1
            continue

        row[CSV_COL_COEF_KASTA] = str(coef)
        log.info(
            "category_id=%-6d  vid=%-40s  royalty_max=%-6.1f  coef_kasta=%s",
            cat_id, vid, max_royalty, coef,
        )
        updated += 1

    # --- перезаписываем файл ---
    out = io.StringIO()
    writer = csv.DictWriter(
        out,
        fieldnames=fieldnames,
        delimiter=CSV_DELIMITER,
        lineterminator="\n",
    )
    writer.writeheader()
    writer.writerows(rows)

    csv_path.write_text(out.getvalue(), encoding=CSV_ENCODING)
    return updated, skipped_no_vid, skipped_no_royalty


# ─────────────────────────────── main ─────────────────────────────────────────

def main() -> None:
    log.info("=== fill_coef_kasta старт ===")

    for path in (MAPPINGS_PATH, ROYALTY_PATH, CSV_PATH):
        if not path.exists():
            log.error("Файл не найден: %s", path)
            sys.exit(1)

    mappings    = load_mappings(MAPPINGS_PATH, MAPPINGS_SHEET)
    royalty_max = load_royalty_max(ROYALTY_PATH, ROYALTY_SHEET)

    updated, skip_vid, skip_roy = process_csv(CSV_PATH, mappings, royalty_max)

    log.info("=== Готово: обновлено=%d, без_вида=%d, без_роялті=%d ===",
             updated, skip_vid, skip_roy)


if __name__ == "__main__":
    main()
