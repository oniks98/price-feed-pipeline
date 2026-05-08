"""
epicenter_export_coef.py
────────────────────────
Заполняет столбик coef_epicenter в markets_coefficients.csv
на основе данных из epicenter_mappings.xlsx (лист «Маппінг»)
и royalty_epicenter.xlsx.

Алгоритм для каждой строки CSV:
  1. По prom_category_id → берём epicenter_category_id из листа «Маппінг»
  2. В royalty_epicenter.xlsx ищем совпадение по столбику «ID категорії»
     → берём Відсоток роялті = X
  3. Y = round(110 / (100 - (8.5 + X)), 2)
  4. Записываем Y в coef_epicenter нужной строки CSV

Запуск:
    python scripts/epicenter_export_coef.py
"""

from __future__ import annotations

import csv
import io
import logging
import sys
from pathlib import Path
from typing import Optional

import openpyxl

# ─────────────────────────────── config ───────────────────────────────────────

BASE_DIR = Path(r"C:\FullStack\PriceFeedPipeline\data\markets")

MAPPINGS_PATH  = BASE_DIR / "epicenter_mappings.xlsx"
ROYALTY_PATH   = BASE_DIR / "royalty_epicenter.xlsx"
CSV_PATH       = BASE_DIR / "markets_coefficients.csv"

MAPPINGS_SHEET = "Маппінг"

# Заголовки столбцов (ищем позицию динамически — устойчиво к сдвигам колонок)
MAPPINGS_COL_PROM_ID      = "prom_category_id"
MAPPINGS_COL_EPICENTER_ID = "epicenter_category_id"

ROYALTY_COL_CATEGORY_ID = "ID категорії"
ROYALTY_COL_PERCENT     = "Відсоток роялті"

CSV_COL_CAT_ID         = "category_id"
CSV_COL_COEF_EPICENTER = "coef_epicenter"

EPICENTER_FEE_PERCENT = 8.5    # фиксированная комиссия Epicenter, %
FORMULA_NUMERATOR     = 110.0  # числитель формулы

CSV_DELIMITER = ";"
CSV_ENCODING  = "utf-8-sig"    # обрабатывает BOM автоматически

# ─────────────────────────────── logging ──────────────────────────────────────

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)


# ─────────────────────────────── helpers ──────────────────────────────────────

def _find_col_index(header_row: tuple, col_name: str, source: str) -> int:
    """
    Возвращает 0-based индекс столбца по имени заголовка.
    Поиск case-insensitive, с удалением пробелов.
    Бросает ValueError если столбец не найден.
    """
    normalized = col_name.strip().lower()
    for idx, cell in enumerate(header_row):
        if cell is not None and str(cell).strip().lower() == normalized:
            return idx
    raise ValueError(
        f"[{source}] Столбик '{col_name}' не найден. "
        f"Доступные: {[c for c in header_row if c is not None]}"
    )


def _to_int(value: object, label: str) -> Optional[int]:
    """Безопасное приведение к int. Возвращает None при ошибке."""
    try:
        return int(value)  # type: ignore[arg-type]
    except (ValueError, TypeError):
        log.warning("Не удалось преобразовать '%s' в int (%s)", value, label)
        return None


def _to_float(value: object, label: str) -> Optional[float]:
    """Безопасное приведение к float. Возвращает None при ошибке."""
    try:
        return float(value)  # type: ignore[arg-type]
    except (ValueError, TypeError):
        log.warning("Не удалось преобразовать '%s' в float (%s)", value, label)
        return None


# ─────────────────────────────── loaders ──────────────────────────────────────

def load_mappings(path: Path, sheet: str) -> dict[int, int]:
    """
    Читает лист «Маппінг» и возвращает {prom_category_id: epicenter_category_id}.
    Позиции столбцов определяются по заголовкам — устойчиво к добавлению колонок.
    Пропускает строки с отсутствующими или невалидными ID.
    """
    wb = openpyxl.load_workbook(path, data_only=True)

    if sheet not in wb.sheetnames:
        raise ValueError(
            f"Лист '{sheet}' не найден в {path}. "
            f"Доступные листы: {wb.sheetnames}"
        )

    ws = wb[sheet]
    rows = ws.iter_rows(values_only=True)

    header = next(rows, None)
    if header is None:
        raise RuntimeError(f"Лист '{sheet}' в {path} пуст")

    prom_col      = _find_col_index(header, MAPPINGS_COL_PROM_ID,      "epicenter_mappings")
    epicenter_col = _find_col_index(header, MAPPINGS_COL_EPICENTER_ID, "epicenter_mappings")

    result: dict[int, int] = {}

    for row_idx, row in enumerate(rows, start=2):  # start=2 — реальный номер строки в файле
        prom_id      = _to_int(row[prom_col],      f"prom_category_id row={row_idx}")
        epicenter_id = _to_int(row[epicenter_col], f"epicenter_category_id row={row_idx}")

        if prom_id is None or epicenter_id is None:
            continue

        result[prom_id] = epicenter_id

    wb.close()
    log.info("mappings: загружено %d записей (prom_id -> epicenter_id)", len(result))
    return result


def load_royalty(path: Path) -> dict[int, float]:
    """
    Читает royalty_epicenter.xlsx и возвращает {epicenter_category_id: royalty_percent}.
    Лист определяется автоматически (активный).
    При дублирующихся ID берём максимальный процент.
    """
    wb = openpyxl.load_workbook(path, data_only=True)
    ws = wb.active

    rows = ws.iter_rows(values_only=True)

    header = next(rows, None)
    if header is None:
        raise RuntimeError(f"Файл {path} пуст")

    cat_col     = _find_col_index(header, ROYALTY_COL_CATEGORY_ID, "royalty_epicenter")
    percent_col = _find_col_index(header, ROYALTY_COL_PERCENT,     "royalty_epicenter")

    royalty_map: dict[int, list[float]] = {}

    for row_idx, row in enumerate(rows, start=2):
        cat_id  = _to_int(row[cat_col],       f"ID категорії row={row_idx}")
        percent = _to_float(row[percent_col], f"Відсоток роялті row={row_idx}")

        if cat_id is None or percent is None:
            continue

        royalty_map.setdefault(cat_id, []).append(percent)

    wb.close()

    result = {cat_id: max(vals) for cat_id, vals in royalty_map.items()}
    log.info("royalty: загружено %d уникальных категорий Epicenter", len(result))
    return result


# ─────────────────────────────── formula ──────────────────────────────────────

def calc_coef(royalty_percent: float) -> float:
    """Y = round(110 / (100 - (8.5 + X)), 2)"""
    denominator = 100.0 - (EPICENTER_FEE_PERCENT + royalty_percent)
    if denominator <= 0:
        raise ValueError(
            f"Знаменатель <= 0 при роялті={royalty_percent}: "
            f"100 - ({EPICENTER_FEE_PERCENT} + {royalty_percent}) = {denominator}"
        )
    return round(FORMULA_NUMERATOR / denominator, 2)


# ─────────────────────────────── CSV processing ───────────────────────────────

def process_csv(
    csv_path: Path,
    mappings: dict[int, int],
    royalty: dict[int, float],
) -> tuple[int, int, int]:
    """
    Читает CSV, обновляет coef_epicenter в памяти, перезаписывает файл.
    Возвращает (updated, skipped_no_mapping, skipped_no_royalty).
    """
    raw = csv_path.read_text(encoding=CSV_ENCODING)
    reader = csv.DictReader(io.StringIO(raw), delimiter=CSV_DELIMITER)
    fieldnames = reader.fieldnames

    if fieldnames is None:
        raise RuntimeError(f"CSV {csv_path} пуст или не читается")
    if CSV_COL_CAT_ID not in fieldnames:
        raise RuntimeError(f"Столбик '{CSV_COL_CAT_ID}' не найден в {csv_path}")
    if CSV_COL_COEF_EPICENTER not in fieldnames:
        raise RuntimeError(f"Столбик '{CSV_COL_COEF_EPICENTER}' не найден в {csv_path}")

    rows = list(reader)

    updated            = 0
    skipped_no_mapping = 0
    skipped_no_royalty = 0

    for row in rows:
        raw_id  = row.get(CSV_COL_CAT_ID, "").strip()
        prom_id = _to_int(raw_id, f"CSV category_id='{raw_id}'")

        if prom_id is None:
            log.warning("CSV: невалидный category_id '%s' — пропускаем", raw_id)
            skipped_no_mapping += 1
            continue

        epicenter_id = mappings.get(prom_id)
        if epicenter_id is None:
            log.warning(
                "prom_category_id=%d: epicenter_category_id не найден в маппинге — пропускаем",
                prom_id,
            )
            skipped_no_mapping += 1
            continue

        royalty_percent = royalty.get(epicenter_id)
        if royalty_percent is None:
            log.warning(
                "prom_category_id=%d -> epicenter_category_id=%d: "
                "не найден в royalty_epicenter — пропускаем",
                prom_id, epicenter_id,
            )
            skipped_no_royalty += 1
            continue

        try:
            coef = calc_coef(royalty_percent)
        except ValueError as exc:
            log.error(
                "prom_id=%d epicenter_id=%d: %s — пропускаем",
                prom_id, epicenter_id, exc,
            )
            skipped_no_royalty += 1
            continue

        row[CSV_COL_COEF_EPICENTER] = str(coef)
        log.info(
            "prom_id=%-6d  epicenter_id=%-6d  royalty=%-6.1f  coef_epicenter=%s",
            prom_id, epicenter_id, royalty_percent, coef,
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
    return updated, skipped_no_mapping, skipped_no_royalty


# ─────────────────────────────── main ─────────────────────────────────────────

def main() -> None:
    log.info("=== fill_coef_epicenter старт ===")

    for path in (MAPPINGS_PATH, ROYALTY_PATH, CSV_PATH):
        if not path.exists():
            log.error("Файл не найден: %s", path)
            sys.exit(1)

    mappings = load_mappings(MAPPINGS_PATH, MAPPINGS_SHEET)
    royalty  = load_royalty(ROYALTY_PATH)

    updated, skip_map, skip_roy = process_csv(CSV_PATH, mappings, royalty)

    log.info(
        "=== Готово: обновлено=%d, без_маппинга=%d, без_роялті=%d ===",
        updated, skip_map, skip_roy,
    )


if __name__ == "__main__":
    main()