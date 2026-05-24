"""
epicenter_export_coef.py
────────────────────────
Генерирует строки epicenter_coefficients.csv из листа «Маппінг»
и заполняет coef на основе epicenter_royalty.xlsx.

Алгоритм:
  1. Из листа «Маппінг» читаем prom_category_id, prom_category_name,
     epicenter_category_id — они становятся строками CSV
  2. В epicenter_royalty.xlsx ищем совпадение по столбику «ID категорії»
     → берём Відсоток роялті = X
     Если категория не найдена → берём coef_uncategorized из строки дефолтов
  3. Y = round(110 / (100 - (8.5 + X)), 2)
  4. Записываем Y в coef нужной строки CSV
     (строка дефолтов с пустым prom_category_id не трогается)

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
ROYALTY_PATH   = BASE_DIR / "epicenter_royalty.xlsx"
CSV_PATH       = BASE_DIR / "epicenter_coefficients.csv"

MAPPINGS_SHEET = "Маппінг"

# Заголовки столбцов (ищем позицию динамически — устойчиво к сдвигам колонок)
MAPPINGS_COL_PROM_ID      = "prom_category_id"
MAPPINGS_COL_PROM_NAME    = "Категорія Прому"
MAPPINGS_COL_EPICENTER_ID = "epicenter_category_id"

ROYALTY_COL_CATEGORY_ID = "ID категорії"
ROYALTY_COL_PERCENT     = "Відсоток роялті"

CSV_COL_CAT_ID             = "prom_category_id"
CSV_COL_CAT_NAME           = "prom_category_name"
CSV_COL_COEF               = "coef"
CSV_COL_COEF_UNCATEGORIZED = "coef_uncategorized"  # источник fallback-значения

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


def read_fallback_coef(csv_path: Path) -> str | None:
    """
    Читает значение coef_uncategorized из строки дефолтов (пустой prom_category_id).
    Возвращает строку (например '1.45') или None если не найдено / пусто.
    """
    raw    = csv_path.read_text(encoding=CSV_ENCODING)
    reader = csv.DictReader(io.StringIO(raw), delimiter=CSV_DELIMITER)

    if reader.fieldnames is None or CSV_COL_COEF_UNCATEGORIZED not in reader.fieldnames:
        return None

    for row in reader:
        if not row.get(CSV_COL_CAT_ID, "").strip():  # строка дефолтов
            coef = row.get(CSV_COL_COEF_UNCATEGORIZED, "").strip()
            return coef if coef else None

    return None


# ─────────────────────────────── loaders ─────────────────────────────────────

def load_mappings(path: Path, sheet: str) -> dict[int, tuple[str, int]]:
    """
    Читает лист «Маппінг» и возвращает
    {prom_category_id: (prom_category_name, epicenter_category_id)}.
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
    prom_name_col = _find_col_index(header, MAPPINGS_COL_PROM_NAME,    "epicenter_mappings")
    epicenter_col = _find_col_index(header, MAPPINGS_COL_EPICENTER_ID, "epicenter_mappings")

    result: dict[int, tuple[str, int]] = {}

    for row_idx, row in enumerate(rows, start=2):  # start=2 — реальный номер строки в файле
        prom_id      = _to_int(row[prom_col],      f"prom_category_id row={row_idx}")
        epicenter_id = _to_int(row[epicenter_col], f"epicenter_category_id row={row_idx}")

        if prom_id is None or epicenter_id is None:
            continue

        prom_name = str(row[prom_name_col] or "").strip()
        result[prom_id] = (prom_name, epicenter_id)

    wb.close()
    log.info("mappings: загружено %d записей (prom_id -> name, epicenter_id)", len(result))
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
    mappings: dict[int, tuple[str, int]],
    royalty: dict[int, float],
    fallback_coef: str | None,
) -> tuple[int, int]:
    """
    Генерирует строки CSV из маппинга и заполняет coef, перезаписывает файл.

    Правила для coef:
      - найден в royalty       → считаем по формуле
      - не найден в royalty    → fallback из coef_uncategorized строки дефолтов

    Порядок записи в файле:
      1. Строка дефолтов (пустой prom_category_id) — без изменений
      2. Данные из маппинга, отсортированные по prom_category_id

    Возвращает (updated, fallback_used).
    """
    raw = csv_path.read_text(encoding=CSV_ENCODING)
    reader = csv.DictReader(io.StringIO(raw), delimiter=CSV_DELIMITER)
    fieldnames = list(reader.fieldnames or [])

    if not fieldnames:
        raise RuntimeError(f"CSV {csv_path} пуст или не читается")
    for col in (CSV_COL_CAT_ID, CSV_COL_CAT_NAME, CSV_COL_COEF):
        if col not in fieldnames:
            raise RuntimeError(f"Столбик '{col}' не найден в {csv_path}")

    # Сохраняем строку дефолтов (пустой prom_category_id)
    defaults_rows = [
        row for row in reader
        if not row.get(CSV_COL_CAT_ID, "").strip()
    ]

    updated       = 0
    fallback_used = 0
    data_rows: list[dict[str, str]] = []

    for prom_id, (prom_name, epicenter_id) in sorted(mappings.items()):
        royalty_percent = royalty.get(epicenter_id)

        if royalty_percent is not None:
            try:
                coef_str = str(calc_coef(royalty_percent))
            except ValueError as exc:
                log.error(
                    "prom_id=%d epicenter_id=%d: %s — пропускаем",
                    prom_id, epicenter_id, exc,
                )
                continue
            log.info(
                "prom_id=%-6d  epicenter_id=%-6d  royalty=%-6.1f  coef=%s",
                prom_id, epicenter_id, royalty_percent, coef_str,
            )
        else:
            if fallback_coef is None:
                log.warning(
                    "prom_category_id=%d -> epicenter_category_id=%d: "
                    "не найден в royalty, fallback (coef_uncategorized) не задан — пропускаем",
                    prom_id, epicenter_id,
                )
                continue
            coef_str = fallback_coef
            log.warning(
                "prom_category_id=%d -> epicenter_category_id=%d: "
                "не найден в royalty → fallback coef_uncategorized=%s",
                prom_id, epicenter_id, coef_str,
            )
            fallback_used += 1

        row: dict[str, str] = {fn: "" for fn in fieldnames}
        row[CSV_COL_CAT_ID]   = str(prom_id)
        row[CSV_COL_CAT_NAME] = prom_name
        row[CSV_COL_COEF]     = coef_str
        data_rows.append(row)
        updated += 1

    # --- перезаписываем файл: дефолты → данные ---
    out = io.StringIO()
    writer = csv.DictWriter(
        out,
        fieldnames=fieldnames,
        delimiter=CSV_DELIMITER,
        lineterminator="\n",
        extrasaction="ignore",
    )
    writer.writeheader()
    writer.writerows(defaults_rows)
    writer.writerows(data_rows)

    csv_path.write_text(out.getvalue(), encoding=CSV_ENCODING)
    return updated, fallback_used


# ─────────────────────────────── main ─────────────────────────────────────────

def main() -> None:
    log.info("=== fill_coef_epicenter старт ===")

    for path in (MAPPINGS_PATH, ROYALTY_PATH, CSV_PATH):
        if not path.exists():
            log.error("Файл не найден: %s", path)
            sys.exit(1)

    mappings = load_mappings(MAPPINGS_PATH, MAPPINGS_SHEET)
    royalty  = load_royalty(ROYALTY_PATH)

    fallback_coef = read_fallback_coef(CSV_PATH)
    if fallback_coef:
        log.info("Fallback из строки дефолтов: coef_uncategorized=%s", fallback_coef)
    else:
        log.warning(
            "Fallback coef_uncategorized не найден в строке дефолтов CSV — "
            "категории без роялті будут пропущены"
        )

    updated, fallback_used = process_csv(CSV_PATH, mappings, royalty, fallback_coef)

    log.info(
        "=== Готово: записано=%d, fallback_использован=%d ===",
        updated, fallback_used,
    )


if __name__ == "__main__":
    main()