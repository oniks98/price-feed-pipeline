"""
services/epicenter_category_service.py
---------------------------------------
Єдина точка читання листа «Маппінг» з epicenter_mappings.xlsx.

Аркуш читається один раз (lru_cache) і роздається всім споживачам.
Доступний інтерфейс:
    get_category_map() → dict[int, CategoryEntry]
    get_category(prom_category_id) → CategoryEntry | None

CategoryEntry = {"code": str, "name": str}
  code — epicenter_category_id (рядком, відповідає set_code у Сетах атрибутів)
  name — назва категорії Епіцентру
"""

from __future__ import annotations

import logging
from functools import lru_cache
from pathlib import Path
from typing import Final, TypedDict

import openpyxl

logger = logging.getLogger(__name__)

# services/ → scripts/ → project root → data/markets/
_XLSX_PATH: Final[Path] = (
    Path(__file__).parents[2] / "data" / "markets" / "epicenter_mappings.xlsx"
)
_SHEET_NAME: Final[str] = "Маппінг"

# Column indices (0-based) in «Маппінг»
_COL_PROM_CAT_ID: Final[int] = 0    # prom_category_id
_COL_EPI_CAT_ID: Final[int] = 2     # epicenter_category_id  (= set_code)
_COL_EPI_NAME: Final[int] = 3       # Назва категорії Епіцентру


# ---------------------------------------------------------------------------
# Public types
# ---------------------------------------------------------------------------

class CategoryEntry(TypedDict):
    code: str   # epicenter_category_id / set_code
    name: str   # Назва категорії Епіцентру


# ---------------------------------------------------------------------------
# Cached loader — xlsx читається рівно один раз на процес
# ---------------------------------------------------------------------------

@lru_cache(maxsize=1)
def _load_category_map() -> dict[int, CategoryEntry]:
    """
    Повертає повний індекс {prom_category_id: CategoryEntry}.
    Пропускає рядки без prom_category_id або epicenter_category_id.
    """
    if not _XLSX_PATH.exists():
        raise FileNotFoundError(
            f"epicenter_mappings.xlsx не знайдено: {_XLSX_PATH}"
        )

    wb = openpyxl.load_workbook(_XLSX_PATH, read_only=True, data_only=True)
    try:
        ws = wb[_SHEET_NAME]
    except KeyError:
        raise KeyError(f"Аркуш «{_SHEET_NAME}» не знайдено у {_XLSX_PATH}")

    result: dict[int, CategoryEntry] = {}
    skipped = 0

    for row_idx, row in enumerate(ws.iter_rows(values_only=True), start=1):
        if row_idx == 1:
            continue  # заголовок

        prom_id_raw = row[_COL_PROM_CAT_ID]
        epi_id_raw = row[_COL_EPI_CAT_ID]
        epi_name_raw = row[_COL_EPI_NAME]

        if prom_id_raw is None or epi_id_raw is None:
            skipped += 1
            continue

        try:
            prom_id = int(prom_id_raw)
            epi_code = str(int(epi_id_raw))  # нормалізуємо до рядка без .0
        except (ValueError, TypeError):
            logger.warning(
                "Рядок %d: неможливо конвертувати prom_id=%r / epi_id=%r — пропущено",
                row_idx, prom_id_raw, epi_id_raw,
            )
            skipped += 1
            continue

        epi_name = str(epi_name_raw).strip() if epi_name_raw else ""

        result[prom_id] = CategoryEntry(code=epi_code, name=epi_name)

    wb.close()

    logger.info(
        "📋 Завантажено %d категорій Маппінгу (пропущено: %d)",
        len(result), skipped,
    )
    return result


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def get_category_map() -> dict[int, CategoryEntry]:
    """Повний індекс {prom_category_id: CategoryEntry}."""
    return _load_category_map()


def get_category(prom_category_id: int) -> CategoryEntry | None:
    """
    Повертає CategoryEntry для prom_category_id або None якщо немає маппінгу.
    Не кидає виключення — безпечно для поштучної обробки товарів.
    """
    return _load_category_map().get(prom_category_id)
