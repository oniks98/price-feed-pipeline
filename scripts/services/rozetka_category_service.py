"""
services/rozetka_category_service.py
--------------------------------------
Єдина точка читання листа «Маппінг» з rozetka_mappings.xlsx.

Аркуш читається один раз (lru_cache) і роздається всім споживачам.
Доступний інтерфейс:
    get_category_map()                        → dict[int, CategoryEntry]
    get_category(prom_category_id)            → CategoryEntry | None
    build_categories_xml(entries)             → str

CategoryEntry = {"category_id": int, "name": str}
  category_id — rozetka_category_id (числовий ідентифікатор категорії Розетки)
  name        — назва категорії Розетки

Файл даних: data/markets/rozetka_mappings.xlsx
Аркуш:      Маппінг
Колонки:    prom_category_id (col 0) | rozetka_category_id (col 2) | Назва (col 3)
"""

from __future__ import annotations

import logging
from collections.abc import Iterable
from functools import lru_cache
from pathlib import Path
from typing import Final, TypedDict

import openpyxl

logger = logging.getLogger(__name__)

# services/ → scripts/ → project root → data/markets/
_XLSX_PATH: Final[Path] = (
    Path(__file__).parents[2] / "data" / "markets" / "rozetka_mappings.xlsx"
)
_SHEET_NAME: Final[str] = "Маппінг"

# Column indices (0-based) in «Маппінг»
_COL_PROM_CAT_ID: Final[int] = 0    # prom_category_id
_COL_ROZ_CAT_ID: Final[int] = 2     # rozetka_category_id
_COL_ROZ_NAME: Final[int] = 3       # Назва категорії Розетки


# ---------------------------------------------------------------------------
# Public types
# ---------------------------------------------------------------------------

class CategoryEntry(TypedDict):
    category_id: int   # rozetka_category_id
    name: str          # Назва категорії Розетки


# ---------------------------------------------------------------------------
# Cached loader — xlsx читається рівно один раз на процес
# ---------------------------------------------------------------------------

@lru_cache(maxsize=1)
def _load_category_map() -> dict[int, CategoryEntry]:
    """
    Повертає повний індекс {prom_category_id: CategoryEntry}.
    Пропускає рядки без prom_category_id або rozetka_category_id.
    """
    if not _XLSX_PATH.exists():
        raise FileNotFoundError(
            f"rozetka_mappings.xlsx не знайдено: {_XLSX_PATH}"
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
        roz_id_raw  = row[_COL_ROZ_CAT_ID]
        roz_name_raw = row[_COL_ROZ_NAME]

        if prom_id_raw is None or roz_id_raw is None:
            skipped += 1
            continue

        try:
            prom_id = int(prom_id_raw)
            roz_id  = int(roz_id_raw)
        except (ValueError, TypeError):
            logger.warning(
                "Рядок %d: неможливо конвертувати prom_id=%r / rozetka_id=%r — пропущено",
                row_idx, prom_id_raw, roz_id_raw,
            )
            skipped += 1
            continue

        roz_name = str(roz_name_raw).strip() if roz_name_raw else ""

        result[prom_id] = CategoryEntry(category_id=roz_id, name=roz_name)

    wb.close()

    logger.info(
        "📋 Завантажено %d категорій Розетки (пропущено: %d)",
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


def build_categories_xml(entries: Iterable[CategoryEntry]) -> str:
    """
    Будує XML-блок <categories> з переданих CategoryEntry.

    Вхід — лише реально використані у фіді категорії (зібрані під час
    replace_category_ids). Сортуємо за category_id для детермінованого виводу.

    Приклад виводу:
        <categories>
            <category id="391">Куртки для хлопчиків</category>
            <category id="80100">Акустичні системи</category>
        </categories>
    """
    sorted_entries = sorted(entries, key=lambda e: e["category_id"])

    if not sorted_entries:
        logger.warning("build_categories_xml: передано порожній список категорій")
        return "<categories/>"

    lines = ["<categories>"]
    for entry in sorted_entries:
        lines.append(f'    <category id="{entry["category_id"]}">{entry["name"]}</category>')
    lines.append("</categories>")
    return "\n".join(lines)
