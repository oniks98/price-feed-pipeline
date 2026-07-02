"""
services/rozetka_category_leaf_service.py
-------------------------------------------
Валідація: чи є використаний rozetka_category_id листовою категорією.

Rozetka приймає товари лише у листові категорії (is_leaf=True). Джерело
істини — лист «Категорії Розетки» rozetka_mappings.xlsx, що оновлюється
через rozetka_export_categories.py (снапшот дерева категорій Rozetka API).

Аркуш читається один раз (lru_cache) і роздається всім споживачам.

Публічний інтерфейс:
    is_leaf(category_id)               → bool | None   (None = невідома категорія)
    get_leaf_info(category_id)         → LeafInfo | None
    validate_used_categories(ids, ...) → LeafValidationResult

Файл даних: data/markets/rozetka_mappings.xlsx
Аркуш:      Категорії Розетки
Колонки:    rozetka_category_id (0) | Назва (1) | parentCode (2) |
            Назва батьківської (3) | level (4) | is_leaf (5) | Повний шлях (6)
"""

from __future__ import annotations

import logging
from collections.abc import Iterable
from dataclasses import dataclass, field
from functools import lru_cache
from pathlib import Path
from typing import Final, Literal, TypedDict

import openpyxl

logger = logging.getLogger(__name__)

# services/ → scripts/ → project root → data/markets/
_XLSX_PATH: Final[Path] = (
    Path(__file__).parents[2] / "data" / "markets" / "rozetka_mappings.xlsx"
)
_SHEET_NAME: Final[str] = "Категорії Розетки"

# Column indices (0-based) в «Категорії Розетки» — мають співпадати
# з CATEGORY_COLUMNS у rozetka_export_categories.py.
_COL_CAT_ID: Final[int] = 0    # rozetka_category_id
_COL_NAME: Final[int] = 1      # Назва категорії Розетки
_COL_LEVEL: Final[int] = 4     # level
_COL_IS_LEAF: Final[int] = 5   # is_leaf
_COL_PATH: Final[int] = 6      # Повний шлях категорії

ViolationReason = Literal["non_leaf", "unknown"]


# ---------------------------------------------------------------------------
# Public types
# ---------------------------------------------------------------------------

class LeafInfo(TypedDict):
    category_id: int
    name: str
    is_leaf: bool
    path: str


@dataclass(frozen=True)
class LeafViolation:
    category_id: int
    name: str
    path: str
    reason: ViolationReason


@dataclass(frozen=True)
class LeafValidationResult:
    checked: int
    violations: tuple[LeafViolation, ...] = field(default_factory=tuple)

    @property
    def ok(self) -> bool:
        return not self.violations

    @property
    def non_leaf(self) -> tuple[LeafViolation, ...]:
        return tuple(v for v in self.violations if v.reason == "non_leaf")

    @property
    def unknown(self) -> tuple[LeafViolation, ...]:
        return tuple(v for v in self.violations if v.reason == "unknown")


# ---------------------------------------------------------------------------
# Cached loader — xlsx читається рівно один раз на процес
# ---------------------------------------------------------------------------

@lru_cache(maxsize=1)
def _load_leaf_index() -> dict[int, LeafInfo]:
    """
    Повертає повний індекс {rozetka_category_id: LeafInfo} зі снапшота
    «Категорії Розетки». Пропускає рядки без валідного category_id.
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

    result: dict[int, LeafInfo] = {}
    skipped = 0

    for row_idx, row in enumerate(ws.iter_rows(values_only=True), start=1):
        if row_idx == 1:
            continue  # заголовок

        cat_id_raw = row[_COL_CAT_ID] if len(row) > _COL_CAT_ID else None
        if cat_id_raw is None:
            skipped += 1
            continue

        try:
            cat_id = int(cat_id_raw)
        except (ValueError, TypeError):
            logger.warning(
                "Рядок %d: неможливо конвертувати rozetka_category_id=%r — пропущено",
                row_idx, cat_id_raw,
            )
            skipped += 1
            continue

        name = str(row[_COL_NAME]).strip() if len(row) > _COL_NAME and row[_COL_NAME] else ""
        path = str(row[_COL_PATH]).strip() if len(row) > _COL_PATH and row[_COL_PATH] else ""
        is_leaf_raw = row[_COL_IS_LEAF] if len(row) > _COL_IS_LEAF else None

        result[cat_id] = LeafInfo(
            category_id=cat_id,
            name=name,
            is_leaf=bool(is_leaf_raw),
            path=path,
        )

    wb.close()

    logger.info(
        "📋 Завантажено %d категорій Розетки (is_leaf-довідник, пропущено: %d)",
        len(result), skipped,
    )
    return result


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def is_leaf(category_id: int) -> bool | None:
    """
    True/False якщо rozetka_category_id є у знімку «Категорії Розетки».
    None — категорія відсутня у знімку (застарілий довідник або новий id,
    якого ще немає у поточному rozetka_export_categories.py).
    """
    info = _load_leaf_index().get(category_id)
    return info["is_leaf"] if info is not None else None


def get_leaf_info(category_id: int) -> LeafInfo | None:
    """Повна інформація про категорію зі знімку, або None якщо невідома."""
    return _load_leaf_index().get(category_id)


def validate_used_categories(
    category_ids: Iterable[int],
    *,
    fail_on_violation: bool = False,
) -> LeafValidationResult:
    """
    Перевіряє реально використані у фіді rozetka_category_id (зібрані під час
    replace_category_ids) на відповідність is_leaf.

    За замовчуванням НЕ кидає виключення — повертає LeafValidationResult
    і логує підсумок, щоб одне зіпсоване маппінг-значення не валило весь фід
    (Reliability: не фейлити на одному айтемі). Виклик — генератор
    (generate_rozetka_feed.py) вирішує, друкувати підсумок чи ні;
    сама валідація і форматування логів живуть тут, а не в генераторі.

    fail_on_violation=True — підняти RuntimeError, якщо є хоч одне порушення.
    Використовується там, де потрібен hard-fail перед аплоадом (напр. CI-крок
    перевірки фіду), а не при звичайній генерації.

    reason="non_leaf" — категорія існує у знімку, але має підкатегорії:
        Rozetka відхилить товар («Проблеми з категорією»).
    reason="unknown"  — category_id відсутній у поточному знімку:
        мапінг міг з'явитись раніше, ніж останній rozetka_export_categories.py,
        або довідник застарів.
    """
    index = _load_leaf_index()
    unique_ids = sorted(set(category_ids))
    violations: list[LeafViolation] = []

    for cat_id in unique_ids:
        info = index.get(cat_id)
        if info is None:
            violations.append(
                LeafViolation(category_id=cat_id, name="", path="", reason="unknown")
            )
            continue
        if not info["is_leaf"]:
            violations.append(
                LeafViolation(
                    category_id=cat_id,
                    name=info["name"],
                    path=info["path"],
                    reason="non_leaf",
                )
            )

    result = LeafValidationResult(checked=len(unique_ids), violations=tuple(violations))
    _log_result(result)

    if fail_on_violation and not result.ok:
        raise RuntimeError(
            f"Leaf-валідація провалена: {len(result.non_leaf)} не листових, "
            f"{len(result.unknown)} невідомих rozetka_category_id"
        )

    return result


def _log_result(result: LeafValidationResult) -> None:
    if result.ok:
        logger.info(
            "✅ Leaf-валідація: усі %d використаних rozetka_category_id — листові",
            result.checked,
        )
        return

    if result.non_leaf:
        details = ", ".join(f"{v.category_id} ({v.name})" for v in result.non_leaf)
        logger.warning(
            "⚠️  Не листові rozetka_category_id (%d/%d): %s",
            len(result.non_leaf), result.checked, details,
        )
    if result.unknown:
        ids_str = ", ".join(str(v.category_id) for v in result.unknown)
        logger.warning(
            "⚠️  Невідомі rozetka_category_id (%d/%d, відсутні у знімку «Категорії Розетки» — "
            "запустіть rozetka_export_categories.py): %s",
            len(result.unknown), result.checked, ids_str,
        )
