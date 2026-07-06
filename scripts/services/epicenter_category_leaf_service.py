"""
services/epicenter_category_leaf_service.py
---------------------------------------------
Валідація: чи придатний використаний epicenter category code для публікації.

Epicenter приймає товари лише у категорії без дочірніх (hasChild=False), які
не позначені як видалені (deleted=False). Джерело істини — лист «Категорії
Епіцентру» epicenter_mappings.xlsx, що оновлюється через
epicenter_export_categories.py (снапшот дерева категорій Epicenter).

Аркуш читається один раз (lru_cache) і роздається всім споживачам.

Публічний інтерфейс:
    is_leaf(category_code)               → bool | None   (None = невідома категорія)
    get_leaf_info(category_code)         → LeafInfo | None
    validate_used_categories(codes, ...) → LeafValidationResult

Файл даних: data/markets/epicenter_mappings.xlsx
Аркуш:      Категорії Епіцентру
Колонки:    code (0) | name_uk (1) | parentCode (2) | hasChild (3) | deleted (4)

is_leaf = (not hasChild) and (not deleted).
reason="deleted" має пріоритет над "non_leaf": видалена категорія непридатна
для публікації незалежно від того, є в неї дочірні чи ні.
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
    Path(__file__).parents[2] / "data" / "markets" / "epicenter_mappings.xlsx"
)
_SHEET_NAME: Final[str] = "Категорії Епіцентру"

# Column indices (0-based) в «Категорії Епіцентру» — мають співпадати
# з CATEGORY_COLUMNS у epicenter_export_categories.py.
_COL_CODE: Final[int] = 0        # code
_COL_NAME: Final[int] = 1        # name_uk
_COL_PARENT: Final[int] = 2      # parentCode
_COL_HAS_CHILD: Final[int] = 3   # hasChild
_COL_DELETED: Final[int] = 4     # deleted

ViolationReason = Literal["non_leaf", "deleted", "unknown"]


# ---------------------------------------------------------------------------
# Public types
# ---------------------------------------------------------------------------

class LeafInfo(TypedDict):
    category_id: str      # epicenter code
    name: str
    is_leaf: bool
    path: str              # parentCode (Epicenter не має повного шляху-breadcrumb)
    invalid_reason: ViolationReason | None   # None коли is_leaf True


@dataclass(frozen=True)
class LeafViolation:
    category_id: str
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
    def deleted(self) -> tuple[LeafViolation, ...]:
        return tuple(v for v in self.violations if v.reason == "deleted")

    @property
    def unknown(self) -> tuple[LeafViolation, ...]:
        return tuple(v for v in self.violations if v.reason == "unknown")


# ---------------------------------------------------------------------------
# Cached loader — xlsx читається рівно один раз на процес
# ---------------------------------------------------------------------------

@lru_cache(maxsize=1)
def _load_leaf_index() -> dict[str, LeafInfo]:
    """
    Повертає повний індекс {epicenter code: LeafInfo} зі снапшота
    «Категорії Епіцентру». Пропускає рядки без валідного code.
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

    result: dict[str, LeafInfo] = {}
    skipped = 0
    deleted_count = 0

    for row_idx, row in enumerate(ws.iter_rows(values_only=True), start=1):
        if row_idx == 1:
            continue  # заголовок

        code_raw = row[_COL_CODE] if len(row) > _COL_CODE else None
        if code_raw is None or str(code_raw).strip() == "":
            skipped += 1
            continue
        code = str(code_raw).strip()

        name = str(row[_COL_NAME]).strip() if len(row) > _COL_NAME and row[_COL_NAME] else ""
        parent_raw = row[_COL_PARENT] if len(row) > _COL_PARENT else None
        parent_code = str(parent_raw).strip() if parent_raw not in (None, "") else ""

        has_child = bool(row[_COL_HAS_CHILD]) if len(row) > _COL_HAS_CHILD else False
        deleted = bool(row[_COL_DELETED]) if len(row) > _COL_DELETED else False

        invalid_reason: ViolationReason | None
        if deleted:
            invalid_reason = "deleted"
            deleted_count += 1
        elif has_child:
            invalid_reason = "non_leaf"
        else:
            invalid_reason = None

        result[code] = LeafInfo(
            category_id=code,
            name=name,
            is_leaf=invalid_reason is None,
            path=parent_code,
            invalid_reason=invalid_reason,
        )

    wb.close()

    logger.info(
        "📋 Завантажено %d категорій Епіцентру (is_leaf-довідник, deleted: %d, пропущено: %d)",
        len(result), deleted_count, skipped,
    )
    return result


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def is_leaf(category_code: str) -> bool | None:
    """
    True/False якщо epicenter code є у знімку «Категорії Епіцентру».
    None — категорія відсутня у знімку (застарілий довідник або новий code,
    якого ще немає у поточному epicenter_export_categories.py).
    """
    info = _load_leaf_index().get(category_code)
    return info["is_leaf"] if info is not None else None


def get_leaf_info(category_code: str) -> LeafInfo | None:
    """Повна інформація про категорію зі знімку, або None якщо невідома."""
    return _load_leaf_index().get(category_code)


def validate_used_categories(
    category_codes: Iterable[str],
    *,
    fail_on_violation: bool = False,
) -> LeafValidationResult:
    """
    Перевіряє реально використані у фіді epicenter code (зібрані під час
    inject_epicenter_attrs) на відповідність is_leaf.

    За замовчуванням НЕ кидає виключення — повертає LeafValidationResult
    і логує підсумок, щоб одне зіпсоване маппінг-значення не валило весь фід
    (Reliability: не фейлити на одному айтемі). Виклик — генератор
    (generate_epicenter_feed.py) вирішує, друкувати підсумок чи ні;
    сама валідація і форматування логів живуть тут, а не в генераторі.

    fail_on_violation=True — підняти RuntimeError, якщо є хоч одне порушення.
    Використовується там, де потрібен hard-fail перед аплоадом (напр. CI-крок
    перевірки фіду), а не при звичайній генерації.

    reason="non_leaf" — категорія існує у знімку, але має підкатегорії
        (hasChild=True): Epicenter відхилить товар.
    reason="deleted"  — категорія позначена deleted=True у знімку
        «Категорії Епіцентру» — видалена, публікація неможлива.
    reason="unknown"  — code відсутній у поточному знімку:
        мапінг міг з'явитись раніше, ніж останній epicenter_export_categories.py,
        або довідник застарів.
    """
    index = _load_leaf_index()
    unique_codes = sorted(set(category_codes))
    violations: list[LeafViolation] = []

    for code in unique_codes:
        info = index.get(code)
        if info is None:
            violations.append(
                LeafViolation(category_id=code, name="", path="", reason="unknown")
            )
            continue
        if not info["is_leaf"]:
            violations.append(
                LeafViolation(
                    category_id=code,
                    name=info["name"],
                    path=info["path"],
                    reason=info["invalid_reason"] or "non_leaf",
                )
            )

    result = LeafValidationResult(checked=len(unique_codes), violations=tuple(violations))
    _log_result(result)

    if fail_on_violation and not result.ok:
        raise RuntimeError(
            f"Leaf-валідація провалена: {len(result.non_leaf)} не листових, "
            f"{len(result.deleted)} видалених, {len(result.unknown)} невідомих epicenter code"
        )

    return result


def _log_result(result: LeafValidationResult) -> None:
    if result.ok:
        logger.info(
            "✅ Leaf-валідація: усі %d використаних epicenter code — придатні",
            result.checked,
        )
        return

    if result.non_leaf:
        details = ", ".join(f"{v.category_id} ({v.name})" for v in result.non_leaf)
        logger.warning(
            "⚠️  Не листові epicenter code (%d/%d): %s",
            len(result.non_leaf), result.checked, details,
        )
    if result.deleted:
        details = ", ".join(f"{v.category_id} ({v.name})" for v in result.deleted)
        logger.warning(
            "⚠️  Видалені (deleted=True) epicenter code (%d/%d): %s",
            len(result.deleted), result.checked, details,
        )
    if result.unknown:
        codes_str = ", ".join(str(v.category_id) for v in result.unknown)
        logger.warning(
            "⚠️  Невідомі epicenter code (%d/%d, відсутні у знімку «Категорії Епіцентру» — "
            "запустіть epicenter_export_categories.py): %s",
            len(result.unknown), result.checked, codes_str,
        )
