"""
services/rozetka_category_service.py
--------------------------------------
Єдина точка читання листа «Маппінг» з rozetka_mappings.xlsx.

Аркуш читається один раз (lru_cache) і роздається всім споживачам.

Публічний інтерфейс:
    resolve_category(prom_category_id, prom_params, offer_id=None) -> CategoryEntry | None
    get_category(prom_category_id)                                  -> CategoryEntry | None  (compat, без param-фільтра)
    get_category_map()                                               -> dict[int, CategoryEntry]
    build_categories_xml(entries)                                    -> str
    flush_fallback_warnings()                                        -> None

CategoryEntry = {"category_id": int, "name": str}

Логіка resolve_category (в порядку пріоритету) — той самий підхід,
що і в services/epicenter_category_service.py (param-based routing):
  1. Правила з param_names: шукаємо в prom_params за іменем аліасу prom_param_name;
     кожне sub-value офера (розбиття значення параметра по ",") порівнюємо
     з prom_option_name. Перше збіжне правило виграє.
     Приклад: prom_category_id=237071 «Камери відеоспостереження» має кілька рядків
     з prom_param_name="Тип пристрою" і різними prom_option_name (IP-камери,
     HDCVI відеокамери, ...) → кожен веде на свій листовий rozetka_category_id.
  2. Plain-правила (param_names порожні) — перше таке правило є fallback,
     якщо жодне param-правило не збіглося.
  3. Якщо для prom_category_id є ЛИШЕ param-правила і жодне не збіглося →
     conservative fallback на перше правило (rules[0]) + накопичений warning
     (виводиться одним зведеним логом через flush_fallback_warnings()).

Файл даних: data/markets/rozetka_mappings.xlsx
Аркуш:      Маппінг
Колонки (fixed index — той самий підхід, що і epicenter_category_service.py;
порядок колонок у xlsx зафіксовано і навмисно НЕ змінюється):
    prom_category_id (0) | Категорія Прому (1) | rozetka_category_id (2) |
    Назва категорії Розетки (3) | parentCode (4) | prom_param_name (5) | prom_option_name (6)
"""

from __future__ import annotations

import logging
from collections import Counter
from collections.abc import Iterable
from dataclasses import dataclass
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

# Column indices (0-based) в «Маппінг» — той самий патерн, що й epicenter_category_service.py.
_COL_PROM_CAT_ID:  Final[int] = 0   # prom_category_id
_COL_ROZ_CAT_ID:   Final[int] = 2   # rozetka_category_id
_COL_ROZ_NAME:     Final[int] = 3   # Назва категорії Розетки
_COL_PARAM_NAME:   Final[int] = 5   # prom_param_name  (аліаси, ";" separated)
_COL_OPTION_NAME:  Final[int] = 6   # prom_option_name (значення, ";" separated)

_ALIAS_SEP: Final[str] = ";"  # роздільник декількох аліасів всередині однієї клітинки

# Лічильник fallback-промахів: (prom_cat_id, rozetka_category_id) → кількість офферів.
_fallback_miss_counts: Counter[tuple[int, int]] = Counter()
_MAX_OFFER_IDS_LOGGED: Final[int] = 10
_fallback_miss_offer_ids: dict[tuple[int, int], list[str]] = {}


# ---------------------------------------------------------------------------
# Public types
# ---------------------------------------------------------------------------

class CategoryEntry(TypedDict):
    category_id: int   # rozetka_category_id
    name: str            # Назва категорії Розетки


@dataclass(frozen=True)
class CategoryMappingRule:
    """Один рядок аркуша «Маппінг» з розпарсеними param-фільтрами."""
    category_id: int
    name: str
    param_names: frozenset[str]    # prom_param_name аліаси (порожньо → no filter)
    option_names: frozenset[str]   # prom_option_name значення (порожньо → no filter)


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _parse_aliases(raw: object) -> frozenset[str]:
    """Розбиває "A; B; C" → frozenset{"A", "B", "C"}. Порожнє значення → порожня множина."""
    if not raw:
        return frozenset()
    return frozenset(part.strip() for part in str(raw).split(_ALIAS_SEP) if part.strip())


# ---------------------------------------------------------------------------
# Cached loader — xlsx читається рівно один раз на процес
# ---------------------------------------------------------------------------

@lru_cache(maxsize=1)
def _load_mapping_rules() -> dict[int, list[CategoryMappingRule]]:
    """
    Повертає повний індекс {prom_category_id: [CategoryMappingRule, ...]}.

    Один prom_category_id може мати кілька правил (param-based routing):
    порядок у списку відповідає порядку рядків у xlsx → визначає пріоритет
    (перше збіжне param-правило виграє, перше plain-правило — fallback).
    Рядки без prom_category_id або rozetka_category_id пропускаються.
    """
    if not _XLSX_PATH.exists():
        raise FileNotFoundError(f"rozetka_mappings.xlsx не знайдено: {_XLSX_PATH}")

    wb = openpyxl.load_workbook(_XLSX_PATH, read_only=True, data_only=True)
    try:
        ws = wb[_SHEET_NAME]
    except KeyError:
        raise KeyError(f"Аркуш «{_SHEET_NAME}» не знайдено у {_XLSX_PATH}")

    result: dict[int, list[CategoryMappingRule]] = {}
    skipped = 0

    for row_idx, row in enumerate(ws.iter_rows(values_only=True), start=1):
        if row_idx == 1:
            continue  # заголовок

        prom_id_raw = row[_COL_PROM_CAT_ID] if len(row) > _COL_PROM_CAT_ID else None
        roz_id_raw  = row[_COL_ROZ_CAT_ID]  if len(row) > _COL_ROZ_CAT_ID  else None

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

        roz_name_raw = row[_COL_ROZ_NAME] if len(row) > _COL_ROZ_NAME else None
        roz_name = str(roz_name_raw).strip() if roz_name_raw else ""

        raw_param  = row[_COL_PARAM_NAME]  if len(row) > _COL_PARAM_NAME  else None
        raw_option = row[_COL_OPTION_NAME] if len(row) > _COL_OPTION_NAME else None

        rule = CategoryMappingRule(
            category_id=roz_id,
            name=roz_name,
            param_names=_parse_aliases(raw_param),
            option_names=_parse_aliases(raw_option),
        )
        result.setdefault(prom_id, []).append(rule)

    wb.close()

    param_rows = sum(1 for rules in result.values() for r in rules if r.param_names)
    plain_rows = sum(1 for rules in result.values() for r in rules if not r.param_names)
    logger.info(
        "📋 Завантажено %d prom-категорій у Маппінгу Розетки "
        "(%d plain, %d param-based, пропущено рядків: %d)",
        len(result), plain_rows, param_rows, skipped,
    )
    return result


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def resolve_category(
    prom_cat_id: int,
    prom_params: dict[str, str],
    offer_id: str | None = None,
) -> CategoryEntry | None:
    """
    Визначає rozetka-категорію для офера з урахуванням його Prom-параметрів
    (param-based routing — наприклад «Тип пристрою» → «IP-камери» / «HDCVI
    відеокамери» замість одного спільного нелистового батька).

    Пріоритет (див. докстрінг модуля для деталей):
    1. param-based правила → перше збіжне
    2. plain-правила (param_names порожні) → перше
    3. conservative fallback на rules[0] + накопичений warning

    offer_id — опціональний ідентифікатор офера для діагностики fallback-промахів
    у flush_fallback_warnings().

    Не кидає виключень — безпечно для поштучної обробки товарів (Reliability:
    не фейлити на одному айтемі). Повертає None лише якщо для prom_cat_id
    немає жодного рядка в Маппінгу.
    """
    rules = _load_mapping_rules().get(prom_cat_id)
    if not rules:
        return None

    plain_fallback: CategoryMappingRule | None = None

    for rule in rules:
        if not rule.param_names:
            if plain_fallback is None:
                plain_fallback = rule
            continue

        for param_name in rule.param_names:
            raw_value = prom_params.get(param_name)
            if raw_value is None:
                continue
            # Prom може віддавати кілька значень через ", " (multiselect) —
            # кожне порівнюємо окремо з prom_option_name.
            for sub_val in (v.strip() for v in raw_value.split(",")):
                if sub_val in rule.option_names:
                    return CategoryEntry(category_id=rule.category_id, name=rule.name)

    if plain_fallback is not None:
        return CategoryEntry(category_id=plain_fallback.category_id, name=plain_fallback.name)

    # Всі правила param-based, але жодне не збіглось → conservative fallback.
    # Не спамимо лог на кожен товар — накопичуємо, flush_fallback_warnings() в кінці ран.
    key = (prom_cat_id, rules[0].category_id)
    _fallback_miss_counts[key] += 1
    if offer_id is not None:
        ids = _fallback_miss_offer_ids.setdefault(key, [])
        if len(ids) < _MAX_OFFER_IDS_LOGGED:
            ids.append(offer_id)
    return CategoryEntry(category_id=rules[0].category_id, name=rules[0].name)


def flush_fallback_warnings() -> None:
    """
    Виводить зведений WARNING по всіх fallback-промахах і скидає лічильники.
    Викликати один раз після завершення обробки всіх товарів фіду.
    """
    if not _fallback_miss_counts:
        return
    for (prom_cat_id, roz_id), count in sorted(_fallback_miss_counts.items()):
        offer_ids = _fallback_miss_offer_ids.get((prom_cat_id, roz_id), [])
        ids_str = ", ".join(offer_ids)
        if count > len(offer_ids):
            ids_str += ", ..."
        logger.warning(
            "prom_cat_id=%d (×%d): param-фільтр не збігся ні з одним правилом "
            "→ fallback rules[0] (rozetka_category_id=%d). Оновіть Маппінг або додайте plain-рядок.\n"
            "  offer ids: %s",
            prom_cat_id, count, roz_id, ids_str,
        )
    _fallback_miss_counts.clear()
    _fallback_miss_offer_ids.clear()


def get_category(prom_category_id: int) -> CategoryEntry | None:
    """
    Backward-compat: повертає CategoryEntry без param-фільтрації.
    Еквівалентно resolve_category(id, {}).
    Для офер-рівневого роутингу використовуй resolve_category().
    """
    return resolve_category(prom_category_id, {})


def get_category_map() -> dict[int, CategoryEntry]:
    """Повний індекс {prom_category_id: CategoryEntry} — перше правило кожної категорії."""
    return {
        prom_id: CategoryEntry(category_id=rules[0].category_id, name=rules[0].name)
        for prom_id, rules in _load_mapping_rules().items()
    }


def build_categories_xml(entries: Iterable[CategoryEntry]) -> str:
    """
    Будує XML-блок <categories> з переданих CategoryEntry.

    Вхід — лише реально використані у фіді категорії (зібрані під час
    replace_category_ids). Сортуємо за category_id для детермінованого виводу.
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
