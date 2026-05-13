"""
services/epicenter_attr_service.py
------------------------------------
Єдина точка читання атрибутних аркушів «Опції атрибутів» та «Сети атрибутів»
з epicenter_mappings.xlsx.

Аркуші читаються один раз (lru_cache) і роздаються всім споживачам.
Доступний інтерфейс:
    get_option_map()  → OptionMap
    get_defaults()    → DefaultsMap
    get_numeric_map() → NumericMap

OptionMap  = dict[str, dict[str, AttrOption]]
    prom_param_name → prom_option_value → AttrOption
    Приклад: {"Кут огляду": {"120": AttrOption(attr_code="6067", ...)}}

DefaultsMap = dict[str, dict[str, AttrOption]]
    set_code → attr_code → AttrOption (дефолтна опція)
    Приклад: {"5926": {"measure": AttrOption(..., option_code="measure_pcs", option_name="шт.")}}

NumericMap = dict[str, AttrMeta]
    prom_param_name → AttrMeta  (для float / int / text / string атрибутів)
    Приклад: {"Ширина": AttrMeta(attr_code="width", attr_name="Ширина", attr_type="float")}
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path
from typing import Final

import openpyxl

logger = logging.getLogger(__name__)

# services/ → scripts/ → project root → data/markets/
_XLSX_PATH: Final[Path] = (
    Path(__file__).parents[2] / "data" / "markets" / "epicenter_mappings.xlsx"
)

_SHEET_ATTRS: Final[str] = "Сети атрибутів"
_SHEET_OPTIONS: Final[str] = "Опції атрибутів"

# Column indices (0-based) в «Сети атрибутів»
_ASET_COL_ATTR_CODE: Final[int] = 2      # attr_code
_ASET_COL_ATTR_NAME: Final[int] = 3      # attr_name_uk
_ASET_COL_ATTR_TYPE: Final[int] = 4      # attr_type (select | multiselect | float | int | text | string)
_ASET_COL_PROM_PARAM: Final[int] = 9     # prom_param_name

# Column indices (0-based) в «Опції атрибутів»
_OPT_COL_ATTR_CODE: Final[int] = 0       # attr_code
_OPT_COL_ATTR_NAME: Final[int] = 1       # attr_name_uk
_OPT_COL_OPTION_CODE: Final[int] = 3     # option_code
_OPT_COL_OPTION_NAME: Final[int] = 4     # option_name_uk
_OPT_COL_PROM_VALUE: Final[int] = 5      # prom_option_name  (значення з Прому)
_OPT_COL_NEEDS_DEFAULT: Final[int] = 6   # needs_default (bool)
_OPT_COL_DEFAULT_CODE: Final[int] = 7    # default_option_code
_OPT_COL_SET_CODES:   Final[int] = 8     # set_codes   (comma-separated epicenter category ids)
_OPT_COL_PROM_PARAMS: Final[int] = 9     # prom_params (comma-separated prom param names, не використовується тут)

# Типи атрибутів без опцій (значення товару підставляється напряму як CDATA)
_NON_OPTION_TYPES: Final[frozenset[str]] = frozenset({"float", "int", "text", "string"})


# ---------------------------------------------------------------------------
# Public types
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class AttrOption:
    """Маппінг одної опції select/multiselect атрибута."""
    attr_code: str    # epicenter attr code (напр. "6067" або "measure")
    attr_name: str    # epicenter attr name (напр. "Кут огляду")
    option_code: str  # epicenter option code (напр. "measure_pcs")
    option_name: str  # epicenter option name (напр. "шт.")


@dataclass(frozen=True)
class AttrMeta:
    """Метадані числового / текстового атрибута (без опцій)."""
    attr_code: str   # epicenter attr code (напр. "width")
    attr_name: str   # epicenter attr name (напр. "Ширина")
    attr_type: str   # float | int | text | string


OptionMap   = dict[str, dict[str, AttrOption]]  # prom_param → prom_value → AttrOption
DefaultsMap = dict[str, dict[str, AttrOption]]  # set_code   → attr_code  → AttrOption
NumericMap  = dict[str, AttrMeta]               # prom_param → AttrMeta


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _clean(value: object) -> str:
    """Конвертує довільне значення клітинки у clean рядок."""
    return str(value).strip() if value is not None else ""


def _parse_set_codes(raw: object) -> list[str]:
    """
    Парсить prom_params (рядок з comma-separated set_codes) у список рядків.
    Повертає порожній список на будь-який невалідний вхід.
    """
    if not raw:
        return []
    return [s.strip() for s in str(raw).split(",") if s.strip()]


def _load_workbook() -> openpyxl.Workbook:
    if not _XLSX_PATH.exists():
        raise FileNotFoundError(
            f"epicenter_mappings.xlsx не знайдено: {_XLSX_PATH}"
        )
    return openpyxl.load_workbook(_XLSX_PATH, read_only=True, data_only=True)


# ---------------------------------------------------------------------------
# Sub-loader: «Сети атрибутів»
# ---------------------------------------------------------------------------

def _build_attr_indexes(
    wb: openpyxl.Workbook,
) -> tuple[dict[str, str], NumericMap]:
    """
    Читає «Сети атрибутів» і повертає два індекси:

    attr_to_prom: {attr_code → prom_param_name}
        Використовується для побудови option_map (select/multiselect).
        Один attr_code у кількох сетах → prom_param_name однаковий,
        останній запис без втрат.

    numeric_map: {prom_param_name → AttrMeta}
        Тільки для float / int / text / string атрибутів.
        Використовується для рендерингу CDATA-параметрів у XML-фіді.
    """
    try:
        ws = wb[_SHEET_ATTRS]
    except KeyError:
        raise KeyError(f"Аркуш «{_SHEET_ATTRS}» не знайдено у {_XLSX_PATH}")

    attr_to_prom: dict[str, str] = {}
    numeric_map: NumericMap = {}

    for row_idx, row in enumerate(ws.iter_rows(values_only=True), start=1):
        if row_idx == 1:
            continue  # заголовок

        attr_code  = _clean(row[_ASET_COL_ATTR_CODE])
        attr_name  = _clean(row[_ASET_COL_ATTR_NAME])
        attr_type  = _clean(row[_ASET_COL_ATTR_TYPE]).lower()
        prom_param = _clean(row[_ASET_COL_PROM_PARAM])

        if not attr_code or not prom_param:
            continue

        attr_to_prom[attr_code] = prom_param

        if attr_type in _NON_OPTION_TYPES:
            numeric_map[prom_param] = AttrMeta(
                attr_code=attr_code,
                attr_name=attr_name,
                attr_type=attr_type,
            )

    logger.debug(
        "Сети атрибутів: attr→prom_param %d записів | numeric_map %d записів",
        len(attr_to_prom), len(numeric_map),
    )
    return attr_to_prom, numeric_map


# ---------------------------------------------------------------------------
# Main cached loader
# ---------------------------------------------------------------------------

@lru_cache(maxsize=1)
def _load_indexes() -> tuple[OptionMap, DefaultsMap, NumericMap]:
    """
    Єдине читання обох аркушів.

    Повертає (option_map, defaults, numeric_map).
    """
    wb = _load_workbook()

    # --- крок 1: «Сети атрибутів» ---
    attr_to_prom, numeric_map = _build_attr_indexes(wb)

    # --- крок 2: «Опції атрибутів» ---
    try:
        ws_opts = wb[_SHEET_OPTIONS]
    except KeyError:
        raise KeyError(f"Аркуш «{_SHEET_OPTIONS}» не знайдено у {_XLSX_PATH}")

    # --- крок 3: один прохід — будуємо key_index, option_map і збираємо pending defaults ---
    # key_index потрібен для резолву default_option_code; будується паралельно з option_map.
    # Рядки з needs_default відкладаємо до повної побудови key_index (default_code
    # може посилатись на опцію, що йде пізніше по файлу).
    key_index:       dict[tuple[str, str], AttrOption] = {}
    option_map:      OptionMap   = {}
    defaults:        DefaultsMap = {}
    pending_defaults: list[tuple[int, str, str, object]] = []  # (row_idx, attr_code, default_code, set_codes_raw)
    opt_mapped = 0
    def_mapped = 0

    for row_idx, row in enumerate(ws_opts.iter_rows(values_only=True), start=1):
        if row_idx == 1:
            continue

        attr_code   = _clean(row[_OPT_COL_ATTR_CODE])
        option_code = _clean(row[_OPT_COL_OPTION_CODE])
        prom_value  = _clean(row[_OPT_COL_PROM_VALUE])

        if not attr_code:
            continue

        # key_index: будь-який рядок з option_code (для резолву дефолтів)
        if option_code:
            key_index[(attr_code, option_code)] = AttrOption(
                attr_code=attr_code,
                attr_name=_clean(row[_OPT_COL_ATTR_NAME]),
                option_code=option_code,
                option_name=_clean(row[_OPT_COL_OPTION_NAME]),
            )

        # option_map: рядки де prom_option_name заповнений
        if prom_value and option_code:
            prom_param = attr_to_prom.get(attr_code)
            if not prom_param:
                logger.debug(
                    "Рядок %d: attr_code=%r не має prom_param_name → пропущено",
                    row_idx, attr_code,
                )
            else:
                option_map.setdefault(prom_param, {})[prom_value] = key_index[(attr_code, option_code)]
                opt_mapped += 1

        # defaults: відкладаємо до завершення побудови key_index.
        # Умова — тільки наявність default_option_code, НЕ needs_default.
        # Причина: needs_default=TRUE означає «маппінгу немає взагалі»,
        # але дефолт потрібен також коли маппінг є (needs_default=FALSE),
        # а конкретний товар не має цього prom-параметра.
        # Генератор (крок 6) застосовує default лише якщо attr_code
        # не потрапив у mapped_attr_codes — це покриває обидва кейси.
        default_code = _clean(row[_OPT_COL_DEFAULT_CODE])
        if default_code:
            pending_defaults.append((row_idx, attr_code, default_code, row[_OPT_COL_SET_CODES]))

    # --- крок 4: резолв дефолтів (key_index тепер повний) ---
    for row_idx, attr_code, default_code, set_codes_raw in pending_defaults:
        default_option = key_index.get((attr_code, default_code))
        if not default_option:
            logger.warning(
                "Рядок %d: default_option_code=%r не знайдено для attr_code=%r → пропущено",
                row_idx, default_code, attr_code,
            )
            continue
        set_codes = _parse_set_codes(set_codes_raw)
        if not set_codes:
            logger.warning(
                "Рядок %d: needs_default=True, але set_codes порожній для attr_code=%r",
                row_idx, attr_code,
            )
        for set_code in set_codes:
            defaults.setdefault(set_code, {})[attr_code] = default_option
            def_mapped += 1

    wb.close()

    logger.info(
        "📐 option_map: %d prom_params / %d опцій | defaults: %d set_codes | numeric_map: %d",
        len(option_map), opt_mapped, len(defaults), len(numeric_map),
    )
    return option_map, defaults, numeric_map


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def get_option_map() -> OptionMap:
    """
    Маппінг select/multiselect опцій: prom_param_name → prom_value → AttrOption.

    Використання:
        option = get_option_map().get(prom_param, {}).get(prom_value)
    """
    option_map, _, _ = _load_indexes()
    return option_map


def get_defaults() -> DefaultsMap:
    """
    Дефолтні опції: set_code → attr_code → AttrOption.

    Використання:
        for attr_code, default in get_defaults().get(set_code, {}).items():
            if attr_code not in already_mapped_codes:
                params.append(default)
    """
    _, defaults, _ = _load_indexes()
    return defaults


def get_numeric_map() -> NumericMap:
    """
    Маппінг float/int/text/string атрибутів: prom_param_name → AttrMeta.

    Значення з Прому підставляється напряму у CDATA без маппінгу опцій.

    Використання:
        meta = get_numeric_map().get(prom_param_name)
        if meta:
            xml_param = f'<param paramcode="{meta.attr_code}" ...><![CDATA[{value}]]></param>'
    """
    _, _, numeric_map = _load_indexes()
    return numeric_map
