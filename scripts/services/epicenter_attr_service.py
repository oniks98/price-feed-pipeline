"""
services/epicenter_attr_service.py
------------------------------------
Єдина точка читання атрибутних аркушів «Опції атрибутів» та «Сети атрибутів»
з epicenter_mappings.xlsx.

Аркуші читаються один раз (lru_cache) і роздаються всім споживачам.
Доступний інтерфейс:
    get_option_map()        → OptionMap
    get_set_option_map()    → SetOptionMap
    get_defaults()          → DefaultsMap
    get_numeric_map()       → NumericMap         (global, без set_codes)
    get_set_numeric_map()   → SetNumericMap       (set-scoped, з set_codes)
    get_attr_defaults()     → AttrDefaultsMap
    get_float_defaults()    → FloatDefaultsMap
    get_numeric_defaults()  → NumericDefaultsMap

OptionMap  = dict[str, dict[str, list[AttrOption]]]
    prom_param_name → prom_option_value → list[AttrOption]
    Приклад: {"Кут огляду": {"120": [AttrOption(attr_code="6067", ...)]}}

DefaultsMap = dict[str, dict[str, AttrOption]]
    set_code → attr_code → AttrOption (дефолтна опція для конкретного сету)
    Для multiselect дефолтів option_code — через кому (вихідний формат Epicenter XML).

NumericMap = dict[str, list[AttrMeta]]
    prom_param_name → list[AttrMeta]  (для float / int / text / string атрибутів БЕЗ set_codes)
    Приклад: {"Ширина": [AttrMeta(attr_code="width", attr_name="Ширина", attr_type="float")]}

SetNumericMap = dict[str, NumericMap]
    set_code → prom_param_name → list[AttrMeta]  (для float / int / text / string З set_codes)
    Вирішує проблему «протікання»: атрибут set_codes=376 більше не потрапляє у set 3516.

AttrDefaultsMap = dict[str, AttrOption]
    attr_code → AttrOption (дефолтна опція незалежно від set_code; перший запис у файлі)
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
_ASET_COL_SET_CODE: Final[int] = 0       # set_code
_ASET_COL_ATTR_CODE: Final[int] = 2      # attr_code
_ASET_COL_ATTR_NAME: Final[int] = 3      # attr_name_uk
_ASET_COL_ATTR_TYPE: Final[int] = 4      # attr_type
_ASET_COL_PROM_PARAM: Final[int] = 9     # prom_param_name

# Column indices (0-based) в «Опції атрибутів»
_OPT_COL_ATTR_CODE: Final[int] = 0
_OPT_COL_ATTR_NAME: Final[int] = 1
_OPT_COL_ATTR_TYPE: Final[int] = 2
_OPT_COL_OPTION_CODE: Final[int] = 3
_OPT_COL_OPTION_NAME: Final[int] = 4
_OPT_COL_PROM_VALUE: Final[int] = 5
_OPT_COL_NEEDS_DEFAULT: Final[int] = 6
_OPT_COL_DEFAULT_CODE: Final[int] = 7
_OPT_COL_SET_CODES: Final[int] = 8
_OPT_COL_PROM_PARAMS: Final[int] = 9

# Типи атрибутів без опцій.
# Публічна константа — імпортується в generate_epicenter_feed.py.
NON_OPTION_TYPES: Final[frozenset[str]] = frozenset({"float", "int", "text", "string", "array"})
_NON_OPTION_TYPES = NON_OPTION_TYPES


# ---------------------------------------------------------------------------
# Public types
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class AttrOption:
    """Маппінг одної опції select/multiselect атрибута."""
    attr_code: str
    attr_name: str
    option_code: str
    option_name: str


@dataclass(frozen=True)
class AttrMeta:
    """Метадані числового / текстового атрибута (без опцій)."""
    attr_code: str
    attr_name: str
    attr_type: str   # float | int | text | string


OptionMap        = dict[str, dict[str, list[AttrOption]]]
SetOptionMap     = dict[str, OptionMap]
DefaultsMap      = dict[str, dict[str, AttrOption]]
NumericMap       = dict[str, list[AttrMeta]]
SetNumericMap    = dict[str, NumericMap]
# set_code → prom_param → list[AttrMeta]
# Вирішує «протікання»: attr з set_codes=376 не потрапляє у set 3516.
# Lookup: set_numeric_map.get(cat_code, {}).get(prom_param) or numeric_map.get(prom_param)
AttrDefaultsMap  = dict[str, AttrOption]
FloatDefaultsMap = dict[str, str]
NumericDefaultsMap = dict[str, dict[str, tuple[AttrMeta, str]]]


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _clean(value: object) -> str:
    return str(value).strip() if value is not None else ""


def _parse_set_codes(raw: object) -> list[str]:
    if not raw:
        return []
    return [s.strip() for s in str(raw).split(",") if s.strip()]


def _parse_default_option_codes(raw: object) -> list[str]:
    """Semicolon-separated список default_option_code."""
    if not raw:
        return []
    return [s.strip() for s in str(raw).split(";") if s.strip()]


def _parse_prom_param_aliases(raw: object) -> list[str]:
    """Semicolon-separated список prom_param_name алиасів."""
    if not raw:
        return []
    return [s.strip() for s in str(raw).split(";") if s.strip()]


def _parse_prom_option_aliases(raw: object) -> list[str]:
    """Semicolon-separated список prom_option_name алиасів."""
    if not raw:
        return []
    return [s.strip() for s in str(raw).split(";") if s.strip()]


def _add_numeric_alias(target: NumericMap, alias: str, meta: AttrMeta) -> bool:
    """Додає AttrMeta до numeric alias без дублювання одного attr_code."""
    metas = target.setdefault(alias, [])
    if any(existing.attr_code == meta.attr_code for existing in metas):
        return False
    metas.append(meta)
    return True


def _load_workbook() -> openpyxl.Workbook:
    if not _XLSX_PATH.exists():
        raise FileNotFoundError(f"epicenter_mappings.xlsx не знайдено: {_XLSX_PATH}")
    return openpyxl.load_workbook(_XLSX_PATH, read_only=True, data_only=True)


# ---------------------------------------------------------------------------
# Sub-loader: «Сети атрибутів»
# ---------------------------------------------------------------------------

def _build_attr_indexes(
    wb: openpyxl.Workbook,
) -> tuple[dict[str, list[str]], NumericMap, dict[str, list[str]]]:
    """
    Читає «Сети атрибутів» і повертає три індекси:

    attr_to_prom: {attr_code → list[prom_param_name]}
        PRIMARY для numeric_map (float без set_codes).
        FALLBACK для option_map якщо col 9 «Опції атрибутів» порожній.

    numeric_map: {prom_param_name → list[AttrMeta]}
        Тільки для NON_OPTION_TYPES атрибутів БЕЗ set_codes (глобальні).
        Атрибути з set_codes потраплять у set_numeric_map через «Опції атрибутів».

    attr_set_codes: {attr_code → list[set_code]}
        set_code з «Сети атрибутів» для кожного attr_code.
        Використовується в _load_indexes для наповнення set_numeric_map.
    """
    try:
        ws = wb[_SHEET_ATTRS]
    except KeyError:
        raise KeyError(f"Аркуш «{_SHEET_ATTRS}» не знайдено у {_XLSX_PATH}")

    attr_to_prom:   dict[str, list[str]] = {}
    numeric_map:    NumericMap           = {}
    attr_set_codes: dict[str, list[str]] = {}

    for row_idx, row in enumerate(ws.iter_rows(values_only=True), start=1):
        if row_idx == 1:
            continue

        attr_code = _clean(row[_ASET_COL_ATTR_CODE])
        attr_name = _clean(row[_ASET_COL_ATTR_NAME])
        attr_type = _clean(row[_ASET_COL_ATTR_TYPE]).lower()
        prom_aliases = _parse_prom_param_aliases(row[_ASET_COL_PROM_PARAM])

        if not attr_code or not prom_aliases:
            continue

        attr_to_prom[attr_code] = prom_aliases

        sc_list = _parse_set_codes(row[_ASET_COL_SET_CODE])
        if sc_list:
            attr_set_codes[attr_code] = sc_list

        if attr_type in _NON_OPTION_TYPES:
            meta = AttrMeta(attr_code=attr_code, attr_name=attr_name, attr_type=attr_type)
            # Глобальний numeric_map — тільки для атрибутів БЕЗ set_codes.
            # Атрибути з set_codes → set_numeric_map (не смітять у чужі категорії).
            if not sc_list:
                for alias in prom_aliases:
                    _add_numeric_alias(numeric_map, alias, meta)

    logger.debug(
        "Сети атрибутів: attr_to_prom=%d | numeric_map=%d (global) | set_numeric candidates=%d",
        len(attr_to_prom), len(numeric_map), len(attr_set_codes),
    )
    return attr_to_prom, numeric_map, attr_set_codes


# ---------------------------------------------------------------------------
# Internal helpers (option merging)
# ---------------------------------------------------------------------------

def _merge_options(opts: list[AttrOption], attr_code: str, row_idx: int) -> AttrOption:
    """Мержить список AttrOption в одну зведену (для multiselect-дефолтів)."""
    if len(opts) == 1:
        return opts[0]
    result = AttrOption(
        attr_code=opts[0].attr_code,
        attr_name=opts[0].attr_name,
        option_code=",".join(o.option_code for o in opts),
        option_name=", ".join(o.option_name for o in opts),
    )
    logger.debug(
        "Рядок %d: multiselect default | attr_code=%r | merged %d опцій → valuecode=%r",
        row_idx, attr_code, len(opts), result.option_code,
    )
    return result


# ---------------------------------------------------------------------------
# Main cached loader
# ---------------------------------------------------------------------------

@lru_cache(maxsize=1)
def _load_indexes() -> tuple[
    OptionMap, SetOptionMap, DefaultsMap,
    NumericMap, SetNumericMap,
    AttrDefaultsMap, FloatDefaultsMap, NumericDefaultsMap,
]:
    """Єдине читання обох аркушів. Результат кешується через lru_cache."""
    wb = _load_workbook()

    # --- крок 1: «Сети атрибутів» ---
    attr_to_prom, numeric_map, attr_set_codes = _build_attr_indexes(wb)

    # --- крок 2: «Опції атрибутів» ---
    try:
        ws_opts = wb[_SHEET_OPTIONS]
    except KeyError:
        raise KeyError(f"Аркуш «{_SHEET_OPTIONS}» не знайдено у {_XLSX_PATH}")

    key_index:        dict[tuple[str, str], AttrOption]       = {}
    set_key_index:    dict[tuple[str, str, str], AttrOption]  = {}
    option_map:       OptionMap         = {}
    set_option_map:   SetOptionMap      = {}
    set_numeric_map:  SetNumericMap     = {}
    defaults:         DefaultsMap       = {}
    attr_defaults:    AttrDefaultsMap   = {}
    float_defaults:   FloatDefaultsMap  = {}
    numeric_defaults: NumericDefaultsMap = {}
    pending_defaults: list[tuple[int, str, str, object]] = []
    opt_mapped = 0
    def_mapped = 0

    for row_idx, row in enumerate(ws_opts.iter_rows(values_only=True), start=1):
        if row_idx == 1:
            continue

        attr_code     = _clean(row[_OPT_COL_ATTR_CODE])
        attr_type_raw = _clean(row[_OPT_COL_ATTR_TYPE]).lower()
        option_code   = _clean(row[_OPT_COL_OPTION_CODE])
        prom_value    = _clean(row[_OPT_COL_PROM_VALUE])

        # --- NON_OPTION_TYPES: рядки без option_code ---
        # Три завдання:
        #   1) Реєстрація prom_param_name у set_numeric_map (є set_codes)
        #      або numeric_map (без set_codes).
        #      КЛЮЧОВИЙ FIX: атрибути з set_codes НЕ потрапляють у глобальний numeric_map
        #      → не «протікають» у чужі категорії.
        #      Приклад: attr_code=12137 «Фокусна відстань, max» (set=376)
        #      більше не з'являється у set=3516.
        #   2) numeric_defaults[sc][attr_code] = (AttrMeta, default_value)
        #   3) float_defaults[attr_code] = default_value  (без set_codes)
        if attr_code and not option_code and attr_type_raw in _NON_OPTION_TYPES:
            default_value = _clean(row[_OPT_COL_OPTION_NAME])
            set_codes     = _parse_set_codes(row[_OPT_COL_SET_CODES])
            attr_name_val = _clean(row[_OPT_COL_ATTR_NAME])

            prom_aliases = _parse_prom_param_aliases(row[_OPT_COL_PROM_PARAMS])
            if not prom_aliases:
                prom_aliases = attr_to_prom.get(attr_code, [])

            if prom_aliases:
                meta_for_map = AttrMeta(
                    attr_code=attr_code,
                    attr_name=attr_name_val,
                    attr_type=attr_type_raw,
                )
                if set_codes:
                    # Set-scoped: тільки для своїх категорій.
                    for sc in set_codes:
                        sc_nmap = set_numeric_map.setdefault(sc, {})
                        for alias in prom_aliases:
                            if _add_numeric_alias(sc_nmap, alias, meta_for_map):
                                logger.debug(
                                    "Рядок %d: set_numeric_map[%r] ← alias=%r | attr_code=%r",
                                    row_idx, sc, alias, attr_code,
                                )
                else:
                    # Global: без прив'язки до категорії.
                    for alias in prom_aliases:
                        if _add_numeric_alias(numeric_map, alias, meta_for_map):
                            logger.debug(
                                "Рядок %d: numeric_map ← alias=%r | attr_code=%r (global)",
                                row_idx, alias, attr_code,
                            )

            if set_codes and default_value:
                meta_def = AttrMeta(
                    attr_code=attr_code,
                    attr_name=attr_name_val,
                    attr_type=attr_type_raw,
                )
                for sc in set_codes:
                    numeric_defaults.setdefault(sc, {}).setdefault(attr_code, (meta_def, default_value))
                logger.debug(
                    "Рядок %d: numeric default (set-scoped) | attr_code=%r value=%r set_codes=%r",
                    row_idx, attr_code, default_value, set_codes,
                )
            elif default_value and attr_code not in float_defaults:
                float_defaults[attr_code] = default_value
                logger.debug(
                    "Рядок %d: float default (global) | attr_code=%r value=%r",
                    row_idx, attr_code, default_value,
                )
            continue

        if not attr_code:
            continue

        # --- key_index / set_key_index ---
        if option_code:
            option = AttrOption(
                attr_code=attr_code,
                attr_name=_clean(row[_OPT_COL_ATTR_NAME]),
                option_code=option_code,
                option_name=_clean(row[_OPT_COL_OPTION_NAME]),
            )
            if (attr_code, option_code) not in key_index:
                key_index[(attr_code, option_code)] = option
            for sc in _parse_set_codes(row[_OPT_COL_SET_CODES]):
                set_key_index[(sc, attr_code, option_code)] = option

        # --- option_map / set_option_map ---
        if prom_value and option_code:
            prom_aliases = _parse_prom_param_aliases(row[_OPT_COL_PROM_PARAMS])
            if not prom_aliases:
                prom_aliases = attr_to_prom.get(attr_code)
                if prom_aliases:
                    logger.debug(
                        "Рядок %d: attr_code=%r — prom_param_name з «Сети атрибутів» (fallback)",
                        row_idx, attr_code,
                    )
            prom_option_aliases = _parse_prom_option_aliases(row[_OPT_COL_PROM_VALUE])
            if not prom_aliases:
                logger.debug(
                    "Рядок %d: attr_code=%r — prom_param_name відсутній → пропущено",
                    row_idx, attr_code,
                )
            elif not prom_option_aliases:
                logger.debug(
                    "Рядок %d: attr_code=%r prom_option_name порожній → пропущено",
                    row_idx, attr_code,
                )
            else:
                global_option = key_index[(attr_code, option_code)]
                for param_alias in prom_aliases:
                    for option_alias in prom_option_aliases:
                        option_map.setdefault(param_alias, {}).setdefault(option_alias, []).append(global_option)

                set_codes_for_opt = _parse_set_codes(row[_OPT_COL_SET_CODES])
                for sc in set_codes_for_opt:
                    sc_option = set_key_index.get((sc, attr_code, option_code))
                    if sc_option is None:
                        continue
                    sc_map = set_option_map.setdefault(sc, {})
                    for param_alias in prom_aliases:
                        for option_alias in prom_option_aliases:
                            sc_map.setdefault(param_alias, {}).setdefault(option_alias, []).append(sc_option)

                opt_mapped += 1

        # --- pending_defaults ---
        default_code = _clean(row[_OPT_COL_DEFAULT_CODE])
        if default_code:
            pending_defaults.append((row_idx, attr_code, default_code, row[_OPT_COL_SET_CODES]))

    # --- крок 4: резолв дефолтів ---
    set_attr_codes:    dict[tuple[str, str], list[str]] = {}
    global_attr_codes: dict[str, list[str]]             = {}
    set_attr_row:    dict[tuple[str, str], int] = {}
    global_attr_row: dict[str, int]             = {}

    for row_idx, attr_code, default_code, set_codes_raw in pending_defaults:
        opt_codes = _parse_default_option_codes(default_code)
        set_codes = _parse_set_codes(set_codes_raw)

        acc_global = global_attr_codes.setdefault(attr_code, [])
        if attr_code not in global_attr_row:
            global_attr_row[attr_code] = row_idx
        for c in opt_codes:
            if c not in acc_global:
                acc_global.append(c)

        if not set_codes:
            logger.debug(
                "Рядок %d: attr_code=%r — set_codes порожній, тільки attr_defaults",
                row_idx, attr_code,
            )
            continue

        for set_code in set_codes:
            key = (set_code, attr_code)
            acc = set_attr_codes.setdefault(key, [])
            if key not in set_attr_row:
                set_attr_row[key] = row_idx
            for c in opt_codes:
                if c not in acc:
                    acc.append(c)

    # Резолв attr_defaults
    for attr_code, acc_codes in global_attr_codes.items():
        row_idx = global_attr_row[attr_code]
        found_global = [
            o for c in acc_codes
            if (o := key_index.get((attr_code, c))) is not None
        ]
        missing_global = [c for c in acc_codes if key_index.get((attr_code, c)) is None]
        if missing_global:
            logger.warning(
                "Рядок %d: default_option_code(s) %r не знайдено для attr_code=%r",
                row_idx, missing_global, attr_code,
            )
        if found_global:
            attr_defaults[attr_code] = _merge_options(found_global, attr_code, row_idx)

    # Резолв defaults (set-scoped)
    for (set_code, attr_code), acc_codes in set_attr_codes.items():
        row_idx = set_attr_row[(set_code, attr_code)]
        found_opts: list[AttrOption] = []
        missing: list[str] = []
        for c in acc_codes:
            set_opt = set_key_index.get((set_code, attr_code, c))
            if set_opt is not None:
                found_opts.append(set_opt)
            else:
                global_opt = key_index.get((attr_code, c))
                if global_opt is not None:
                    logger.warning(
                        "Рядок %d: default_option_code %r є в key_index (option_name=%r), "
                        "але відсутнє в set_key_index для set_code=%r / attr_code=%r → пропущено",
                        row_idx, c, global_opt.option_name, set_code, attr_code,
                    )
                else:
                    missing.append(c)

        if missing:
            logger.warning(
                "Рядок %d: default_option_code(s) %r не знайдено для attr_code=%r set_code=%r",
                row_idx, missing, attr_code, set_code,
            )
        if not found_opts:
            continue

        defaults.setdefault(set_code, {})[attr_code] = _merge_options(
            found_opts, attr_code, row_idx
        )
        def_mapped += 1

    wb.close()

    logger.info(
        "📐 option_map: %d prom_params / %d (param,value) ключів / %d opt_mapped "
        "| set_option_map: %d set_codes "
        "| defaults: %d set_codes | attr_defaults: %d глобальних "
        "| numeric_map (global): %d | set_numeric_map: %d set_codes "
        "| numeric_defaults: %d set_codes",
        len(option_map),
        sum(len(vals) for vals in option_map.values()),
        opt_mapped,
        len(set_option_map),
        len(defaults),
        len(attr_defaults),
        len(numeric_map),
        len(set_numeric_map),
        len(numeric_defaults),
    )
    return (
        option_map, set_option_map, defaults,
        numeric_map, set_numeric_map,
        attr_defaults, float_defaults, numeric_defaults,
    )


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def get_option_map() -> OptionMap:
    """prom_param_name → prom_value → list[AttrOption] (глобальний)."""
    option_map, _, _, _, _, _, _, _ = _load_indexes()
    return option_map


def get_set_option_map() -> SetOptionMap:
    """set_code → prom_param_name → prom_value → list[AttrOption] (set-scoped)."""
    _, set_option_map, _, _, _, _, _, _ = _load_indexes()
    return set_option_map


def get_defaults() -> DefaultsMap:
    """set_code → attr_code → AttrOption (дефолтна опція для категорії)."""
    _, _, defaults, _, _, _, _, _ = _load_indexes()
    return defaults


def get_numeric_map() -> NumericMap:
    """
    prom_param_name → list[AttrMeta] (global, для атрибутів БЕЗ set_codes).

    Lookup у генераторі (крок 5c):
        metas = get_set_numeric_map().get(cat_code, {}).get(prom_param)
                or get_numeric_map().get(prom_param)
                or []
    """
    _, _, _, numeric_map, _, _, _, _ = _load_indexes()
    return numeric_map


def get_set_numeric_map() -> SetNumericMap:
    """
    set_code → prom_param_name → list[AttrMeta] (set-scoped, для атрибутів З set_codes).

    Пріоритетний lookup перед get_numeric_map().
    Запобігає «протіканню»: attr_code=12137 «Фокусна відстань, max» (set=376)
    більше не потрапляє у set 3516.
    """
    _, _, _, _, set_numeric_map, _, _, _ = _load_indexes()
    return set_numeric_map


def get_attr_defaults() -> AttrDefaultsMap:
    """attr_code → AttrOption (глобальний дефолт, незалежно від set_code)."""
    _, _, _, _, _, attr_defaults, _, _ = _load_indexes()
    return attr_defaults


def get_float_defaults() -> FloatDefaultsMap:
    """attr_code → default value string (для float/int/text/string без set_codes)."""
    _, _, _, _, _, _, float_defaults, _ = _load_indexes()
    return float_defaults


def get_numeric_defaults() -> NumericDefaultsMap:
    """
    set_code → attr_code → (AttrMeta, default_value).
    Категорійні дефолти для NON_OPTION_TYPES атрибутів з set_codes.
    Застосовується у кроці 6c генератора.
    """
    _, _, _, _, _, _, _, numeric_defaults = _load_indexes()
    return numeric_defaults
