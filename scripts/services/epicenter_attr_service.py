"""
services/epicenter_attr_service.py
------------------------------------
Єдина точка читання атрибутних аркушів «Опції атрибутів» та «Сети атрибутів»
з epicenter_mappings.xlsx.

Аркуші читаються один раз (lru_cache) і роздаються всім споживачам.
Доступний інтерфейс:
    get_option_map()    → OptionMap
    get_defaults()      → DefaultsMap
    get_numeric_map()   → NumericMap
    get_attr_defaults() → AttrDefaultsMap

OptionMap  = dict[str, dict[str, AttrOption]]
    prom_param_name → prom_option_value → AttrOption
    Приклад: {"Кут огляду": {"120": AttrOption(attr_code="6067", ...)}}

DefaultsMap = dict[str, dict[str, AttrOption]]
    set_code → attr_code → AttrOption (дефолтна опція для конкретного сету)
    Приклад: {"5926": {"measure": AttrOption(..., option_code="measure_pcs", option_name="шт.")}}
    Для multiselect дефолтів option_code містить кілька кодів через кому (без пробілів):
    Приклад: {"5926": {"3176":  AttrOption(option_code="bsz6btxa,wle9vq5zsirz1dni", option_name="тварини, коти")}}

NumericMap = dict[str, AttrMeta]
    prom_param_name → AttrMeta  (для float / int / text / string атрибутів)
    Приклад: {"Ширина": AttrMeta(attr_code="width", attr_name="Ширина", attr_type="float")}

AttrDefaultsMap = dict[str, AttrOption]
    attr_code → AttrOption (дефолтна опція незалежно від set_code; перший запис у файлі)
    Приклад: {"measure": AttrOption(attr_code="measure", ..., option_code="measure_pcs")}
    Призначення: глобальні дефолти для атрибутів, де set_codes порожній,
    або як fallback коли атрибут відсутній у DefaultsMap для конкретного сету.
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
_OPT_COL_ATTR_TYPE: Final[int] = 2       # attr_type (float | int | text | string | select | ...)
_OPT_COL_OPTION_CODE: Final[int] = 3     # option_code
_OPT_COL_OPTION_NAME: Final[int] = 4     # option_name_uk
_OPT_COL_PROM_VALUE: Final[int] = 5      # prom_option_name  (значення з Прому)
_OPT_COL_NEEDS_DEFAULT: Final[int] = 6   # needs_default (bool)
_OPT_COL_DEFAULT_CODE: Final[int] = 7    # default_option_code
_OPT_COL_SET_CODES:   Final[int] = 8     # set_codes   (comma-separated epicenter category ids)
_OPT_COL_PROM_PARAMS: Final[int] = 9     # prom_params (comma-separated prom param names) — primary джерело для option_map

# Типи атрибутів без опцій (значення товару підставляється напряму як CDATA).
# Публічна константа — імпортується в generate_epicenter_feed.py для
# узгодженої перевірки attr_type без дублювання.
NON_OPTION_TYPES: Final[frozenset[str]] = frozenset({"float", "int", "text", "string", "array"})
_NON_OPTION_TYPES = NON_OPTION_TYPES  # зворотна сумісність (внутрішнє використання)


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


OptionMap       = dict[str, dict[str, AttrOption]]  # prom_param → prom_value → AttrOption
DefaultsMap     = dict[str, dict[str, AttrOption]]  # set_code   → attr_code  → AttrOption
NumericMap      = dict[str, AttrMeta]               # prom_param → AttrMeta
AttrDefaultsMap = dict[str, AttrOption]             # attr_code  → AttrOption (global default)
FloatDefaultsMap = dict[str, str]                   # attr_code  → default value string (for float/int/text/string)


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _clean(value: object) -> str:
    """Конвертує довільне значення клітинки у clean рядок."""
    return str(value).strip() if value is not None else ""


def _parse_set_codes(raw: object) -> list[str]:
    """
    Парсить set_codes (рядок з comma-separated категорій) у список рядків.
    Повертає порожній список на будь-який невалідний вхід.
    """
    if not raw:
        return []
    return [s.strip() for s in str(raw).split(",") if s.strip()]


def _parse_default_option_codes(raw: object) -> list[str]:
    """
    Парсить default_option_code як comma-separated список кодів опцій.

    Розділювач — кома БЕЗ пробілів: option_code є технічними slug-рядками
    (напр. "bsz6btxa"), тому кома без пробілу є однозначним роздільником.
    Порівняти з:
        _parse_prom_param_aliases  → розділювач «;»  (назви можуть містити кому)
        _parse_set_codes           → розділювач «,»  (числові ID, кома безпечна)

    Одне значення — повертає список з одного елемента (поведінка без змін).
    Кілька значень → merged AttrOption з combined option_code і option_name.

    Приклад: "bsz6btxa,wle9vq5zsirz1dni"
             → ["bsz6btxa", "wle9vq5zsirz1dni"]
    Приклад: "measure_pcs"
             → ["measure_pcs"]
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

def _parse_prom_param_aliases(raw: object) -> list[str]:
    """
    Парсить prom_param_name як semicolon-separated список алиасів.

    Розділювач «;» (крапка з комою) — а НЕ кома — щоб уникнути конфліктів
    з назвами характеристик що самі містять кому (напр. «Вихідна напруга, В»).

    Перший елемент — найчастіший варіант (основний).
    Повертає порожній список якщо значення відсутнє.

    Приклад: "Розміри; Розмір; Размер; Розмір упаковки"
             → ["Розміри", "Розмір", "Размер", "Розмір упаковки"]
    Приклад: "Вихідна напруга, В"
             → ["Вихідна напруга, В"]   (кома всередині — не розділювач)
    """
    if not raw:
        return []
    return [s.strip() for s in str(raw).split(";") if s.strip()]


def _parse_prom_option_aliases(raw: object) -> list[str]:
    """
    Парсить prom_option_name як semicolon-separated список алиасів значення опції.

    Розділювач «;» (крапка з комою) — а НЕ кома — щоб уникнути конфліктів
    з опціями що самі містять кому (напр. «4-тактний, з повітряним охолодженням»).

    Аналог _parse_prom_param_aliases — для колонки prom_option_name
    аркуша «Опції атрибутів».
    Всі алиаси реєструються як окремі ключі:
        option_map[param_alias][option_alias] → AttrOption

    Приклад: "Білий; Белый; White"
             → ["Білий", "Белый", "White"]
    Приклад: "4-тактний, з повітряним охолодженням"
             → ["4-тактний, з повітряним охолодженням"]   (кома всередині — не розділювач)
    Приклад: "120"
             → ["120"]   (одне значення — поведінка без змін)
    """
    if not raw:
        return []
    return [s.strip() for s in str(raw).split(";") if s.strip()]


def _build_attr_indexes(
    wb: openpyxl.Workbook,
) -> tuple[dict[str, list[str]], NumericMap]:
    """
    Читає «Сети атрибутів» і повертає два індекси:

    attr_to_prom: {attr_code → list[prom_param_name]}
        Список алиасів prom_param_name (comma-separated у xlsx).
        Перший елемент — найчастіший варіант.
        ПРИЗНАЧЕННЯ:
          - primary джерело для numeric_map (float/int/text/string);
          - fallback для option_map якщо col 9 «Опції атрибутів» порожній.
        Primary джерело для option_map — col 9 «Опції атрибутів».

    numeric_map: {prom_param_name → AttrMeta}
        Тільки для float / int / text / string атрибутів.
        Всі алиаси реєструються як окремі ключі → один AttrMeta.
        Використовується для рендерингу CDATA-параметрів у XML-фіді.
    """
    try:
        ws = wb[_SHEET_ATTRS]
    except KeyError:
        raise KeyError(f"Аркуш «{_SHEET_ATTRS}» не знайдено у {_XLSX_PATH}")

    attr_to_prom: dict[str, list[str]] = {}
    numeric_map: NumericMap = {}

    for row_idx, row in enumerate(ws.iter_rows(values_only=True), start=1):
        if row_idx == 1:
            continue  # заголовок

        attr_code  = _clean(row[_ASET_COL_ATTR_CODE])
        attr_name  = _clean(row[_ASET_COL_ATTR_NAME])
        attr_type  = _clean(row[_ASET_COL_ATTR_TYPE]).lower()
        prom_param_aliases = _parse_prom_param_aliases(row[_ASET_COL_PROM_PARAM])

        if not attr_code or not prom_param_aliases:
            continue

        attr_to_prom[attr_code] = prom_param_aliases

        if attr_type in _NON_OPTION_TYPES:
            meta = AttrMeta(
                attr_code=attr_code,
                attr_name=attr_name,
                attr_type=attr_type,
            )
            for alias in prom_param_aliases:
                numeric_map[alias] = meta

    logger.debug(
        "Сети атрибутів: attr→prom_param %d записів | numeric_map %d ключів (%d атрибутів з аліасами)",
        len(attr_to_prom),
        len(numeric_map),
        sum(1 for aliases in attr_to_prom.values() if len(aliases) > 1),
    )
    return attr_to_prom, numeric_map


# ---------------------------------------------------------------------------
# Main cached loader
# ---------------------------------------------------------------------------

@lru_cache(maxsize=1)
def _load_indexes() -> tuple[OptionMap, DefaultsMap, NumericMap, AttrDefaultsMap]:
    """
    Єдине читання обох аркушів.

    Повертає (option_map, defaults, numeric_map, attr_defaults).

    attr_defaults — глобальні дефолти: attr_code → AttrOption.
        Будується з усіх рядків де default_option_code заповнений;
        перший зустрічний запис для кожного attr_code виграє.
        Призначення: атрибути без set_codes (наприклад, «measure» що
        діє на всі категорії) або fallback коли конкретний set_code
        відсутній у DefaultsMap.
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
    # Рядки з default_option_code відкладаємо до повної побудови key_index (default_code
    # може посилатись на опцію, що йде пізніше по файлу).
    key_index:        dict[tuple[str, str], AttrOption] = {}
    option_map:       OptionMap        = {}
    defaults:         DefaultsMap      = {}
    attr_defaults:    AttrDefaultsMap  = {}
    float_defaults:   FloatDefaultsMap = {}
    # (row_idx, attr_code, default_code, set_codes_raw)
    pending_defaults: list[tuple[int, str, str, object]] = []
    opt_mapped = 0
    def_mapped = 0

    for row_idx, row in enumerate(ws_opts.iter_rows(values_only=True), start=1):
        if row_idx == 1:
            continue

        attr_code    = _clean(row[_OPT_COL_ATTR_CODE])
        attr_type_raw = _clean(row[_OPT_COL_ATTR_TYPE]).lower()
        option_code  = _clean(row[_OPT_COL_OPTION_CODE])
        prom_value   = _clean(row[_OPT_COL_PROM_VALUE])

        # Float/numeric defaults: рядки без option_code, де option_name_uk = дефолтне значення.
        # Перший зустрічний запис для кожного attr_code — глобальний дефолт.
        if attr_code and not option_code and attr_type_raw in _NON_OPTION_TYPES:
            default_value = _clean(row[_OPT_COL_OPTION_NAME])
            if default_value and attr_code not in float_defaults:
                float_defaults[attr_code] = default_value
                logger.debug(
                    "Рядок %d: float default | attr_code=%r value=%r",
                    row_idx, attr_code, default_value,
                )
            continue  # ці рядки не є опціями — решту полів не обробляємо

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

        # option_map: рядки де prom_option_name заповнений.
        # prom_param_name  — може бути comma-separated → _parse_prom_param_aliases
        # prom_option_name — може бути comma-separated → _parse_prom_option_aliases
        # Кожна пара (param_alias, option_alias) реєструється як окремий ключ
        # і вказує на одну і ту саму AttrOption.
        if prom_value and option_code:
            # PRIMARY: col 9 «Опції атрибутів» — self-contained, не залежить від синхронізації з «Сети атрибутів».
            # FALLBACK: attr_to_prom з «Сети атрибутів» — якщо col 9 порожній.
            prom_aliases = _parse_prom_param_aliases(row[_OPT_COL_PROM_PARAMS])
            if not prom_aliases:
                prom_aliases = attr_to_prom.get(attr_code)
                if prom_aliases:
                    logger.debug(
                        "Рядок %d: attr_code=%r — prom_param_name взято з «Сети атрибутів» "
                        "(col %d «Опції атрибутів» порожній): %r",
                        row_idx, attr_code, _OPT_COL_PROM_PARAMS, prom_aliases,
                    )
            prom_option_aliases = _parse_prom_option_aliases(row[_OPT_COL_PROM_VALUE])
            if not prom_aliases:
                logger.debug(
                    "Рядок %d: attr_code=%r не має prom_param_name "
                    "ні в «Сети атрибутів», ні в col %d «Опції атрибутів» → пропущено",
                    row_idx, attr_code, _OPT_COL_PROM_PARAMS,
                )
            elif not prom_option_aliases:
                logger.debug(
                    "Рядок %d: attr_code=%r prom_option_name порожній → пропущено",
                    row_idx, attr_code,
                )
            else:
                option = key_index[(attr_code, option_code)]
                for param_alias in prom_aliases:
                    for option_alias in prom_option_aliases:
                        option_map.setdefault(param_alias, {})[option_alias] = option
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
        # default_option_code може містити один або кілька кодів через кому (multiselect).
        # Кожен код шукається окремо в key_index → знайдені опції мержаться в один AttrOption:
        #   option_code = "bsz6btxa,wle9vq5zsirz1dni"  (comma-joined, без пробілів)
        #   option_name = "тварини, коти"               (comma-space-joined)
        # XML: valuecode="bsz6btxa,wle9vq5zsirz1dni">тварини, коти</param>
        # _render_select_param використовує option_code напряму → змін у генераторі не потрібно.
        opt_codes   = _parse_default_option_codes(default_code)
        found_opts  = [o for c in opt_codes if (o := key_index.get((attr_code, c))) is not None]
        missing     = [c for c in opt_codes if key_index.get((attr_code, c)) is None]

        if missing:
            logger.warning(
                "Рядок %d: default_option_code(s) %r не знайдено для attr_code=%r → пропущено",
                row_idx, missing, attr_code,
            )
        if not found_opts:
            continue

        if len(found_opts) == 1:
            default_option: AttrOption = found_opts[0]
        else:
            # multiselect: мержимо всі знайдені опції в одну зведену AttrOption.
            # attr_code / attr_name беруться з першої опції (однакові для всіх).
            default_option = AttrOption(
                attr_code=found_opts[0].attr_code,
                attr_name=found_opts[0].attr_name,
                option_code=",".join(o.option_code for o in found_opts),
                option_name=", ".join(o.option_name for o in found_opts),
            )
            logger.debug(
                "Рядок %d: multiselect default | attr_code=%r | merged %d опцій: %r → valuecode=%r",
                row_idx, attr_code, len(found_opts),
                [o.option_code for o in found_opts],
                default_option.option_code,
            )

        # AttrDefaultsMap: перший зустрічний запис для кожного attr_code.
        # Це «глобальний» дефолт — незалежно від категорії.
        if attr_code not in attr_defaults:
            attr_defaults[attr_code] = default_option

        set_codes = _parse_set_codes(set_codes_raw)
        if not set_codes:
            # Рядок без set_codes → тільки у attr_defaults (глобальний fallback).
            # У defaults по set_code не додаємо.
            logger.debug(
                "Рядок %d: attr_code=%r default_option_code=%r — set_codes порожній, "
                "додано тільки до attr_defaults як глобальний дефолт",
                row_idx, attr_code, default_code,
            )
            continue

        for set_code in set_codes:
            defaults.setdefault(set_code, {})[attr_code] = default_option
            def_mapped += 1

    wb.close()

    logger.info(
        "📐 option_map: %d prom_params / %d опцій | defaults: %d set_codes "
        "| attr_defaults: %d глобальних | numeric_map: %d",
        len(option_map), opt_mapped, len(defaults), len(attr_defaults), len(numeric_map),
    )
    return option_map, defaults, numeric_map, attr_defaults, float_defaults


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def get_option_map() -> OptionMap:
    """
    Маппінг select/multiselect опцій: prom_param_name → prom_value → AttrOption.

    Використання:
        option = get_option_map().get(prom_param, {}).get(prom_value)
    """
    option_map, _, _, _, _ = _load_indexes()
    return option_map


def get_defaults() -> DefaultsMap:
    """
    Дефолтні опції прив'язані до set_code: set_code → attr_code → AttrOption.

    Використання:
        for attr_code, default in get_defaults().get(set_code, {}).items():
            if attr_code not in already_mapped_codes:
                params.append(default)
    """
    _, defaults, _, _, _ = _load_indexes()
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
    _, _, numeric_map, _, _ = _load_indexes()
    return numeric_map


def get_attr_defaults() -> AttrDefaultsMap:
    """
    Глобальні дефолтні опції: attr_code → AttrOption.

    На відміну від get_defaults() не залежить від set_code категорії.
    Будується з колонки default_option_code аркуша «Опції атрибутів»;
    перший зустрічний запис для кожного attr_code.

    Призначення:
        - атрибути без set_codes, що діють на всі категорії (напр. «measure»)
        - fallback коли конкретний set_code відсутній у DefaultsMap

    Використання:
        option = get_attr_defaults().get("measure")
        if option:
            params.append(_render_select_param(option))
    """
    _, _, _, attr_defaults, _ = _load_indexes()
    return attr_defaults


def get_float_defaults() -> FloatDefaultsMap:
    """
    Глобальні дефолти для float/int/text/string атрибутів: attr_code → value string.

    Читається з колонки option_name_uk для рядків без option_code в аркуші «Опції атрибутів».
    Перший зустрічний запис для кожного attr_code.

    Призначення:
        - fallback значення для габаритних атрибутів (weight, height, length, width)
          та кратності (ratio), коли відповідний <param> відсутній у Prom XML

    Використання:
        float_defs = get_float_defaults()  # {"ratio": "1", "weight": "500", ...}
        _attr_defs = AttrDefaults(option_name_uk=float_defs, ...)
    """
    _, _, _, _, float_defaults = _load_indexes()
    return float_defaults
