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
    Для multiselect дефолтів у xlsx default_option_code задається через «;» (крапка з комою):
    Приклад у xlsx: "bsz6btxa ; wle9vq5zsirz1dni"
    У зібраному AttrOption option_code — через кому (вихідний формат Epicenter XML):
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


OptionMap       = dict[str, dict[str, list[AttrOption]]]  # prom_param → prom_value → list[AttrOption]
# Примітка: один (prom_param, prom_value) може маппитись на ДЕКІЛЬКА attr_code.
# Приклад: "Форм-фактор" / "Безконтактна картка" → [AttrOption(4626, "ключ"), AttrOption(10701, "картка")]
# dict[str, AttrOption] (старий тип) перезаписував попередній запис → "лузер" ніколи не маппився.
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
    Парсить default_option_code як semicolon-separated список кодів опцій.

    Розділювач — «;» (крапка з комою), узгоджено з prom_param_name і prom_option_name.
    Пробіли навколо «;» ігноруються.
    Порівняти з:
        _parse_prom_param_aliases  → розділювач «;»  (назви можуть містити кому)
        _parse_prom_option_aliases → розділювач «;»  (значення можуть містити кому)
        _parse_set_codes           → розділювач «,»  (числові ID, кома безпечна)

    Одне значення — повертає список з одного елемента (поведінка без змін).
    Кілька значень → _merge_options збирає combined option_code через «,» для XML.

    Приклад у xlsx: "bsz6btxa ; wle9vq5zsirz1dni"
                    → ["bsz6btxa", "wle9vq5zsirz1dni"]
    Приклад у xlsx: "measure_pcs"
                    → ["measure_pcs"]
    Приклад у xlsx: "0a3018e14116a9a8427677e287a3f265 ; 5eb7a105492792475511f1d900ca75b7"
                    → ["0a3018e14116a9a8427677e287a3f265", "5eb7a105492792475511f1d900ca75b7"]
    """
    if not raw:
        return []
    return [s.strip() for s in str(raw).split(";") if s.strip()]


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
# Internal helpers (option merging)
# ---------------------------------------------------------------------------

def _merge_options(opts: list[AttrOption], attr_code: str, row_idx: int) -> AttrOption:
    """
    Мержить список AttrOption в одну зведену AttrOption для multiselect-дефолтів.

    Один елемент → повертається без змін.
    Кілька → option_code = comma-joined, option_name = comma-space-joined.
    attr_code / attr_name беруться з першого елемента (однакові для всіх у групі).
    """
    if len(opts) == 1:
        return opts[0]
    result = AttrOption(
        attr_code=opts[0].attr_code,
        attr_name=opts[0].attr_name,
        option_code=",".join(o.option_code for o in opts),
        option_name=", ".join(o.option_name for o in opts),
    )
    logger.debug(
        "Рядок %d: multiselect default | attr_code=%r | merged %d опцій: %r → valuecode=%r",
        row_idx, attr_code, len(opts),
        [o.option_code for o in opts],
        result.option_code,
    )
    return result


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
    key_index:        dict[tuple[str, str], AttrOption] = {}          # (attr_code, option_code) → global fallback
    set_key_index:    dict[tuple[str, str, str], AttrOption] = {}     # (set_code, attr_code, option_code) → set-scoped
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

        # key_index: глобальний (attr_code, option_code) — перший рядок виграє (fallback).
        # set_key_index: (set_code, attr_code, option_code) — set-scoped, перемагає global.
        # Потрібен бо один і той самий option_code може мати РІЗНІ option_name у різних set_codes.
        # Приклад: option_code cf442e72... = "свердління з ударом" у set 2569,
        #          але "контактний" у set 8241 — без set_key_index дефолт для 8241
        #          отримав би назву з 2569.
        if option_code:
            option = AttrOption(
                attr_code=attr_code,
                attr_name=_clean(row[_OPT_COL_ATTR_NAME]),
                option_code=option_code,
                option_name=_clean(row[_OPT_COL_OPTION_NAME]),
            )
            # global fallback: перший рядок виграє
            if (attr_code, option_code) not in key_index:
                key_index[(attr_code, option_code)] = option
            # set-scoped: кожен set_code отримує свою AttrOption
            for sc in _parse_set_codes(row[_OPT_COL_SET_CODES]):
                set_key_index[(sc, attr_code, option_code)] = option

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
                        # append: один (param, value) може маппитись на кілька attr_code
                        option_map.setdefault(param_alias, {}).setdefault(option_alias, []).append(option)
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

    # --- крок 4: резолв дефолтів (key_index та set_key_index тепер повні) ---
    #
    # Проблема: один і той самий option_code може мати РІЗНІ option_name у різних set_codes.
    # Приклад: cf442e72... = «свердління з ударом» (set 2569) і «контактний» (set 8241).
    # key_index (глобальний) пишеться першим-виграє → дефолт для set 8241 отримував
    # назву з set 2569, якщо той аркуш ішов раніше.
    #
    # Рішення:
    #   - attr_defaults (global fallback) — як раніше, через key_index.
    #   - defaults[set_code][attr_code]  — резолв через set_key_index → key_index (fallback).
    for row_idx, attr_code, default_code, set_codes_raw in pending_defaults:
        opt_codes = _parse_default_option_codes(default_code)
        set_codes = _parse_set_codes(set_codes_raw)

        # --- attr_defaults: глобальний fallback через key_index (перший запис виграє) ---
        if attr_code not in attr_defaults:
            found_global = [
                o for c in opt_codes
                if (o := key_index.get((attr_code, c))) is not None
            ]
            missing_global = [c for c in opt_codes if key_index.get((attr_code, c)) is None]
            if missing_global:
                logger.warning(
                    "Рядок %d: default_option_code(s) %r не знайдено в key_index "
                    "для attr_code=%r → пропущено з attr_defaults",
                    row_idx, missing_global, attr_code,
                )
            if found_global:
                attr_defaults[attr_code] = _merge_options(found_global, attr_code, row_idx)

        if not set_codes:
            # Рядок без set_codes → тільки у attr_defaults (глобальний fallback).
            logger.debug(
                "Рядок %d: attr_code=%r default_option_code=%r — set_codes порожній, "
                "додано тільки до attr_defaults як глобальний дефолт",
                row_idx, attr_code, default_code,
            )
            continue

        # --- defaults: set-scoped резолв: set_key_index → key_index (fallback) ---
        #
        # ВАЖЛИВО: якщо default_option_code існує тільки в іншому set (наприклад,
        # cf442e72... є опцією set 2569 але НЕ set 8241) — fallback на key_index
        # поверне неправильну назву. Це симптом того, що в xlsx не оновлено
        # default_option_code для рядків відповідного set_code.
        # Виявляємо такі випадки через cross_set_fallback і логуємо WARNING.
        for set_code in set_codes:
            found_opts: list[AttrOption] = []
            cross_set_fallback: list[str] = []   # option_code знайдено тільки в key_index (інший set)
            missing: list[str] = []
            for c in opt_codes:
                set_opt = set_key_index.get((set_code, attr_code, c))
                if set_opt is not None:
                    found_opts.append(set_opt)
                else:
                    global_opt = key_index.get((attr_code, c))
                    if global_opt is not None:
                        # option_code існує, але НЕ в поточному set → неправильний дефолт
                        cross_set_fallback.append(c)
                        logger.warning(
                            "Рядок %d: default_option_code %r знайдено в key_index "
                            "(option_name=%r), але ВІДСУТНЄ в set_key_index для "
                            "set_code=%r / attr_code=%r. "
                            "Схоже, що default_option_code у xlsx не оновлено для цього set. "
                            "Пропускаємо — не додаємо до defaults[%r]",
                            row_idx, c, global_opt.option_name,
                            set_code, attr_code, set_code,
                        )
                    else:
                        missing.append(c)

            if missing:
                logger.warning(
                    "Рядок %d: default_option_code(s) %r не знайдено ні в set_key_index, "
                    "ні в key_index для attr_code=%r set_code=%r → пропущено",
                    row_idx, missing, attr_code, set_code,
                )
            if not found_opts:
                # Якщо всі коди були cross_set_fallback або missing → не пишемо дефолт.
                # Краще відсутній дефолт (помітно), ніж неправильний (непомітно).
                continue

            defaults.setdefault(set_code, {})[attr_code] = _merge_options(
                found_opts, attr_code, row_idx
            )
            def_mapped += 1

    wb.close()

    logger.info(
        "📐 option_map: %d prom_params / %d (param,value) ключів / %d opt_mapped записів "
        "| defaults: %d set_codes | attr_defaults: %d глобальних | numeric_map: %d",
        len(option_map),
        sum(len(vals) for vals in option_map.values()),
        opt_mapped,
        len(defaults),
        len(attr_defaults),
        len(numeric_map),
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
