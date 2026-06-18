"""
Генерує фід для Епіцентру:
  1. Завантажує XML фід з сайту
  2. Читає data/markets/epicenter_coefficients.csv (через services/market_pricing.py)
  3. Визначає базову ціну: Оптова_ціна з *_old.csv або fallback на ціну з XML
  4. Базова ціна × коефіцієнт категорії = нова ціна
  5. Інжектує атрибути Epicenter (<category>, <attribute_set>, <param>) з маппінгу
  6. Зберігає результат в data/markets/epicenter_feed.xml

Запуск локально:
    python scripts/generate_epicenter_feed.py

Запуск у GitHub Actions: Stage 5 → needs: process-and-publish
ВАЖЛИВО: у GitHub Actions job повинен відновити *_old.csv з data-latest
(see pipeline.yml step "Restore *_old.csv from data-latest").
"""

import logging
import re
from collections import Counter
from collections.abc import Callable
from pathlib import Path
from dataclasses import dataclass
from typing import Final, Literal

_logger = logging.getLogger(__name__)

from constants_feed_url import FEED_URL_PROM_UK as FEED_URL
from generate_utils_feed import (
    add_name_ua,
    fetch_xml,
    fill_missing_vendor,
    filter_unavailable_offers,
    load_wholesale_price_index,
    parse_currency_rates,
)
from services.epicenter_params_to_description_service import inject_params_into_description
from services.epicenter_stop_brand_service import filter_stop_brand_offers
from services.epicenter_text_sanitizer_service import sanitize_russian_chars, strip_html_classes, strip_external_links
from services.epicenter_attr_service import (
    CategoryAttrRules,
    AttrMeta,
    AttrOption,
    NON_OPTION_TYPES,
    get_category_attr_rules,
)
from services.epicenter_category_service import CategoryEntry, resolve_category, flush_fallback_warnings
from services.market_pricing import apply_market_prices

# ---------------------------------------------------------------------------
# Market-specific config
# ---------------------------------------------------------------------------

MARKET = "epicenter"

# Prom category IDs що повністю виключаються з Epicenter фіду.
# Всі офери з цими categoryId видаляються на етапі inject_epicenter_attrs.
EXCLUDED_PROM_CAT_IDS: Final[frozenset[int]] = frozenset({
    20783,
})

ROOT = Path(__file__).parents[1]
OUTPUT_PATH = ROOT / "data" / "markets" / "epicenter_feed.xml"


# Regex для парсингу <param> тегів Прому в тілі офера.
# _PROM_PARAM_STRIP_RE: об'єднує парсинг + видалення в один прохід (використовується в _on_offer).
# _PROM_PARAM_RE: залишено як fallback якщо потрібен тільки finditer без видалення.
_PROM_PARAM_RE = re.compile(
    r'<param\b[^>]*\bname="([^"]+)"[^>]*>(.*?)</param>',
    re.DOTALL,
)
_PROM_PARAM_STRIP_RE = re.compile(
    r'[ \t]*<param\b[^>]*\bname="([^"]+)"[^>]*>(.*?)</param>[ \t]*\n?',
    re.DOTALL,
)
_CDATA_RE = re.compile(r'<!\[CDATA\[(.*?)\]\]>', re.DOTALL)


# ---------------------------------------------------------------------------
# Unit normalization helpers for float system attributes
# ---------------------------------------------------------------------------

# Matches optional leading sign, integer or decimal (comma or dot), optional unit
_UNIT_VALUE_RE: Final[re.Pattern[str]] = re.compile(
    r"^\s*([+-]?\d+(?:[.,]\d+)?)\s*(кг|г|см|мм|kg|g|cm|mm)?\s*$",
    re.IGNORECASE,
)


def _parse_numeric(raw: str) -> tuple[float | None, str]:
    """Returns (numeric_value, unit_lowercase) or (None, '') on parse failure."""
    m = _UNIT_VALUE_RE.match(raw)
    if not m:
        return None, ""
    try:
        return float(m.group(1).replace(",", ".")), (m.group(2) or "").lower()
    except ValueError:
        return None, ""


def _to_grams(raw: str) -> str:
    """
    Нормалізує значення ваги до грамів.

    кг / kg → × 1000
    г / g   → без змін
    без одиниці → без змін (вважаємо що вже в г)
    """
    val, unit = _parse_numeric(raw)
    if val is None:
        return raw
    if unit in ("кг", "kg"):
        val *= 1000
    return f"{val:g}"


def _to_mm(raw: str) -> str:
    """
    Нормалізує значення розміру до міліметрів.

    см / cm → × 10
    мм / mm → без змін
    без одиниці → без змін (вважаємо що вже в мм)
    """
    val, unit = _parse_numeric(raw)
    if val is None:
        return raw
    if unit in ("см", "cm"):
        val *= 10
    return f"{val:g}"


# ---------------------------------------------------------------------------
# System attributes config
# ---------------------------------------------------------------------------

AttrType = Literal["float", "text", "array", "select", "multiselect"]


@dataclass(frozen=True)
class AttrConfig:
    """Конфіг одного системного атрибута Epicenter."""

    attr_code: str
    attr_type: AttrType
    attr_name_uk: str
    prom_param_name: str | None  # None → одразу до дефолту
    prom_aliases: tuple[str, ...] = ()
    normalize: Callable[[str], str] | None = None  # опціональна нормалізація значення (одиниці виміру)


def resolve_attr_value(
    cfg: AttrConfig,
    prom_params: dict[str, str],
    rules: CategoryAttrRules,
) -> dict[str, str] | None:
    """
    Повертає payload для <param> або None (→ drop).

    float / text / array : value = текст, option_code не передається
    select / multiselect : valuecode = option_code з xlsx (обов'язковий)
    """
    raw_value: str | None = None

    # 1. Шукаємо в Prom params
    if cfg.prom_param_name is not None:
        for key in (cfg.prom_param_name, *cfg.prom_aliases):
            if found := prom_params.get(key):
                raw_value = found
                break

    # 2. Fallback залежно від attr_type
    if raw_value is None:
        if cfg.attr_type in NON_OPTION_TYPES:
            raw_value = rules.global_non_option_defaults.get(cfg.attr_code)
        else:
            # select / multiselect — дефолт через option_code, не option_name_uk
            default = rules.system_select_default(cfg.attr_code)
            if default is None:
                _logger.warning(
                    "attr drop | no default_option_code | attr_code=%s", cfg.attr_code
                )
                return None
            return {
                "paramcode": cfg.attr_code,
                "name": cfg.attr_name_uk,
                "valuecode": default.option_code,
                "text": default.option_name,
            }

    if raw_value is None:
        return None

    # Нормалізація одиниць виміру (кг→г, см→мм) якщо задано для цього атрибута
    if cfg.normalize is not None:
        raw_value = cfg.normalize(raw_value)

    return {
        "paramcode": cfg.attr_code,
        "name": cfg.attr_name_uk,
        "value": raw_value,
    }


# Системні атрибути Epicenter, спільні для всіх категорій.
# float  → значення береться з Prom-параму або option_name_uk дефолту
# select → valuecode береться з default_option_code у xlsx через CategoryAttrRules
#
# country_of_origin та brand — НЕ тут:
#   вони є select-атрибутами категорійного рівня → обробляються через option_map (крок 6c).
# Для коректної роботи country_of_origin повинен бути у xlsx:
#   «Сети атрибутів»  → prom_param_name = "Країна-виробник", attr_type = select
#   «Опції атрибутів» → рядки: prom_option_name = "Китай" → option_code = "chn" і т.д.
_ATTRS: Final[tuple[AttrConfig, ...]] = (
    AttrConfig(
        attr_code="weight",
        attr_type="float",
        attr_name_uk="Вага",
        prom_param_name="Вага",
        # RU aliases: Prom-фід може віддавати назви параметрів будь-якою мовою
        prom_aliases=("Вес",),
        normalize=_to_grams,   # кг → г; г/без одиниці → без змін
    ),
    AttrConfig(
        attr_code="width",
        attr_type="float",
        attr_name_uk="Ширина",
        prom_param_name="Ширина",  # однаково UA і RU
        normalize=_to_mm,          # см → мм; мм/без одиниці → без змін
    ),
    AttrConfig(
        attr_code="height",
        attr_type="float",
        attr_name_uk="Висота",
        prom_param_name="Висота",
        prom_aliases=("Высота",),
        normalize=_to_mm,
    ),
    AttrConfig(
        attr_code="length",
        attr_type="float",
        attr_name_uk="Глибина",
        prom_param_name="Глибина",
        prom_aliases=("Довжина", "Длина", "Глубина"),
        normalize=_to_mm,
    ),
    AttrConfig(
        attr_code="ratio",
        attr_type="float",
        attr_name_uk="Мінімальна кратність товару",
        prom_param_name="Кратність",
        prom_aliases=("Кратность",),
    ),
    AttrConfig(
        attr_code="measure",
        attr_type="select",
        attr_name_uk="Одиниця виміру та кількість",
        prom_param_name=None,
    ),
)

# Множина prom_param_name значень, що обробляються через _ATTRS (для фільтрації в 5c).
_ATTRS_PROM_NAMES: Final[frozenset[str]] = frozenset(
    name
    for cfg in _ATTRS
    if cfg.prom_param_name is not None
    for name in (cfg.prom_param_name, *cfg.prom_aliases)
)


# Атрибути, що повинні бути присутні у кожному офері незалежно від наявності даних.
# Після кроків 6–7c будь-який незамаплений отримує last-resort fallback:
#   float/text  → "0"  (або значення з float_defaults якщо є у xlsx)
#   select      → global_select_default з xlsx  (warning якщо відсутній у xlsx)
# tuple (не frozenset) — детермінований порядок додавання в params.
_ALWAYS_PRESENT_ATTR_CODES: Final[tuple[str, ...]] = (
    "height",
    "length",
    "width",
    "weight",
    "ratio",
    "measure",
    "country_of_origin",
    "brand",
)

# Швидкий доступ до AttrConfig за attr_code (тільки для системних float-атрибутів з _ATTRS).
_ATTR_CONFIG_BY_CODE: Final[dict[str, AttrConfig]] = {
    cfg.attr_code: cfg for cfg in _ATTRS
}

# Lookup: attr_name_uk → AttrConfig для атрибутів-розмірів (width / height / length).
# Дозволяє кроку 6c отримувати вже нормоване індивідуальне значення
# замість складеного рядка типу «Ф120х121х173.5 мм» від окремого prom-параметра («Розміри» тощо).
_DIM_ATTR_CONFIG_BY_NAME: Final[dict[str, AttrConfig]] = {
    cfg.attr_name_uk: cfg
    for cfg in _ATTRS
    if cfg.attr_code in ("width", "height", "length")
}

# Підмножина _ALWAYS_PRESENT_ATTR_CODES з типом float/text/array (NON_OPTION_TYPES).
# Кроки 6a+6b НЕ лічать їх у attr_drops — крок 8 ЗАВЖДИ підставить "0".
_ALWAYS_PRESENT_FLOAT_CODES: Final[frozenset[str]] = frozenset(
    code for code in _ALWAYS_PRESENT_ATTR_CODES
    if code in _ATTR_CONFIG_BY_CODE
    and _ATTR_CONFIG_BY_CODE[code].attr_type in NON_OPTION_TYPES
)


# ---------------------------------------------------------------------------
# Epicenter XML helpers
# ---------------------------------------------------------------------------

def _xml_attr(value: str) -> str:
    """Екранує спецсимволи XML для підстановки у значення атрибутів (у лапках)."""
    return (
        value
        .replace("&", "&amp;")
        .replace('"', "&quot;")
        .replace("<", "&lt;")
        .replace(">", "&gt;")
    )


def _strip_cdata(value: str) -> str:
    """Витягує текст з CDATA-обгортки; якщо її немає — повертає рядок як є."""
    m = _CDATA_RE.match(value.strip())
    return m.group(1).strip() if m else value.strip()


def _render_select_param(option: AttrOption) -> str:
    """
    <param paramcode="6067" name="Кут огляду" valuecode="opt_120">120°</param>
    """
    return (
        f'<param paramcode="{_xml_attr(option.attr_code)}" '
        f'name="{_xml_attr(option.attr_name)}" '
        f'valuecode="{_xml_attr(option.option_code)}">'
        f'{option.option_name}</param>'
    )


def _render_numeric_param(meta: AttrMeta, value: str) -> str:
    """
    <param paramcode="width" name="Ширина"><![CDATA[100]]></param>
    """
    return (
        f'<param paramcode="{_xml_attr(meta.attr_code)}" '
        f'name="{_xml_attr(meta.attr_name)}">'
        f'<![CDATA[{value}]]></param>'
    )


def _render_attr_payload(payload: dict[str, str]) -> str:
    """
    Рендерить <param> з payload, поверненого resolve_attr_value.

    select / multiselect (є "valuecode"):
        <param paramcode="measure" name="..." valuecode="measure_pcs">шт.</param>
    float / text / array (є "value"):
        <param paramcode="width" name="..."><![CDATA[100]]></param>
    """
    if "valuecode" in payload:
        return (
            f'<param paramcode="{_xml_attr(payload["paramcode"])}" '
            f'name="{_xml_attr(payload["name"])}" '
            f'valuecode="{_xml_attr(payload["valuecode"])}">'
            f'{payload.get("text", "")}</param>'
        )
    return (
        f'<param paramcode="{_xml_attr(payload["paramcode"])}" '
        f'name="{_xml_attr(payload["name"])}">'
        f'<![CDATA[{payload["value"]}]]></param>'
    )


# ---------------------------------------------------------------------------
# Core: inject Epicenter attributes into every offer
# ---------------------------------------------------------------------------

def inject_epicenter_attrs(xml: str) -> tuple[str, list[CategoryEntry]]:
    """
    Для кожного офера:
      1. <categoryId>N</categoryId>
            → <category code="EPIC_CODE">EPIC_NAME</category>
               <attribute_set code="EPIC_CODE">EPIC_NAME</attribute_set>
      2. Усі prom <param name="...">...</param> — видаляються.
      3. Prom params → Epicenter <param paramcode="..."> через індекси:
            _ATTRS      — системні атрибути (габарити, вага, кратність, measure)
                          float → CDATA, select → valuecode з xlsx
            option_map  — select/multiselect категорійні (з valuecode), включно з country та brand
            numeric_map — float/int/text/string категорійні (CDATA значення) з xlsx
            defaults    — обов'язкові атрибути без маппінгу (fallback option)
      4. Оффери без маппінгу категорії залишаються без змін (логується).

    Повертає оновлений XML.
    """
    mapped_count    = 0
    skipped_no_cat  = 0
    skipped_cat_ids: set[int] = set()
    # dict keyed by epicenter code — дедублікація, порядок першої появи
    used: dict[str, CategoryEntry] = {}
    total_params    = 0
    missing_measure = 0
    brand_defaults_total: int = 0             # скільки офферів отримали дефолтний brand
    missed_brands: Counter[str] = Counter()   # prom-бренд → кількість (є в Prom, нема в Epicenter)
    attr_drops: Counter[str] = Counter()      # attr_code → кількість дропів (no value and no default)

    always_absent_drops: Counter[str] = Counter()  # attr_code → к-ть офферів без global default у xlsx
    always_present_fallbacks: Counter[str] = Counter()  # step 7: float/text "0" fallback per attr_code
    brand_prom_names_by_set: dict[str, frozenset[str]] = {}

    def _brand_prom_names(rules: CategoryAttrRules) -> frozenset[str]:
        if rules.set_code not in brand_prom_names_by_set:
            brand_prom_names_by_set[rules.set_code] = rules.prom_names_for_attr("brand")
        return brand_prom_names_by_set[rules.set_code]

    def _on_offer(m: re.Match) -> str:
        nonlocal mapped_count, skipped_no_cat, total_params, missing_measure, brand_defaults_total, attr_drops

        offer_id   = m.group(1)
        tail_attrs = m.group(2)
        body       = m.group(3)

        # --- 1. Знаходимо prom categoryId ---
        cat_match = re.search(r'<categoryId>(\d+)</categoryId>', body)
        if not cat_match:
            return m.group(0)

        prom_cat_id = int(cat_match.group(1))

        if prom_cat_id in EXCLUDED_PROM_CAT_IDS:
            return ""

        # --- 2. Парсимо prom params (до визначення категорії —
        #        потрібні для param-based routing Epicenter).
        #        Prom може передавати кілька <param> тегів з однаковим name (multiselect).
        #        Дублікати об'єднуються через ", " →
        #        step 6c розіб'є по ", " і знайде кожне значення в option_map окремо.
        prom_params: dict[str, str] = {}
        for _pm in _PROM_PARAM_RE.finditer(body):
            _name  = _pm.group(1).strip()
            _value = _strip_cdata(_pm.group(2))
            if _name in prom_params:
                prom_params[_name] = f"{prom_params[_name]}, {_value}"
            else:
                prom_params[_name] = _value

        # --- 2b. Pre-resolve dimension values (Ширина/Висота/Глибина) для кроку 6c ---
        # Деякі продукти мають одночасно окремі prom-параметри (Ширина=120, Висота=121)
        # і складений рядок в іншому параметрі («Розміри»=«Ф120х121х173.5 мм»).
        # _ATTRS (крок 6a) використовує окремі значення коректно.
        # Category numeric attrs (attr_code=110/111, крок 6c) раніше отримували
        # складений рядок без нормалізації — dim_resolved виправляє це.
        dim_resolved: dict[str, str] = {}
        for _dcfg in _ATTRS:
            if _dcfg.attr_code not in ("width", "height", "length"):
                continue
            for _dkey in (_dcfg.prom_param_name, *_dcfg.prom_aliases):
                if _dkey and (_draw := prom_params.get(_dkey)):
                    dim_resolved[_dcfg.attr_name_uk] = (
                        _dcfg.normalize(_draw) if _dcfg.normalize else _draw
                    )
                    break

        # --- 3. Визначаємо категорію Epicenter (може залежати від prom_params) ---
        category = resolve_category(prom_cat_id, prom_params)

        if not category:
            skipped_no_cat += 1
            skipped_cat_ids.add(prom_cat_id)
            return m.group(0)

        cat_code = category['code']
        cat_name = category['name']
        used.setdefault(cat_code, category)
        rules = get_category_attr_rules(cat_code)

        # --- 4. Замінюємо <categoryId> на <category> + <attribute_set> ---
        body = body.replace(
            cat_match.group(0),
            f'<category code="{cat_code}">{cat_name}</category>\n'
            f'<attribute_set code="{cat_code}">{cat_name}</attribute_set>',
        )

        # --- 5. Видаляємо ВСІ prom <param> теги (разом з рядком що залишається після видалення) ---
        body = re.sub(r'[ \t]*<param\b[^>]*>.*?</param>[ \t]*\n?', '', body, flags=re.DOTALL)

        # --- 6. Будуємо epicenter params ---
        params: list[str] = []
        mapped_attr_codes: set[str] = set()

        # ── 6a+6b. Системні атрибути (_ATTRS) ───────────────────────────────
        # Для select-системних атрибутів (measure) пріоритет:
        # rules.select_defaults → rules.global_select_defaults.
        # Для float-системних атрибутів дефолт береться з global_non_option_defaults.
        for cfg in _ATTRS:
            if cfg.attr_code in mapped_attr_codes:
                continue
            payload = resolve_attr_value(cfg, prom_params, rules)
            if payload is None:
                if cfg.attr_code == "measure":
                    missing_measure += 1
                elif cfg.attr_code not in _ALWAYS_PRESENT_FLOAT_CODES:
                    # Тільки справжні дропи — always-present float-атрибути врятує крок 7
                    attr_drops[cfg.attr_code] += 1
                continue
            params.append(_render_attr_payload(payload))
            mapped_attr_codes.add(cfg.attr_code)

        # ── 6c. Категорійні атрибути з xlsx ──────────────────────────────────
        # Сюди потрапляють: select/multiselect (option_map) та float/int/text/string (numeric_map).
        # Включно з country_of_origin ("Країна-виробник") та brand ("Бренд") — через option_map.

        for prom_name, prom_value in prom_params.items():
            if not prom_value:
                continue

            # Системні prom_name вже оброблені вище (_ATTRS) — пропускаємо
            if prom_name in _ATTRS_PROM_NAMES:
                continue

            # select / multiselect — маппінг через правила поточного set_code.
            # rules.option_map вже містить правильний set-scoped option_code
            # або дозволений глобальний fallback.
            # prom_value може містити кілька значень через ", ":
            #   - multiselect: кілька <param> тегів з однаковим name (об'єднані у крок 2)
            #   - або одне значення без коми
            # Кожне значення маппиться окремо → окремий <param> тег.
            param_opts = rules.option_map.get(prom_name, {})
            if param_opts:
                # option_map має маппінг для цього prom_param_name.
                # Один (prom_param, prom_value) може маппитись на КІЛЬКА attr_code:
                #   "Форм-фактор" / "Безконтактна картка" → [4626 "ключ", 10701 "картка"]
                # param_opts[value] — list[AttrOption], ітеруємо всі.
                is_brand_param = rules.option_param_targets_attr(prom_name, "brand")
                for single_value in (v.strip() for v in prom_value.split(",")):
                    if not single_value:
                        continue
                    matched_options: list[AttrOption] = param_opts.get(single_value, [])
                    if matched_options:
                        for option in matched_options:
                            if option.attr_code not in mapped_attr_codes:
                                params.append(_render_select_param(option))
                                mapped_attr_codes.add(option.attr_code)
                    else:
                        # Значення відсутнє в option_map → піде дефолт у кроці 6.
                        if is_brand_param and "brand" not in mapped_attr_codes:
                            missed_brands[single_value] += 1
                        _logger.debug(
                            "offer %s | option_map miss | prom_param=%r value=%r "
                            "— значення відсутнє у маппінгу, дефолт буде застосовано",
                            offer_id, prom_name, single_value,
                        )

            # float / int / text / string — значення напряму
            # rules.numeric_map вже має пріоритет set-scoped над global.
            numeric_metas = rules.numeric_map.get(prom_name, [])
            for meta in numeric_metas:
                if meta.attr_code not in mapped_attr_codes:
                    # Для category numeric attrs що відповідають розмірам (Ширина/Висота/Глибина):
                    # підставляємо pre-resolved індивідуальне значення (вже нормоване через _to_mm)
                    # замість сирого prom_value (напр. «Ф120х121х173.5 мм»).
                    # Fallback 1: якщо окремого prom-параметра нема — нормалізуємо prom_value напряму.
                    # Fallback 2: якщо attr не є розміром — підставляємо prom_value без змін.
                    _pre = dim_resolved.get(meta.attr_name)
                    if _pre is not None:
                        _meta_value = _pre
                    else:
                        _dim_cfg = _DIM_ATTR_CONFIG_BY_NAME.get(meta.attr_name)
                        _meta_value = (
                            _dim_cfg.normalize(prom_value)
                            if _dim_cfg and _dim_cfg.normalize
                            else prom_value
                        )
                    params.append(_render_numeric_param(meta, _meta_value))
                    mapped_attr_codes.add(meta.attr_code)

        # --- 7. Дефолти для атрибутів без маппінгу ---
        # Застосовуємо set-специфічні дефолти.
        for attr_code, default in rules.select_defaults.items():
            if attr_code not in mapped_attr_codes:
                params.append(_render_select_param(default))
                mapped_attr_codes.add(attr_code)
                if attr_code == "brand":
                    brand_defaults_total += 1
                    brand_names = _brand_prom_names(rules)
                    if brand_names and not any(name in prom_params for name in brand_names):
                        missed_brands["(відсутній у Prom)"] += 1

        # --- 7b. Fallback: глобальний дефолт brand якщо не замаплено ---
        # Застосовуємо ТІЛЬКИ brand — не всі global_select_defaults,
        # щоб не смітити чужими атрибутами.
        if "brand" not in mapped_attr_codes:
            _brand_default = rules.global_select_default("brand")
            if _brand_default:
                params.append(_render_select_param(_brand_default))
                mapped_attr_codes.add("brand")
                brand_defaults_total += 1
                brand_names = _brand_prom_names(rules)
                if brand_names and not any(name in prom_params for name in brand_names):
                    missed_brands["(відсутній у Prom)"] += 1

        # --- 7c. Numeric (array/float/text) категорійні дефолти ---
        # Атрибути без option_code (не select), що мають задане значення за замовчуванням
        # і немають маппінгу з Prom (напр. «Максимальний перетин дроту» для set 2793).
        for attr_code, (meta, value) in rules.numeric_defaults.items():
            if attr_code not in mapped_attr_codes:
                params.append(_render_numeric_param(meta, value))
                mapped_attr_codes.add(attr_code)

        # --- 8. Гарантовані always-present атрибути ---
        # Після всіх кроків 5–6c переконуємось що кожен з 8 обов'язкових атрибутів
        # присутній у params. Last-resort fallback:
        #   float/text → "0" (або global_non_option_defaults якщо задано у xlsx)
        #   select     → global_select_default з xlsx (warning якщо відсутній)
        _always_absent: list[str] = []
        for _ap_code in _ALWAYS_PRESENT_ATTR_CODES:
            if _ap_code in mapped_attr_codes:
                continue
            _ap_cfg = _ATTR_CONFIG_BY_CODE.get(_ap_code)
            if _ap_cfg is not None and _ap_cfg.attr_type in NON_OPTION_TYPES:
                # float/text/array — last-resort "0"
                _ap_raw = rules.global_non_option_defaults.get(_ap_code, "0")
                if _ap_cfg.normalize is not None:
                    _ap_raw = _ap_cfg.normalize(_ap_raw)
                _ap_meta = AttrMeta(
                    attr_code=_ap_cfg.attr_code,
                    attr_name=_ap_cfg.attr_name_uk,
                    attr_type=_ap_cfg.attr_type,
                )
                params.append(_render_numeric_param(_ap_meta, _ap_raw))
                mapped_attr_codes.add(_ap_code)
                always_present_fallbacks[_ap_code] += 1
            else:
                # select/multiselect (measure, country_of_origin, brand)
                _ap_opt = rules.global_select_default(_ap_code)
                if _ap_opt is not None:
                    params.append(_render_select_param(_ap_opt))
                    mapped_attr_codes.add(_ap_code)
                else:
                    _always_absent.append(_ap_code)
        if _always_absent:
            for _code in _always_absent:
                always_absent_drops[_code] += 1

        # --- 9. Вставляємо блок «Параметри» в description_ua ---
        body = inject_params_into_description(body, prom_params)

        # --- 10. Вставляємо params у кінець body ---
        if params:
            params_block = '\n'.join(params)
            body = body.rstrip() + f'\n{params_block}\n'

        total_params += len(params)
        mapped_count += 1
        return f'<offer id="{offer_id}"{tail_attrs}>{body}</offer>'

    xml = re.sub(
        r'<offer\s+id="(\d+)"([^>]*)>(.*?)</offer>',
        _on_offer,
        xml,
        flags=re.DOTALL,
    )

    print(
        f'🎯 Epicenter attrs: {mapped_count} офферів оброблено '
        f'| {total_params} params вставлено '
        f'| без маппінгу категорії: {skipped_no_cat}'
        + (f' | measure не знайдено у xlsx: {missing_measure}' if missing_measure else '')
    )
    if brand_defaults_total:
        # Бренди що були у Prom але відсутні в option_map Epicenter
        known = sum(v for k, v in missed_brands.items() if k != "(відсутній у Prom)")
        absent = missed_brands.get("(відсутній у Prom)", 0)
        brands_str = ", ".join(
            f"{brand} ({cnt}x)" if cnt > 1 else brand
            for brand, cnt in missed_brands.most_common()
            if brand != "(відсутній у Prom)"
        )
        print(
            f"🏷️  Brand → дефолт Epicenter: {brand_defaults_total} товарів"
            + (f" | відсутній у Prom: {absent}" if absent else "")
            + (f" | бренди з Prom без маппінгу ({known}): {brands_str}" if brands_str else "")
        )
    if skipped_cat_ids:
        ids_str = ', '.join(str(i) for i in sorted(skipped_cat_ids))
        _logger.warning('Prom categoryId без маппінгу (%d): %s', len(skipped_cat_ids), ids_str)
    if always_present_fallbacks:
        fb_str = ', '.join(f'{code}×{cnt}' for code, cnt in always_present_fallbacks.most_common())
        _logger.info('attr float fallback→"0" (step 7): %s', fb_str)
    if attr_drops:
        drops_str = ', '.join(f'{code}×{cnt}' for code, cnt in attr_drops.most_common())
        _logger.warning('⚠️  attr drop (no value+no default): %s', drops_str)
    if always_absent_drops:
        absent_str = ', '.join(f'{code}×{cnt}' for code, cnt in always_absent_drops.most_common())
        _logger.warning('⚠️  always-present attr без global default у xlsx: %s', absent_str)
    return xml, list(used.values())


# ---------------------------------------------------------------------------
# XML cleanup: strip Prom-specific blocks not needed in Epicenter feed
# ---------------------------------------------------------------------------


def strip_prom_shop_block(xml: str) -> str:
    """
    Витягує <offers>...</offers> з внутрішньості <shop> і піднімає його
    напряму в <yml_catalog>, видаляючи усе інше (<name>, <company>, <currencies>, <categories>).

    Prom: <yml_catalog><shop><name/>...<offers>...</offers></shop></yml_catalog>
    Epicenter: <yml_catalog><offers>...</offers></yml_catalog>
    """
    offers_match = re.search(r'<offers>.*?</offers>', xml, flags=re.DOTALL)
    if not offers_match:
        _logger.error("<offers> блок не знайдено — фід не може бути збережено")
        return xml

    yml_open_match = re.search(r'<yml_catalog[^>]*>', xml)
    if not yml_open_match:
        _logger.error("<yml_catalog> не знайдено")
        return xml

    result = f'{yml_open_match.group(0)}\n{offers_match.group(0)}\n</yml_catalog>'
    print("🗑️  Видалено блок <shop> (name, company, url, currencies, categories)")
    return result


# ---------------------------------------------------------------------------
# Epicenter: нормалізація тегів назв / описів + очищення Prom-специфічних полів
# ---------------------------------------------------------------------------

# Prom-теги верхнього рівня оффера, яких немає у форматі Epicenter.
# country_of_origin і brand передаються через <param paramcode="..."> (option_map).
# ВАЖЛИВО: викликати ПІСЛЯ fill_missing_vendor — вона залежить від цих тегів.
_PROM_FIELDS_TO_STRIP: Final[tuple[str, ...]] = (
    "vendor",
    "country_of_origin",
    "stock_quantity",
    "currencyId",
    "url",
    "article",
)

# Перейменування тегів: (prom_tag, epicenter_tag, lang)
# Prom UK-only фід віддає тільки <name> і <description> (без суфікса _ua) — вже українською мовою.
# Обидва теги ренеймимо до Epicenter-формату з lang="ua".
_TAG_RENAMES: Final[tuple[tuple[str, str, str], ...]] = (
    ("name",        "name",        "ua"),
    ("description", "description", "ua"),
)


def strip_prom_offer_fields(xml: str) -> str:
    """
    Видаляє Prom-специфічні теги з фіду:
        <vendor>, <country_of_origin>, <stock_quantity>

    Ці поля є стандартними у Prom-форматі, але відсутні у форматі Epicenter.
    country_of_origin і brand передаються через <param paramcode="...">опції.
    """
    removed: dict[str, int] = {}
    for tag in _PROM_FIELDS_TO_STRIP:
        # [^<]* — безпечніше ніж .*? DOTALL: вміст цих тегів — плаский текст (XML не дозволяє < у значенні)
        # [ \t]* і \n? — прибирають відступи та порожній рядок що залишається після видалення тегу
        xml, n = re.subn(rf'[ \t]*<{tag}>[^<]*</{tag}>[ \t]*\n?', '', xml)
        if n:
            removed[tag] = n
    if removed:
        summary = ', '.join(f'<{t}>\xd7{c}' for t, c in removed.items())
        print(f"🗑️  Видалено Prom-тегів з офферів: {summary}")
    return xml


_DESCRIPTION_TAGS: Final[frozenset[str]] = frozenset({"description"})


def _wrap_cdata(content: str) -> str:
    """
    Повертає контент, загорнутий у єдину CDATA-секцію.

    Розгортає ВСІ CDATA-секції через sub (не match), тому коректно
    обробляє змішаний контент: CDATA-обгортка Prom + чистий HTML,
    дописаний після неї (наприклад блок «Параметри» від
    inject_params_into_description).

    Приклад: '<![CDATA[html...]]>\n<div>Параметри</div>'
             → '<![CDATA[html...\n<div>Параметри</div>]]>'

    Захищає від ']]>' всередині контенту (заміна на ']]]]><![CDATA[>').
    """
    # Розгортаємо всі CDATA-секції; якщо їх нема — sub не змінює рядок
    text = _CDATA_RE.sub(lambda m: m.group(1), content).strip()
    # Екрануємо ']]>' щоб не закрити CDATA передчасно
    text = text.replace(']]>', ']]]]><![CDATA[>')
    return f'<![CDATA[{text}]]>'


def normalize_name_description_tags(xml: str) -> str:
    """
    Перейменовує теги назв і опису у формат Epicenter (lang-атрибут).
    Description-тег додатково загортається у CDATA-секцію.

    Prom UK-only фід віддає тільки ці два теги (вже українською мовою):
        <name>TEXT</name>               → <name lang="ua">TEXT</name>
        <description>...</description>  → <description lang="ua"><![CDATA[...]]></description>

    Безпечно для CDATA-вмісту (ламбда замість рядка заміни уникає проблем з спецсимволами).
    Викликати ПІСЛЯ inject_epicenter_attrs — вміст description вже оновлено.
    """
    renamed_counts: dict[str, int] = {}
    for prom_tag, epic_tag, lang in _TAG_RENAMES:
        is_description = prom_tag in _DESCRIPTION_TAGS
        # (?:\s[^>]*)? — матчить теги як з атрибутами (<description lang="ua">),
        # так і без (<description>). Без цього Prom-фіди, що вже містять lang="ua",
        # не проходять підміну → CDATA не додається.
        pattern = rf'<{prom_tag}(?:\s[^>]*)?>(.*?)</{prom_tag}>'
        if is_description:
            xml, n = re.subn(
                pattern,
                lambda m, t=epic_tag, l=lang: (
                    f'<{t} lang="{l}">{_wrap_cdata(m.group(1))}</{t}>'
                ),
                xml,
                flags=re.DOTALL,
            )
        else:
            xml, n = re.subn(
                pattern,
                lambda m, t=epic_tag, l=lang: f'<{t} lang="{l}">{m.group(1)}</{t}>',
                xml,
                flags=re.DOTALL,
            )
        renamed_counts[prom_tag] = n

    total_renamed = sum(renamed_counts.values())
    if total_renamed:
        detail = ", ".join(f"<{t}>×{c}" for t, c in renamed_counts.items() if c)
        print(f"🏷️  Перейменовано теги → Epicenter формат (lang=..., description у CDATA): {detail}")
    else:
        _logger.warning(
            "normalize_name_description_tags: жодного тегу не перейменовано — "
            "перевірте структуру фіду (очікуються теги: %s)",
            ", ".join(t for t, _, _ in _TAG_RENAMES),
        )
    return xml


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main() -> None:
    xml = fetch_xml(FEED_URL)
    print(f"📄 Отримано {len(xml):,} символів")

    # Одразу після завантаження — витягуємо тільки <offers> з Prom-структури.
    # Prom завжди загортає <offers> у <shop> разом з <name>, <company>, <currencies>, <categories>.
    # Epicenter потребує лише <yml_catalog><offers>...</offers></yml_catalog>.
    # Робимо це першим кроком щоб всі наступні трансформації працювали
    # вже на скороченому XML без зайвих блоків.
    xml = strip_prom_shop_block(xml)

    currency_rates = parse_currency_rates(xml)
    updated_xml = filter_unavailable_offers(xml)

    wholesale_index = load_wholesale_price_index(ROOT)

    updated_xml = apply_market_prices(MARKET, updated_xml, wholesale_index, currency_rates)
    updated_xml = fill_missing_vendor(updated_xml)
    updated_xml = filter_stop_brand_offers(updated_xml)
    updated_xml = add_name_ua(updated_xml)
    updated_xml, _used_entries = inject_epicenter_attrs(updated_xml)
    flush_fallback_warnings()  # зведений лог fallback-промахів категорій
    updated_xml = normalize_name_description_tags(updated_xml)   # після inject: description вже оновлено
    updated_xml = strip_prom_offer_fields(updated_xml)           # після fill_missing_vendor
    updated_xml = sanitize_russian_chars(updated_xml)             # ы→и, ъ→' у всьому фіді
    updated_xml = strip_html_classes(updated_xml)                  # видаляємо class="..." з HTML
    updated_xml = strip_external_links(updated_xml)               # видаляємо "Детальніше:" та bare URLs з описів

    # Гарантуємо коректну XML-декларацію незалежно від того,
    # чи Prom-фід її надсилає і в якому форматі.
    # <!DOCTYPE ...> видаляємо: зовнішній DTD-посилання призводить до того що
    # GitHub raw віддає файл як octet-stream (скачування) замість application/xml.
    updated_xml = re.sub(r'^\s*<\?xml[^?]*\?>\s*', '', updated_xml)
    updated_xml = re.sub(r'^\s*<!DOCTYPE[^>]*>\s*', '', updated_xml, flags=re.DOTALL)
    updated_xml = '<?xml version="1.0" encoding="UTF-8"?>\n' + updated_xml

    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    OUTPUT_PATH.write_text(updated_xml, encoding="utf-8")
    print(f"✅ Збережено: {OUTPUT_PATH}")


if __name__ == "__main__":
    main()