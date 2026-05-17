"""
Генерує фід для Епіцентру:
  1. Завантажує XML фід з сайту
  2. Читає data/markets/markets_coefficients.csv (колонка coef_epicenter)
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
from collections.abc import Callable
from pathlib import Path
from dataclasses import dataclass, field
from typing import Final, Literal

_logger = logging.getLogger(__name__)

from constants_feed_url import FEED_URL_PROM as FEED_URL
from generate_utils_feed import (
    add_name_ua,
    apply_prices,
    build_offer_data_map,
    fetch_xml,
    fill_missing_vendor,
    filter_unavailable_offers,
    load_wholesale_price_index,
    parse_currency_rates,
    transform_prom_image_urls,
)
from services.prom_params_to_description_service import inject_params_into_description
from services.epicenter_attr_service import (
    AttrDefaultsMap,
    AttrMeta,
    AttrOption,
    get_attr_defaults,
    get_defaults,
    get_float_defaults,
    get_numeric_map,
    get_option_map,
)
from services.epicenter_category_service import get_category
from services.market_coefficients import get_coefficients, get_default_coefficient

# ---------------------------------------------------------------------------
# Market-specific config
# ---------------------------------------------------------------------------

MARKET = "epicenter"

ROOT = Path(__file__).parents[1]
OUTPUT_PATH = ROOT / "data" / "markets" / "epicenter_feed.xml"

DEFAULT_COEFFICIENT = get_default_coefficient(MARKET)

# Regex для парсингу <param> тегів Прому в тілі офера
_PROM_PARAM_RE = re.compile(
    r'<param\b[^>]*\bname="([^"]+)"[^>]*>(.*?)</param>',
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


@dataclass(frozen=True)
class AttrDefaults:
    """
    Дефолтні значення для системних атрибутів.

    float / text / array → option_name_uk (текстове значення)
    select / multiselect → option_code   (код опції з xlsx)
                           option_name   (відображуваний текст всередині тегу, напр. "шт.")
    """

    option_name_uk: dict[str, str] = field(default_factory=dict)  # attr_code → значення
    option_code: dict[str, str] = field(default_factory=dict)      # attr_code → code
    option_name: dict[str, str] = field(default_factory=dict)      # attr_code → display text


def resolve_attr_value(
    cfg: AttrConfig,
    prom_params: dict[str, str],
    defaults: AttrDefaults,
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
        if cfg.attr_type in ("float", "text", "array"):
            raw_value = defaults.option_name_uk.get(cfg.attr_code)
        else:
            # select / multiselect — дефолт через option_code, не option_name_uk
            option_code = defaults.option_code.get(cfg.attr_code)
            if option_code is None:
                _logger.warning(
                    "attr drop | no default_option_code | attr_code=%s", cfg.attr_code
                )
                return None
            return {
                "paramcode": cfg.attr_code,
                "name": cfg.attr_name_uk,
                "valuecode": option_code,
                "text": defaults.option_name.get(cfg.attr_code, ""),
            }

    if raw_value is None:
        _logger.warning(
            "attr drop | no value and no default | attr_code=%s", cfg.attr_code
        )
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
# select → valuecode береться з default_option_code у xlsx (через AttrDefaults)
#
# country_of_origin та brand — НЕ тут:
#   вони є select-атрибутами категорійного рівня → обробляються через option_map (крок 5c).
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

def inject_epicenter_attrs(xml: str) -> str:
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
    option_map    = get_option_map()
    defaults      = get_defaults()
    numeric_map   = get_numeric_map()
    attr_defaults  = get_attr_defaults()
    float_defaults = get_float_defaults()

    mapped_count    = 0
    skipped_no_cat  = 0
    skipped_cat_ids: set[int] = set()
    total_params    = 0
    missing_measure = 0

    def _on_offer(m: re.Match) -> str:
        nonlocal mapped_count, skipped_no_cat, total_params, missing_measure

        offer_id   = m.group(1)
        tail_attrs = m.group(2)
        body       = m.group(3)

        # --- 1. Знаходимо prom categoryId ---
        cat_match = re.search(r'<categoryId>(\d+)</categoryId>', body)
        if not cat_match:
            return m.group(0)

        prom_cat_id = int(cat_match.group(1))
        category = get_category(prom_cat_id)

        if not category:
            skipped_no_cat += 1
            skipped_cat_ids.add(prom_cat_id)
            return m.group(0)

        cat_code = category['code']
        cat_name = category['name']

        # --- 2. Парсимо prom params до видалення ---
        # Prom може передавати кілька <param> тегів з однаковим name (multiselect).
        # dict comprehension залишав би тільки останній → втрата значень.
        # Замість цього: дублікати об'єднуються через ", " →
        # step 5c розіб'є по ", " і знайде кожне значення в option_map окремо.
        prom_params: dict[str, str] = {}
        for _pm in _PROM_PARAM_RE.finditer(body):
            _name  = _pm.group(1).strip()
            _value = _strip_cdata(_pm.group(2))
            if _name in prom_params:
                prom_params[_name] = f"{prom_params[_name]}, {_value}"
            else:
                prom_params[_name] = _value

        # --- 3. Замінюємо <categoryId> на <category> + <attribute_set> ---
        body = body.replace(
            cat_match.group(0),
            f'<category code="{cat_code}">{cat_name}</category>\n'
            f'<attribute_set code="{cat_code}">{cat_name}</attribute_set>',
        )

        # --- 4. Видаляємо ВСІ prom <param> теги ---
        body = re.sub(r'<param\b[^>]*>.*?</param>', '', body, flags=re.DOTALL)

        # --- 5. Будуємо epicenter params ---
        params: list[str] = []
        mapped_attr_codes: set[str] = set()

        # ── 5a+5b. Системні атрибути (_ATTRS) через resolve_attr_value ─────
        # AttrDefaults будується per-offer: option_code для select з xlsx.
        # Пріоритет measure: defaults[cat_code]["measure"] → attr_defaults["measure"].
        _measure_opt = (
            defaults.get(cat_code, {}).get("measure")
            or attr_defaults.get("measure")
        )
        _attr_defs = AttrDefaults(
            # float дефолти з xlsx (option_name_uk без option_code):
            # ratio=1, weight=500, height/length/width=150 тощо
            option_name_uk=float_defaults,
            option_code={"measure": _measure_opt.option_code} if _measure_opt else {},
            option_name={"measure": _measure_opt.option_name} if _measure_opt else {},
        )

        for cfg in _ATTRS:
            if cfg.attr_code in mapped_attr_codes:
                continue
            payload = resolve_attr_value(cfg, prom_params, _attr_defs)
            if payload is None:
                if cfg.attr_code == "measure":
                    missing_measure += 1
                continue
            params.append(_render_attr_payload(payload))
            mapped_attr_codes.add(cfg.attr_code)

        # ── 5c. Категорійні атрибути з xlsx ──────────────────────────────────
        # Сюди потрапляють: select/multiselect (option_map) та float/int/text/string (numeric_map).
        # Включно з country_of_origin ("Країна-виробник") та brand ("Бренд") — через option_map.

        for prom_name, prom_value in prom_params.items():
            if not prom_value:
                continue

            # Системні prom_name вже оброблені вище (_ATTRS) — пропускаємо
            if prom_name in _ATTRS_PROM_NAMES:
                continue

            # select / multiselect — маппінг через option_map
            # prom_value може містити кілька значень через ", ":
            #   - multiselect: кілька <param> тегів з однаковим name (об'єднані у крок 2)
            #   - або одне значення без коми
            # Кожне значення маппиться окремо → окремий <param> тег.
            # Якщо option_map має цей prom_name але конкретне значення не знайдено —
            # логуємо debug (дефолт буде застосований у кроці 6, якщо attr_code не mapped).
            param_opts = option_map.get(prom_name, {})
            if param_opts:
                # option_map має маппінг для цього prom_param_name.
                # Кожне значення (після split по ", ") шукається окремо.
                # Якщо значення не знайдено — логуємо debug (не тихо ігноруємо).
                # continue виконується завжди: цей prom_name — select/multiselect,
                # numeric_map lookup нижче не потрібен.
                for single_value in (v.strip() for v in prom_value.split(",")):
                    if not single_value:
                        continue
                    option = param_opts.get(single_value)
                    if option and option.attr_code not in mapped_attr_codes:
                        params.append(_render_select_param(option))
                        mapped_attr_codes.add(option.attr_code)
                    elif not option:
                        _logger.debug(
                            "offer %s | option_map miss | prom_param=%r value=%r "
                            "— значення відсутнє у маппінгу, дефолт буде застосовано",
                            offer_id, prom_name, single_value,
                        )
                continue

            # float / int / text / string — значення напряму
            meta = numeric_map.get(prom_name)
            if meta and meta.attr_code not in mapped_attr_codes:
                params.append(_render_numeric_param(meta, prom_value))
                mapped_attr_codes.add(meta.attr_code)

        # --- 6. Дефолти для атрибутів без маппінгу ---
        for attr_code, default in defaults.get(cat_code, {}).items():
            if attr_code not in mapped_attr_codes:
                params.append(_render_select_param(default))
                mapped_attr_codes.add(attr_code)

        # --- 7. Вставляємо блок «Параметри» в description_ua ---
        body = inject_params_into_description(body, prom_params)

        # --- 8. Вставляємо params у кінець body ---
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
    if skipped_cat_ids:
        ids_str = ', '.join(str(i) for i in sorted(skipped_cat_ids))
        _logger.warning('Prom categoryId без маппінгу (%d): %s', len(skipped_cat_ids), ids_str)
    return xml


# ---------------------------------------------------------------------------
# XML cleanup: strip Prom-specific blocks not needed in Epicenter feed
# ---------------------------------------------------------------------------


_PROM_CATEGORIES_RE: Final[re.Pattern[str]] = re.compile(
    r'<categories>.*?</categories>',
    re.DOTALL,
)


def strip_prom_categories(xml: str) -> str:
    """
    Видаляє блок <categories>...</categories> з Prom XML.

    Epicenter-фід не має власного дерева категорій у такому форматі:
    назва та код категорії підставляються безпосередньо у кожен <offer>
    через <category code="..."> та <attribute_set code="...">,
    тому батьківський блок <categories> є зайвим і відсутній у форматі Епіцентру.
    """
    cleaned, n = _PROM_CATEGORIES_RE.subn('', xml)
    if n:
        print(f"🗑️  Видалено Prom <categories> блок ({n} входжень)")
    else:
        _logger.warning("<categories> блок не знайдено у XML — перевірте структуру фіду")
    return cleaned


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
)

# Перейменування тегів: (prom_tag, epicenter_tag, lang)
# Prom: <n>, <name_ua>, <description>, <description_ua>
# Epicenter: <name lang="ru">, <name lang="ua">, <description lang="ru">, <description lang="ua">
_TAG_RENAMES: Final[tuple[tuple[str, str, str], ...]] = (
    ("n",              "name",        "ru"),
    ("name",           "name",        "ru"),
    ("name_ua",        "name",        "ua"),
    ("description",    "description", "ru"),
    ("description_ua", "description", "ua"),
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
        xml, n = re.subn(rf'<{tag}>[^<]*</{tag}>', '', xml)
        if n:
            removed[tag] = n
    if removed:
        summary = ', '.join(f'<{t}>\xd7{c}' for t, c in removed.items())
        print(f"🗑️  Видалено Prom-тегів з офферів: {summary}")
    return xml


_DESCRIPTION_TAGS: Final[frozenset[str]] = frozenset({"description", "description_ua"})


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
    Перейменовує теги назв і описів у формат Epicenter (lang-атрибут).
    Description-теги додатково загортаються у CDATA-секцію.

        <n>TEXT</n>                          → <name lang="ru">TEXT</name>
        <name_ua>TEXT</name_ua>              → <name lang="ua">TEXT</name>
        <description>...</description>       → <description lang="ru"><![CDATA[...]]></description>
        <description_ua>...</description_ua> → <description lang="ua"><![CDATA[...]]></description>

    Безпечно для CDATA-вмісту (ламбда замість рядка заміни уникає проблем з спецсимволами).
    Викликати ПІСЛЯ inject_epicenter_attrs — вміст description_ua вже оновлено.
    """
    for prom_tag, epic_tag, lang in _TAG_RENAMES:
        is_description = prom_tag in _DESCRIPTION_TAGS
        if is_description:
            xml = re.sub(
                rf'<{prom_tag}>(.*?)</{prom_tag}>',
                lambda m, t=epic_tag, l=lang: (
                    f'<{t} lang="{l}">{_wrap_cdata(m.group(1))}</{t}>'
                ),
                xml,
                flags=re.DOTALL,
            )
        else:
            xml = re.sub(
                rf'<{prom_tag}>(.*?)</{prom_tag}>',
                lambda m, t=epic_tag, l=lang: f'<{t} lang="{l}">{m.group(1)}</{t}>',
                xml,
                flags=re.DOTALL,
            )
    print("🏷️  Перейменовано теги name/description → Epicenter формат (lang=..., description у CDATA)")
    return xml


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main() -> None:
    xml = fetch_xml(FEED_URL)
    print(f"📄 Отримано {len(xml):,} символів")

    currency_rates = parse_currency_rates(xml)
    updated_xml = filter_unavailable_offers(xml)

    coefficients = get_coefficients(MARKET)

    wholesale_index = load_wholesale_price_index(ROOT)

    offer_map = build_offer_data_map(updated_xml, coefficients, wholesale_index, DEFAULT_COEFFICIENT)
    print(f"🏷️  Доступних офферів: {len(offer_map)}")

    updated_xml = apply_prices(updated_xml, offer_map, currency_rates)
    updated_xml = transform_prom_image_urls(updated_xml)
    updated_xml = fill_missing_vendor(updated_xml)
    updated_xml = add_name_ua(updated_xml)
    updated_xml = strip_prom_categories(updated_xml)
    updated_xml = inject_epicenter_attrs(updated_xml)
    updated_xml = normalize_name_description_tags(updated_xml)  # після inject: description_ua вже оновлена
    updated_xml = strip_prom_offer_fields(updated_xml)           # після fill_missing_vendor

    # Гарантуємо коректну XML-декларацію незалежно від того,
    # чи Prom-фід її надсилає і в якому форматі.
    updated_xml = re.sub(r'^\s*<\?xml[^?]*\?>\s*', '', updated_xml)
    updated_xml = '<?xml version="1.0" encoding="UTF-8"?>\n' + updated_xml

    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    OUTPUT_PATH.write_text(updated_xml, encoding="utf-8")
    print(f"✅ Збережено: {OUTPUT_PATH}")


if __name__ == "__main__":
    main()
