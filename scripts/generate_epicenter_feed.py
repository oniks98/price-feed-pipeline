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

import re
from pathlib import Path
from typing import Final

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
    AttrMeta,
    AttrOption,
    get_defaults,
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
# System attributes config
# ---------------------------------------------------------------------------
# Ці paramcode фіксовані Epicenter для всіх категорій.
# Значення з Prom-параму підставляється напряму через CDATA.
# Одиниці: width/height/length — мм, weight — г.
#
# country_of_origin та brand — НЕ тут.
# Вони є select-атрибутами і читаються з «Опції атрибутів» через option_map.
# Для коректної роботи country_of_origin повинен бути у xlsx:
#   «Сети атрибутів»  → prom_param_name = "Країна-виробник", attr_type = select
#   «Опції атрибутів» → рядки: prom_option_name = "Китай" → option_code = "chn" і т.д.

# prom_param_name → (epicenter_paramcode, epicenter_name)
_SYSTEM_NUMERIC: Final[dict[str, tuple[str, str]]] = {
    "Ширина":    ("width",  "Ширина"),
    "Висота":    ("height", "Висота"),
    "Довжина":   ("length", "Глибина"),  # Prom="Довжина" → Epicenter="Глибина"
    "Глибина":   ("length", "Глибина"),
    "Вага":      ("weight", "Вага"),
    "Кратність": ("ratio",  "Мінімальна кратність товару"),
}

# measure: завжди шт., фіксований для всіх категорій
_SYSTEM_MEASURE: Final[str] = (
    '<param paramcode="measure" name="Міра виміру" valuecode="measure_pcs">шт.</param>'
)


# ---------------------------------------------------------------------------
# Epicenter XML helpers
# ---------------------------------------------------------------------------

def _strip_cdata(value: str) -> str:
    """Витягує текст з CDATA-обгортки; якщо її немає — повертає рядок як є."""
    m = _CDATA_RE.match(value.strip())
    return m.group(1).strip() if m else value.strip()


def _render_select_param(option: AttrOption) -> str:
    """
    <param paramcode="6067" name="Кут огляду" valuecode="opt_120">120°</param>
    """
    return (
        f'<param paramcode="{option.attr_code}" '
        f'name="{option.attr_name}" '
        f'valuecode="{option.option_code}">'
        f'{option.option_name}</param>'
    )


def _render_numeric_param(meta: AttrMeta, value: str) -> str:
    """
    <param paramcode="width" name="Ширина"><![CDATA[100]]></param>
    """
    return (
        f'<param paramcode="{meta.attr_code}" '
        f'name="{meta.attr_name}">'
        f'<![CDATA[{value}]]></param>'
    )


def _render_system_numeric(paramcode: str, name: str, value: str) -> str:
    """
    Системні CDATA-атрибути (фіксований paramcode, не з xlsx).
    <param paramcode="width" name="Ширина"><![CDATA[100]]></param>
    """
    return f'<param paramcode="{paramcode}" name="{name}"><![CDATA[{value}]]></param>'


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
      3. Prom params → Epicenter <param paramcode="..."> через три індекси:
            _SYSTEM_NUMERIC — фіксовані CDATA-атрибути (габарити, вага, кратність)
            option_map      — select/multiselect (з valuecode), включно з country та brand
            numeric_map     — float/int/text/string (CDATA значення) з xlsx
            defaults        — обов'язкові атрибути без маппінгу (fallback option)
      4. Оффери без маппінгу категорії залишаються без змін (логується).

    Повертає оновлений XML.
    """
    option_map  = get_option_map()
    defaults    = get_defaults()
    numeric_map = get_numeric_map()

    mapped_count   = 0
    skipped_no_cat = 0
    total_params   = 0

    def _on_offer(m: re.Match) -> str:
        nonlocal mapped_count, skipped_no_cat, total_params

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
            return m.group(0)

        cat_code = category['code']
        cat_name = category['name']

        # --- 2. Парсимо prom params до видалення ---
        prom_params: dict[str, str] = {
            pm.group(1).strip(): _strip_cdata(pm.group(2))
            for pm in _PROM_PARAM_RE.finditer(body)
        }

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

        # ── 5a. Системні CDATA-атрибути (фіксований paramcode, не з xlsx) ──
        for prom_name, (sys_code, sys_name) in _SYSTEM_NUMERIC.items():
            value = prom_params.get(prom_name, "")
            if value and sys_code not in mapped_attr_codes:
                params.append(_render_system_numeric(sys_code, sys_name, value))
                mapped_attr_codes.add(sys_code)

        # measure: завжди шт.
        if "measure" not in mapped_attr_codes:
            params.append(_SYSTEM_MEASURE)
            mapped_attr_codes.add("measure")

        # ── 5b. Категорійні атрибути з xlsx ──────────────────────────────────
        # Сюди потрапляють: select/multiselect (option_map) та float/int/text/string (numeric_map).
        # Включно з country_of_origin ("Країна-виробник") та brand ("Бренд") — через option_map.

        for prom_name, prom_value in prom_params.items():
            if not prom_value:
                continue

            # Системні prom_name вже оброблені вище — пропускаємо
            if prom_name in _SYSTEM_NUMERIC:
                continue

            # select / multiselect — маппінг через option_map
            # multiselect: Prom передає кілька значень через ", " (напр. "тварини, коти")
            # кожне значення маппиться окремо → окремий <param> тег
            param_opts = option_map.get(prom_name, {})
            if param_opts:
                for single_value in (v.strip() for v in prom_value.split(",")):
                    option = param_opts.get(single_value)
                    if option and option.attr_code not in mapped_attr_codes:
                        params.append(_render_select_param(option))
                        mapped_attr_codes.add(option.attr_code)
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
    )
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
    updated_xml = inject_epicenter_attrs(updated_xml)

    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    OUTPUT_PATH.write_text(updated_xml, encoding="utf-8")
    print(f"✅ Збережено: {OUTPUT_PATH}")


if __name__ == "__main__":
    main()
