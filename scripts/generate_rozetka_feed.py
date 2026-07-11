"""
Генерує фід для Розетки:
  1. Завантажує XML фід з сайту
  2. Читає data/markets/rozetka_coefficients.csv (через services/market_pricing.py)
  3. Визначає базову ціну: Оптова_ціна з *_old.csv або fallback на ціну з XML
  4. Базова ціна × коефіцієнт категорії = нова ціна
  5. Замінює prom categoryId на rozetka_category_id (через services/rozetka_category_service.py)
  6. Замінює Prom <categories> блок на Rozetka-категорії (тільки реально використані)
  7. Вставляє блок «Особливості»/«Особенности» з PROM-параметрів у
     <description_ua>/<description> (services/rozetka_params_to_description_service.py)
  8. Перейменовує param «Країна-виробник» → «Країна-виробник товару», знімає unit=""
  9. Зберігає результат в data/markets/rozetka_feed.xml

ВАЖЛИВО: Розетка забирає Prom-фід практично в оригінальному вигляді.
  - Теги XML НЕ перейменовуються і НЕ перетворюються (немає normalize_name_description_tags).
  - add_name_ua НЕ викликається — Розетка використовує тег <name> напряму.
  - <currencies> НЕ видаляється — Розетка потребує курси валют для конвертації цін.
  - <company> та <url> (shop + offer рівні) — видаляються як зайві для Розетки.

Запуск локально:
    python scripts/generate_rozetka_feed.py

Запуск у GitHub Actions: Stage 6 → needs: process-and-publish
ВАЖЛИВО: у GitHub Actions job повинен відновити *_old.csv з data-latest
(see pipeline.yml step "Restore *_old.csv from data-latest").
"""

import logging
import re
from pathlib import Path
from typing import Final

from constants_feed_url import FEED_URL_PROM as FEED_URL
from generate_utils_feed import (
    fetch_xml,
    fill_missing_vendor,
    filter_unavailable_offers,
    load_article_price_index,
    normalize_vendor_language,
    parse_currency_rates,
)
from services.market_pricing import apply_market_prices
from services.rozetka_stop_brand_service import filter_stop_brand_offers
from services.rozetka_unique_name_service import deduplicate_offer_names
from services.rozetka_category_service import (
    CategoryEntry,
    build_categories_xml,
    flush_fallback_warnings,
    resolve_category,
)
from services.rozetka_category_leaf_service import validate_used_categories
from services.rozetka_params_to_description_service import inject_params_into_descriptions
from services.rozetka_text_sanitizer_service import sanitize_rozetka_text

_logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Market-specific config
# ---------------------------------------------------------------------------

MARKET = "rozetka"

ROOT = Path(__file__).parents[1]
OUTPUT_PATH = ROOT / "data" / "markets" / "rozetka_feed.xml"
LOG_PATH    = ROOT / "rozetka_default_id.log"   # пишеться services/market_pricing.py

SHOP_NAME: Final[str] = "DomSys"

# ---------------------------------------------------------------------------
# Regex — одноразова компіляція
# ---------------------------------------------------------------------------

_PROM_CATEGORIES_RE: Final[re.Pattern[str]] = re.compile(
    r'<categories>.*?</categories>',
    re.DOTALL,
)

_CATEGORY_ID_RE: Final[re.Pattern[str]] = re.compile(
    r'<categoryId>(\d+)</categoryId>'
)

# Офер-рівневий парсинг — потрібен для param-based роутингу категорій
# (resolve_category читає prom_params, щоб розрізнити напр. «IP-камери» /
# «HDCVI відеокамери» всередині однієї prom-категорії «Камери відеоспостереження»).
_OFFER_RE: Final[re.Pattern[str]] = re.compile(
    r'<offer\s+id="(\d+)"([^>]*)>(.*?)</offer>',
    re.DOTALL,
)
_PROM_PARAM_RE: Final[re.Pattern[str]] = re.compile(
    r'<param\b[^>]*\bname="([^"]+)"[^>]*>(.*?)</param>',
    re.DOTALL,
)
_CDATA_RE: Final[re.Pattern[str]] = re.compile(r'<!\[CDATA\[(.*?)\]\]>', re.DOTALL)

# Prom віддає: <param name="Країна-виробник" unit="">Китай</param>
# Розетка очікує: <param name="Країна-виробник товару">Китай</param>
# [^>]* — поглинає будь-які атрибути між name="..." і ">", зокрема unit="".
# (.*?) з re.DOTALL — безпечно для CDATA-вмісту.
_COUNTRY_PARAM_RE: Final[re.Pattern[str]] = re.compile(
    r'<param\s+name="Країна-виробник"[^>]*>(.*?)</param>',
    re.DOTALL,
)
_COUNTRY_PARAM_REPLACEMENT: Final[str] = '<param name="Країна-виробник товару">\\1</param>'

# Розетка: видаляємо <company> та <url> (shop-рівень + кожен offer),
# але НЕ <currencies> — Розетка конвертує ціни через курси.
# <url> в Prom-фіді містить лише ASCII/символи без '<', тому [^<]* безпечний.
_ROZETKA_FIELDS_TO_STRIP: Final[tuple[str, ...]] = (
    "company",
    "url",
)


# ---------------------------------------------------------------------------
# XML transformation helpers (Rozetka-specific)
# ---------------------------------------------------------------------------

def _strip_cdata(value: str) -> str:
    """Витягує текст з CDATA-обгортки; якщо її немає — повертає рядок як є."""
    m = _CDATA_RE.match(value.strip())
    return m.group(1).strip() if m else value.strip()


def replace_category_ids(xml: str) -> tuple[str, list[CategoryEntry]]:
    """
    Замінює prom categoryId на rozetka_category_id у кожному <offer>.
    Повертає оновлений XML та список унікальних використаних CategoryEntry
    (відсортовано за category_id — для детермінованого <categories> блоку).

    Обробляється пооферно (а не одним re.sub по всьому XML), тому що resolve_category
    потребує prom_params конкретного офера для param-based роутингу
    (напр. «Kamери відеоспостереження» → IP-камери / HDCVI відеокамери
    залежно від його параметра «Тип пристрою»).

    Маппінг береться з rozetka_mappings.xlsx через services.rozetka_category_service
    (lru_cache — читається один раз). Оффери без маппінгу залишаються з
    оригінальним prom categoryId: фід не ламається, але відсутні ID логуються як warning.
    Param-fallback промахи (всі правила param-based, але жодне не збіглось) логуються
    окремо через flush_fallback_warnings() після виклику цієї функції (в main()).
    """
    mapped = 0
    skipped_ids: set[int] = set()
    # dict keyed by rozetka category_id — дедублікація без втрати порядку вставки
    used: dict[int, CategoryEntry] = {}

    def _on_offer(m: re.Match) -> str:
        nonlocal mapped
        offer_id, tail_attrs, body = m.group(1), m.group(2), m.group(3)

        cat_match = _CATEGORY_ID_RE.search(body)
        if not cat_match:
            return m.group(0)   # немає categoryId — залишаємо офер без змін
        prom_id = int(cat_match.group(1))

        # Prom може віддавати кілька <param> тегів з однаковим name (multiselect) —
        # дублікати об'єднуються через ", " (так само, як в epicenter_category_service).
        prom_params: dict[str, str] = {}
        for pm in _PROM_PARAM_RE.finditer(body):
            name = pm.group(1).strip()
            value = _strip_cdata(pm.group(2))
            if name in prom_params:
                prom_params[name] = f"{prom_params[name]}, {value}"
            else:
                prom_params[name] = value

        entry = resolve_category(prom_id, prom_params, offer_id)
        if entry is None:
            skipped_ids.add(prom_id)
            return m.group(0)   # fallback — залишаємо prom categoryId без змін

        used.setdefault(entry["category_id"], entry)
        mapped += 1
        new_body = _CATEGORY_ID_RE.sub(
            f'<categoryId>{entry["category_id"]}</categoryId>', body, count=1
        )
        return f'<offer id="{offer_id}"{tail_attrs}>{new_body}</offer>'

    result = _OFFER_RE.sub(_on_offer, xml)

    print(f"🗂️  categoryId → Rozetka: {mapped} замінено | унікальних категорій: {len(used)}", end="")
    if skipped_ids:
        ids_str = ", ".join(str(i) for i in sorted(skipped_ids))
        _logger.warning(
            "Prom categoryId без маппінгу (%d): %s", len(skipped_ids), ids_str
        )
        print(f" | без маппінгу: {len(skipped_ids)}", end="")
    print()

    return result, list(used.values())


def replace_prom_categories(xml: str, entries: list[CategoryEntry]) -> str:
    """
    Замінює Prom <categories>...</categories> блок на Rozetka-категорії.

    Вхід entries — лише реально використані у фіді категорії,
    зібрані під час replace_category_ids.

    До:   <categories><category id="513">Подовжувачі</category>...</categories>
    Після: <categories>
               <category id="84863">Мережеві фільтри...</category>
           </categories>
    """
    rozetka_block = build_categories_xml(entries)

    # Використовуємо lambda щоб уникнути інтерпретації спецсимволів (\1, \g) у replacement.
    cleaned, n = _PROM_CATEGORIES_RE.subn(lambda _: rozetka_block, xml)
    if n:
        print(f"🗂️  <categories> замінено на {len(entries)} Rozetka-категорій")
    else:
        _logger.warning("<categories> блок не знайдено у XML — перевірте структуру фіду")
    return cleaned


def rename_country_param(xml: str) -> str:
    """
    Перейменовує Prom-param країни у формат Розетки та прибирає зайвий атрибут unit.

    Prom:    <param name="Країна-виробник" unit="">Китай</param>
    Розетка: <param name="Країна-виробник товару">Китай</param>

    Зміни:
      - name="Країна-виробник"  →  name="Країна-виробник товару"
      - усі інші атрибути тегу (unit="", тощо) — видаляються
      - вміст тегу залишається без змін
    """
    result, n = _COUNTRY_PARAM_RE.subn(_COUNTRY_PARAM_REPLACEMENT, xml)
    if n:
        print(f'🌍 rename_country_param: «Країна-виробник» → «Країна-виробник товару» ({n}×)')
    else:
        _logger.warning(
            'rename_country_param: param «Країна-виробник» не знайдено у фіді'
        )
    return result


def strip_prom_shop_fields(xml: str) -> str:
    """
    Видаляє Prom-специфічні shop/offer теги, не потрібні Розетці:
        <company>  — назва компанії (shop-рівень, ×1)
        <url>      — посилання на магазин (×1) та на кожен товар (×N offers)

    <currencies> свідомо НЕ видаляється: Розетка використовує блок <currencies>
    для конвертації цін у різних валютах (USD → UAH тощо).
    """
    removed: dict[str, int] = {}
    for tag in _ROZETKA_FIELDS_TO_STRIP:
        xml, n = re.subn(rf'[ \t]*<{tag}>[^<]*</{tag}>[ \t]*\n?', '', xml)
        if n:
            removed[tag] = n
    if removed:
        summary = ', '.join(f'<{t}>×{c}' for t, c in removed.items())
        print(f"🗑️  Видалено Prom shop-тегів: {summary}")
    return xml


def set_shop_name(xml: str) -> str:
    """
    Замінює перший <name>…</name> (назва магазину) на SHOP_NAME.

    В основному Prom-фіді назви товарів зберігаються у <name>, тому
    перший тег <name> завжди є назвою магазину на рівні <shop>.
    Викликати ПІСЛЯ strip_prom_shop_fields (щоб структура XML була стабільна).
    """
    xml, n = re.subn(
        r'<name>.*?</name>',
        f'<name>{SHOP_NAME}</name>',
        xml,
        count=1,
        flags=re.DOTALL,
    )
    if n:
        print(f"🏷️  Назва магазину → {SHOP_NAME!r}")
    else:
        _logger.warning("<name> магазину не знайдено — перевірте структуру фіду")
    return xml


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main() -> None:
    xml = fetch_xml(FEED_URL)
    print(f"📄 Отримано {len(xml):,} символів")

    currency_rates = parse_currency_rates(xml)
    updated_xml = filter_unavailable_offers(xml)

    price_index = load_article_price_index(ROOT)

    updated_xml = apply_market_prices(MARKET, updated_xml, price_index, currency_rates)
    updated_xml = fill_missing_vendor(updated_xml)
    updated_xml = normalize_vendor_language(updated_xml)  # "Без бренда" → "Без бренду"
    updated_xml = filter_stop_brand_offers(updated_xml)
    updated_xml = deduplicate_offer_names(updated_xml)  # <name>/<name_ua> мають бути унікальними для Rozetka
    updated_xml = sanitize_rozetka_text(updated_xml)  # sale-мітки в назвах, емодзі та «причина уцінки» в описах
    updated_xml = inject_params_into_descriptions(updated_xml)  # блок «Особливості»/«Особенности» з PROM-параметрів

    # --- Розетка-специфічне очищення та трансформація XML ---
    # replace_category_ids повертає (xml, used_entries) — entries потрібні для <categories> блоку
    updated_xml, used_entries = replace_category_ids(updated_xml)
    flush_fallback_warnings()  # зведений warning по param-fallback промахам (див. resolve_category)
    validate_used_categories(entry["category_id"] for entry in used_entries)  # логує non-leaf/unknown id, не блокує генерацію
    updated_xml = replace_prom_categories(updated_xml, used_entries)  # Prom → Rozetka <categories>
    updated_xml = rename_country_param(updated_xml)                    # «Країна-виробник» → «...товару»
    updated_xml = strip_prom_shop_fields(updated_xml)                  # видаляємо <company>, <url>
    updated_xml = set_shop_name(updated_xml)                           # після strip: <name> доступний

    # Примітка: add_name_ua не викликається — Розетка використовує тег <name> напряму.
    # Примітка: normalize_name_description_tags не викликається — теги не перейменовуються.

    # Гарантуємо коректну XML-декларацію з великої літери (UTF-8).
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
