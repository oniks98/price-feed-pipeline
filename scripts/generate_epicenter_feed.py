"""
Генерує фід для Епіцентру:
  1. Завантажує XML фід з сайту
  2. Читає data/markets/markets_coefficients.csv (колонка coef_epicenter)
  3. Визначає базову ціну: Оптова_ціна з *_old.csv або fallback на ціну з XML
  4. Базова ціна × коефіцієнт категорії = нова ціна
  5. Зберігає результат в data/markets/epicenter_feed.xml

Запуск локально:
    python scripts/generate_epicenter_feed.py

Запуск у GitHub Actions: Stage 5 → needs: process-and-publish
ВАЖЛИВО: у GitHub Actions job повинен відновити *_old.csv з data-latest
(see pipeline.yml step "Restore *_old.csv from data-latest").
"""

from pathlib import Path

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
from services.market_coefficients import get_coefficients, get_default_coefficient

# ---------------------------------------------------------------------------
# Market-specific config
# ---------------------------------------------------------------------------

MARKET = "epicenter"

ROOT = Path(__file__).parents[1]
OUTPUT_PATH = ROOT / "data" / "markets" / "epicenter_feed.xml"

DEFAULT_COEFFICIENT = get_default_coefficient(MARKET)


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

    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    OUTPUT_PATH.write_text(updated_xml, encoding="utf-8")
    print(f"✅ Збережено: {OUTPUT_PATH}")


if __name__ == "__main__":
    main()