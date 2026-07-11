"""
Генерує фід для Касти:
  1. Завантажує XML фід з сайту
  2. Читає data/markets/kasta_coefficients.csv (цінові правила Kasta)
  3. Визначає базову ціну: Оптова_ціна з *_old.csv або fallback на ціну з XML
  4. Базова ціна × правило Kasta для категорії/цінового діапазону = нова ціна
  5. Вставляє блок «Особливості»/«Особенности» з PROM-параметрів у
     <description_ua>/<description> (services/kasta_params_to_description_service.py)
  6. Зберігає результат в data/markets/kasta_feed.xml

Запуск локально:
    python scripts/generate_kasta_feed.py

Запуск у GitHub Actions: Stage 4 → needs: process-and-publish
ВАЖЛИВО: у GitHub Actions job повинен відновити *_old.csv з data-latest
(see pipeline.yml step "Restore *_old.csv from data-latest").
"""

from pathlib import Path

from constants_feed_url import FEED_URL_PROM as FEED_URL
from generate_utils_feed import (
    add_name_ua,
    fetch_xml,
    fill_missing_vendor,
    filter_unavailable_offers,
    load_article_price_index,
    parse_currency_rates,
    replace_vendor_aliases,
)
from services.kasta_params_to_description_service import inject_params_into_descriptions
from services.market_pricing import apply_market_prices

# ---------------------------------------------------------------------------
# Market-specific config
# ---------------------------------------------------------------------------

MARKET = "kasta"

ROOT = Path(__file__).parents[1]
OUTPUT_PATH = ROOT / "data" / "markets" / "kasta_feed.xml"


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
    updated_xml = replace_vendor_aliases(updated_xml)
    updated_xml = fill_missing_vendor(updated_xml)
    updated_xml = add_name_ua(updated_xml)
    updated_xml = inject_params_into_descriptions(updated_xml)

    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    OUTPUT_PATH.write_text(updated_xml, encoding="utf-8")
    print(f"✅ Збережено: {OUTPUT_PATH}")


if __name__ == "__main__":
    main()
