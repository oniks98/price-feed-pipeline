"""
Генерує фід для Розетки:
  1. Завантажує XML фід з сайту
  2. Читає data/markets/markets_coefficients.csv (колонка coef_rozetka)
  3. Визначає базову ціну: Оптова_ціна з *_old.csv або fallback на ціну з XML
  4. Базова ціна × коефіцієнт категорії = нова ціна
  5. Зберігає результат in data/markets/rozetka_feed.xml

ВАЖЛИВО: Розетка забирає Prom-фід практично в оригінальному вигляді.
  - Теги XML НЕ перейменовуються і НЕ перетворюються.
  - Єдина зміна — ціноутворення та нормалізація зображень.
  - add_name_ua НЕ викликається — Розетка використовує тег <n> напряму.

Запуск локально:
    python scripts/generate_rozetka_feed.py

Запуск у GitHub Actions: Stage 6 → needs: process-and-publish
ВАЖЛИВО: у GitHub Actions job повинен відновити *_old.csv з data-latest
(see pipeline.yml step "Restore *_old.csv from data-latest").
"""

from pathlib import Path

from constants_feed_url import FEED_URL_PROM as FEED_URL
from generate_utils_feed import (
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

MARKET = "rozetka"

ROOT = Path(__file__).parents[1]
OUTPUT_PATH = ROOT / "data" / "markets" / "rozetka_feed.xml"

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
    # Примітка: add_name_ua не викликається — Розетка використовує тег <n> напряму

    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    OUTPUT_PATH.write_text(updated_xml, encoding="utf-8")
    print(f"✅ Збережено: {OUTPUT_PATH}")


if __name__ == "__main__":
    main()