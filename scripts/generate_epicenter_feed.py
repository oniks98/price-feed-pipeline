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

from decimal import Decimal
from pathlib import Path

from feed_common import (
    add_name_ua,
    apply_prices,
    build_offer_data_map,
    fetch_xml,
    fill_missing_vendor,
    filter_unavailable_offers,
    load_coefficients,
    load_wholesale_price_index,
    parse_currency_rates,
    transform_prom_image_urls,
)

# ---------------------------------------------------------------------------
# Market-specific config
# ---------------------------------------------------------------------------

FEED_URL = (
    "https://oniks.org.ua/rozetka_feed.xml?rozetka_hash_tag=33ec12f81c283cc0524764696220b10c&product_ids=&label_ids=&languages=uk%2Cru&group_ids=2221523%2C2222437%2C2222561%2C2234751%2C4320349%2C4321341%2C4325742%2C4325743%2C4328775%2C4331550%2C4339717%2C4551903%2C8950007%2C8950011%2C10015559%2C16703618%2C17012111%2C17012456%2C22818554%2C23295147%2C45720479%2C72575633%2C83889367%2C90718784%2C90906797%2C90997501%2C90997677%2C90997694%2C91056839%2C127351905%2C127351912%2C127351948%2C127351950%2C127351973%2C127628160%2C127628166%2C127628170%2C127628173%2C127628176%2C139094517%2C139094704%2C139094708%2C144038788%2C144038790%2C144038804%2C151114178%2C152084397%2C152084437%2C152084460%2C152084594%2C152084669%2C152084678%2C152084703%2C152086699%2C152088176%2C152088624%2C152090354%2C152090439%2C152090654%2C152090742%2C152090999%2C152091016%2C152091894%2C152092523%2C152092600%2C152092613%2C152092625%2C152104228%2C152104243%2C152133169%2C152133408%2C152133464%2C152133483%2C152135823%2C152195979%2C152196244%2C152197115%2C152197317%2C152197474%2C152206635%2C152207998%2C152208073%2C152208101%2C152208132%2C152208469%2C152208563%2C152208591%2C152208632%2C152481182%2C152481185%2C152481192%2C152481294%2C152483771&nested_group_ids=4321341%2C4325742%2C4325743%2C4328775%2C4331550%2C4339717%2C4551903%2C8950007%2C8950011%2C16703618%2C17012111%2C17012456%2C22818554%2C23295147%2C45720479%2C72575633%2C83889367%2C90718784%2C90906797%2C90997501%2C90997677%2C90997694%2C91056839%2C127351912%2C127351948%2C127351950%2C127351973%2C127628160%2C127628166%2C127628170%2C127628173%2C127628176%2C139094704%2C139094708%2C144038788%2C144038790%2C144038804%2C151114178%2C152084397%2C152084437%2C152084460%2C152084594%2C152084669%2C152084678%2C152084703%2C152086699%2C152088176%2C152088624%2C152090354%2C152090439%2C152090654%2C152090742%2C152090999%2C152091016%2C152091894%2C152092523%2C152092600%2C152092613%2C152092625%2C152104243%2C152133169%2C152133408%2C152133464%2C152133483%2C152135823%2C152195979%2C152196244%2C152197115%2C152197317%2C152197474%2C152206635%2C152207998%2C152208073%2C152208101%2C152208132%2C152208469%2C152208563%2C152208591%2C152208632%2C152481182%2C152481185%2C152481192%2C152481294%2C152483771"
)

DEFAULT_COEFFICIENT = Decimal("1.2")
COEF_COLUMN = "coef_epicenter"

ROOT = Path(__file__).parents[1]
COEFFICIENTS_PATH = ROOT / "data" / "markets" / "markets_coefficients.csv"
OUTPUT_PATH = ROOT / "data" / "markets" / "epicenter_feed.xml"


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main() -> None:
    xml = fetch_xml(FEED_URL)
    print(f"📄 Отримано {len(xml):,} символів")

    currency_rates = parse_currency_rates(xml)
    updated_xml = filter_unavailable_offers(xml)

    coefficients = load_coefficients(COEFFICIENTS_PATH, COEF_COLUMN, DEFAULT_COEFFICIENT)
    print(f"📂 CSV шлях: {COEFFICIENTS_PATH} | існує: {COEFFICIENTS_PATH.exists()}")

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
