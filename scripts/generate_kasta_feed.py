"""
Генерує фід для Касти:
  1. Завантажує XML фід з сайту
  2. Читає data/markets/markets_coefficients.csv (колонка coef_kasta)
  3. Множить <price> кожного оферу на коефіцієнт його категорії
  4. Зберігає результат в data/markets/kasta_feed.xml

Запуск локально:
    python scripts/generate_kasta_feed.py

Запуск у GitHub Actions: Stage 4 → needs: process-and-publish
"""

import csv
import re
from decimal import Decimal, ROUND_HALF_UP
from pathlib import Path

import requests

FEED_URL = (
    "https://oniks.org.ua/rozetka_feed.xml?rozetka_hash_tag=33ec12f81c283cc0524764696220b10c&product_ids=&label_ids=&languages=uk%2Cru&group_ids=2221523%2C2222437%2C2222561%2C2234751%2C4320349%2C4321341%2C4325742%2C4325743%2C4328775%2C4331550%2C4339717%2C4551903%2C8950007%2C8950011%2C10015559%2C16703618%2C17012111%2C17012456%2C22818554%2C23295147%2C45720479%2C72575633%2C83889367%2C90718784%2C90906797%2C90997501%2C90997677%2C90997694%2C91056839%2C127351905%2C127351912%2C127351948%2C127351950%2C127351973%2C127628160%2C127628166%2C127628170%2C127628173%2C127628176%2C139094517%2C139094704%2C139094708%2C144038788%2C144038790%2C144038804%2C151114178%2C152084397%2C152084437%2C152084460%2C152084594%2C152084669%2C152084678%2C152084703%2C152086699%2C152088176%2C152088624%2C152090354%2C152090439%2C152090654%2C152090742%2C152090999%2C152091016%2C152091894%2C152092523%2C152092600%2C152092613%2C152092625%2C152104228%2C152104243%2C152133169%2C152133408%2C152133464%2C152133483%2C152135823%2C152195979%2C152196244%2C152197115%2C152197317%2C152197474%2C152206635%2C152207998%2C152208073%2C152208101%2C152208132%2C152208469%2C152208563%2C152208591%2C152208632%2C152481182%2C152481185%2C152481192%2C152481294%2C152483771&nested_group_ids=4321341%2C4325742%2C4325743%2C4328775%2C4331550%2C4339717%2C4551903%2C8950007%2C8950011%2C16703618%2C17012111%2C17012456%2C22818554%2C23295147%2C45720479%2C72575633%2C83889367%2C90718784%2C90906797%2C90997501%2C90997677%2C90997694%2C91056839%2C127351912%2C127351948%2C127351950%2C127351973%2C127628160%2C127628166%2C127628170%2C127628173%2C127628176%2C139094704%2C139094708%2C144038788%2C144038790%2C144038804%2C151114178%2C152084397%2C152084437%2C152084460%2C152084594%2C152084669%2C152084678%2C152084703%2C152086699%2C152088176%2C152088624%2C152090354%2C152090439%2C152090654%2C152090742%2C152090999%2C152091016%2C152091894%2C152092523%2C152092600%2C152092613%2C152092625%2C152104243%2C152133169%2C152133408%2C152133464%2C152133483%2C152135823%2C152195979%2C152196244%2C152197115%2C152197317%2C152197474%2C152206635%2C152207998%2C152208073%2C152208101%2C152208132%2C152208469%2C152208563%2C152208591%2C152208632%2C152481182%2C152481185%2C152481192%2C152481294%2C152483771"
)

DEFAULT_COEFFICIENT = Decimal("1.22")
COEF_COLUMN = "coef_kasta"

ROOT = Path(__file__).parents[1]
COEFFICIENTS_PATH = ROOT / "data" / "markets" / "markets_coefficients.csv"
OUTPUT_PATH = ROOT / "data" / "markets" / "kasta_feed.xml"


def fetch_xml(url: str) -> str:
    """Завантажує XML, декодує з правильним кодуванням
    і виправляє encoding declaration на utf-8."""
    print("⬇️  Завантаження фіду...")
    response = requests.get(url, timeout=120)
    response.raise_for_status()
    raw = response.content
    match = re.search(rb'encoding=["\']([^"\']+)["\']', raw[:200])
    encoding = match.group(1).decode("ascii") if match else (response.encoding or "utf-8")
    print(f"🔍 Кодування фіду: {encoding}")
    xml = raw.decode(encoding)

    xml = re.sub(
        r'(<\?xml[^?]*encoding=["\'])[^"\']+(["\'])',
        r'\g<1>utf-8\g<2>',
        xml,
        count=1,
    )
    return xml


def parse_currency_rates(xml: str) -> dict[str, Decimal]:
    rates: dict[str, Decimal] = {}
    for m in re.finditer(r'<currency\s+id="([^"]+)"\s+rate="([^"]+)"', xml):
        currency_id = m.group(1).strip().upper()
        if currency_id == "UAH":
            continue
        try:
            rates[currency_id] = Decimal(m.group(2).strip().replace(",", "."))
        except Exception:
            print(f"⚠️  Невірний курс для {currency_id}: '{m.group(2)}' — пропущено")
    if rates:
        print(f"💱 Курси валют: { {k: str(v) for k, v in rates.items()} }")
    else:
        print("ℹ️  Курси валют не знайдено — конвертація не потрібна")
    return rates


def load_coefficients() -> dict[str, Decimal]:
    """
    Читає markets_coefficients.csv і повертає {category_id: coefficient}
    з колонки coef_kasta.
    """
    if not COEFFICIENTS_PATH.exists():
        print(f"⚠️  {COEFFICIENTS_PATH} не знайдено — використовується DEFAULT {DEFAULT_COEFFICIENT}")
        return {}

    coefficients: dict[str, Decimal] = {}

    with COEFFICIENTS_PATH.open(encoding="utf-8-sig") as f:
        first_line = f.readline()
        delimiter = ";" if ";" in first_line else ","
        f.seek(0)

        reader = csv.DictReader(f, delimiter=delimiter)
        for row in reader:
            cat_id = row["category_id"].strip().strip("\ufeff")
            raw_coeff = row[COEF_COLUMN].strip().replace(",", ".")
            try:
                coefficients[cat_id] = Decimal(raw_coeff)
            except Exception:
                print(f"⚠️  Невірний коефіцієнт для {cat_id}: '{raw_coeff}' — пропущено")

    print(f"📋 Завантажено {len(coefficients)} категорій з коефіцієнтами ({COEF_COLUMN})")
    return coefficients


OfferData = dict[str, dict]


def build_offer_data_map(
    xml: str,
    coefficients: dict[str, Decimal],
) -> OfferData:
    offer_map: OfferData = {}

    for offer_match in re.finditer(r'<offer\s+id="(\d+)"[^>]*>(.*?)</offer>', xml, re.DOTALL):
        offer_id = offer_match.group(1)
        body = offer_match.group(2)

        cat_match = re.search(r"<categoryId>(\d+)</categoryId>", body)
        cat_id = cat_match.group(1) if cat_match else ""

        cur_match = re.search(r"<currencyId>([^<]+)</currencyId>", body)
        currency_id = cur_match.group(1).strip().upper() if cur_match else "UAH"

        offer_map[offer_id] = {
            "coefficient": coefficients.get(cat_id, DEFAULT_COEFFICIENT),
            "currency_id": currency_id,
        }

    return offer_map


def apply_prices(
    xml: str,
    offer_map: OfferData,
    currency_rates: dict[str, Decimal],
) -> str:
    converted_count = 0

    def on_offer(m: re.Match) -> str:
        nonlocal converted_count
        offer_id = m.group(1)
        tail_attrs = m.group(2)
        body = m.group(3)

        data = offer_map.get(offer_id)
        if data is None:
            return m.group(0)

        coeff = data["coefficient"]
        currency_id = data["currency_id"]

        def replace_price(pm: re.Match) -> str:
            nonlocal converted_count
            raw = pm.group(1).strip()
            try:
                price = Decimal(raw.replace(",", "."))

                if currency_id != "UAH":
                    rate = currency_rates.get(currency_id)
                    if rate is None:
                        print(
                            f"⚠️  Курс для {currency_id} не знайдено, "
                            f"оффер {offer_id} — ціна без конвертації"
                        )
                    else:
                        price = price * rate
                        converted_count += 1

                new_price = (price * coeff).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
                return f"<price>{new_price}</price>"
            except Exception:
                return pm.group(0)

        new_body = re.sub(r"<price>(.*?)</price>", replace_price, body)
        new_body = re.sub(
            r"<currencyId>[^<]+</currencyId>",
            "<currencyId>UAH</currencyId>",
            new_body,
        )
        return f'<offer id="{offer_id}"{tail_attrs}>{new_body}</offer>'

    xml = re.sub(
        r'<offer\s+id="(\d+)"([^>]*)>(.*?)</offer>',
        on_offer,
        xml,
        flags=re.DOTALL,
    )

    if converted_count:
        print(f"💱 Конвертовано цін з іноземної валюти в UAH: {converted_count}")
    else:
        print("ℹ️  Всі ціни вже в UAH — конвертація не потрібна")

    return xml


PROM_IMAGE_RE = re.compile(
    r'https://images\.prom\.ua/(?:[^"<\s]*/)?(\d+)_[^"<\s]+\.jpg'
)


def transform_prom_image_urls(xml: str) -> str:
    result, count = PROM_IMAGE_RE.subn(
        lambda m: f"https://images.prom.ua/{m.group(1)}_w640_h640_{m.group(1)}.jpg",
        xml,
    )
    print(f"🖼️  Нормалізовано URL зображень Prom.ua → w640_h640: {count}")
    return result


DEFAULT_VENDOR = "Anker"
DEFAULT_COUNTRY = "Китай"


def fill_missing_vendor(xml: str) -> str:
    """Підставляє <vendor> і <country_of_origin> якщо вони відсутні або порожні.

    Prom стирає виробника якого немає в своїй базі → фід приходить
    з порожнім <vendor>. Маркетплейси (Kasta, Rozetka, Epicenter)
    відхиляють такі товари при валідації.
    Fallback: Anker / Китай — нейтральний бренд, який є в базах усіх маркетплейсів.
    """
    filled = 0

    def on_offer(m: re.Match) -> str:
        nonlocal filled
        offer_id, tail_attrs, body = m.group(1), m.group(2), m.group(3)

        # Перевіряємо vendor
        vendor_match = re.search(r"<vendor>(.*?)</vendor>", body, re.DOTALL)
        if not vendor_match or not vendor_match.group(1).strip():
            if vendor_match:
                body = body.replace(vendor_match.group(0), f"<vendor>{DEFAULT_VENDOR}</vendor>", 1)
            else:
                insert_after = re.search(r"</price>", body)
                pos = insert_after.end() if insert_after else 0
                body = body[:pos] + f"\n<vendor>{DEFAULT_VENDOR}</vendor>" + body[pos:]
            filled += 1

        # Перевіряємо country_of_origin
        country_match = re.search(r"<country_of_origin>(.*?)</country_of_origin>", body, re.DOTALL)
        if not country_match or not country_match.group(1).strip():
            if country_match:
                body = body.replace(country_match.group(0), f"<country_of_origin>{DEFAULT_COUNTRY}</country_of_origin>", 1)
            else:
                vendor_end = re.search(r"</vendor>", body)
                pos = vendor_end.end() if vendor_end else len(body)
                body = body[:pos] + f"\n<country_of_origin>{DEFAULT_COUNTRY}</country_of_origin>" + body[pos:]

        return f'<offer id="{offer_id}"{tail_attrs}>{body}</offer>'

    xml = re.sub(
        r'<offer\s+id="(\d+)"([^>]*)>(.*?)</offer>',
        on_offer,
        xml,
        flags=re.DOTALL,
    )
    print(f"🏭  Підставлено виробника за замовчуванням ({DEFAULT_VENDOR} / {DEFAULT_COUNTRY}): {filled} товарів")
    return xml


def filter_unavailable_offers(xml: str) -> str:
    before = len(re.findall(r'<offer\s', xml))
    xml = re.sub(
        r'<offer\s[^>]*available="false"[^>]*>.*?</offer>',
        '',
        xml,
        flags=re.DOTALL,
    )
    after = len(re.findall(r'<offer\s', xml))
    print(f"🗑️  Відфільтровано товарів не в наявності: {before - after} (залишилось {after})")
    return xml


def add_name_ua(xml: str) -> str:
    def on_offer(m: re.Match) -> str:
        offer_id = m.group(1)
        tail_attrs = m.group(2)
        body = m.group(3)

        if "<name_ua>" in body:
            return m.group(0)

        name_match = re.search(r"<n>(.*?)</n>", body, re.DOTALL)
        if not name_match:
            return m.group(0)

        name_value = name_match.group(1)
        body = body.replace(
            name_match.group(0),
            f"{name_match.group(0)}\n<name_ua>{name_value}</name_ua>",
            1,
        )
        return f'<offer id="{offer_id}"{tail_attrs}>{body}</offer>'

    result, count = re.subn(
        r'<offer\s+id="(\d+)"([^>]*)>(.*?)</offer>',
        on_offer,
        xml,
        flags=re.DOTALL,
    )
    print(f"🏷️  Додано <name_ua> до {count} офферів")
    return result


def main() -> None:
    xml = fetch_xml(FEED_URL)
    print(f"📄 Отримано {len(xml):,} символів")

    currency_rates = parse_currency_rates(xml)

    coefficients = load_coefficients()
    offer_map = build_offer_data_map(xml, coefficients)
    print(f"🏷️  Офферів у фіді: {len(offer_map)}")

    updated_xml = filter_unavailable_offers(xml)
    updated_xml = apply_prices(updated_xml, offer_map, currency_rates)
    updated_xml = transform_prom_image_urls(updated_xml)
    updated_xml = fill_missing_vendor(updated_xml)
    updated_xml = add_name_ua(updated_xml)

    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    OUTPUT_PATH.write_text(updated_xml, encoding="utf-8")
    print(f"✅ Збережено: {OUTPUT_PATH}")


if __name__ == "__main__":
    main()
