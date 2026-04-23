"""
feed_common.py — спільні утиліти для генерації фідів маркетплейсів.

Підключається з generate_{market}_feed.py.
Кожна функція відповідає за одну конкретну задачу.

Алгоритм ціноутворення (новий):
  1. З XML-фіду витягуємо <article>ЧИСЛО</article> кожного оферу
  2. Шукаємо ЧИСЛО в data/{supplier}/{supplier}_old.csv (стовпець Код_товару)
  3. З відповідних рядків беремо той, де Ідентифікатор_товару НЕ має префіксу prom_
  4. Оптова_ціна з цього рядка × коефіцієнт категорії = нова ціна
  5. Fallback (якщо артикул не знайдено або Оптова_ціна порожня / нульова):
     ціна з XML × DEFAULT_COEFFICIENT (не коефіцієнт категорії)
"""

import csv
import re
from decimal import Decimal, ROUND_CEILING, ROUND_HALF_UP
from pathlib import Path
from typing import TypeAlias

import requests

# ---------------------------------------------------------------------------
# Public config — змінюйте тут при додаванні нових постачальників
# ---------------------------------------------------------------------------

# Постачальники, чиї *_old.csv містять оптові ціни.
# При додаванні нового постачальника в update_products.py —
# додайте його і сюди, якщо він має стовпець Оптова_ціна.
WHOLESALE_SUPPLIERS: list[str] = ["viatec", "secur"]

# ---------------------------------------------------------------------------
# Private constants
# ---------------------------------------------------------------------------

# Назви стовпців у Prom.ua-форматному CSV (роздільник ';')
_COL_CODE: str = "Код_товару"             # відповідає <article> у XML
_COL_IDENTIFIER: str = "Ідентифікатор_товару"
_COL_WHOLESALE: str = "Оптова_ціна"
_PROM_ID_PREFIX: str = "prom_"

PROM_IMAGE_RE: re.Pattern[str] = re.compile(
    r'https://images\.prom\.ua/(?:[^"<\s]*/)?(\d+)_[^"<\s]+\.jpg'
)

DEFAULT_VENDOR: str = "Anker"
DEFAULT_COUNTRY: str = "Китай"

# Виробники-псевдоніми: vendor містить «електрон» (case-insensitive) → замінюємо на Anker.
# Країну замінюємо лише якщо вона рівно «Україна» → «Китай».
_VENDOR_ALIAS_RE: re.Pattern[str] = re.compile(r"електрон", re.IGNORECASE)
_VENDOR_ALIAS_TARGET: str = "Anker"
_COUNTRY_ALIAS_MAP: dict[str, str] = {
    "Україна": "Китай",
}

# ---------------------------------------------------------------------------
# Type aliases
# ---------------------------------------------------------------------------

OfferData: TypeAlias = dict[str, dict]


# ---------------------------------------------------------------------------
# XML feed — завантаження та парсинг
# ---------------------------------------------------------------------------

def fetch_xml(url: str) -> str:
    """
    Завантажує XML-фід, декодує з правильним кодуванням,
    нормалізує encoding declaration на utf-8.
    """
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
    """Витягує {CURRENCY_ID: курс} з XML, ігноруючи UAH."""
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


# ---------------------------------------------------------------------------
# CSV — завантаження коефіцієнтів та оптових цін
# ---------------------------------------------------------------------------

def _detect_csv_encoding(path: Path) -> str:
    """Визначає кодування CSV-файлу за першими байтами."""
    try:
        raw = path.read_bytes()[:10_000]
    except OSError:
        return "utf-8-sig"

    if raw.startswith(b"\xef\xbb\xbf"):
        return "utf-8-sig"

    for enc in ("utf-8", "utf-8-sig", "windows-1251", "cp1251", "latin-1"):
        try:
            raw.decode(enc)
            return enc
        except (UnicodeDecodeError, LookupError):
            continue

    return "utf-8-sig"


def load_coefficients(
    csv_path: Path,
    coef_column: str,
    default: Decimal,
) -> dict[str, Decimal]:
    """
    Читає markets_coefficients.csv і повертає {category_id: коефіцієнт}
    з вказаної колонки. Якщо файл відсутній — повертає порожній dict
    (caller використовує default).
    """
    if not csv_path.exists():
        print(f"⚠️  {csv_path} не знайдено — використовується DEFAULT {default}")
        return {}

    encoding = _detect_csv_encoding(csv_path)
    coefficients: dict[str, Decimal] = {}

    with csv_path.open(encoding=encoding, errors="replace", newline="") as f:
        first = f.readline()
        delimiter = ";" if ";" in first else ","
        f.seek(0)

        for row in csv.DictReader(f, delimiter=delimiter):
            cat_id = (row.get("category_id") or "").strip().strip("\ufeff")
            raw = (row.get(coef_column) or "").strip().replace(",", ".")
            try:
                coefficients[cat_id] = Decimal(raw)
            except Exception:
                print(f"⚠️  Невірний коефіцієнт для category_id={cat_id!r}: '{raw}' — пропущено")

    print(f"📋 Завантажено {len(coefficients)} категорій з коефіцієнтами ({coef_column})")
    return coefficients


def load_wholesale_price_index(root: Path) -> dict[str, Decimal]:
    """
    Будує {Код_товару: Оптова_ціна} з усіх supplier *_old.csv файлів
    (визначених у WHOLESALE_SUPPLIERS).

    Правило вибору рядка:
      - Серед рядків з однаковим Код_товару беремо той,
        де Ідентифікатор_товару НЕ починається з 'prom_'.
      - Рядки з порожньою або нульовою Оптова_ціна пропускаємо.

    Повертає порожній dict, якщо файли відсутні (GitHub Actions:
    гілка data-latest не була відновлена) — apply_prices автоматично
    використає ціну з XML-фіду.
    """
    index: dict[str, Decimal] = {}

    for supplier in WHOLESALE_SUPPLIERS:
        csv_path = root / "data" / supplier / f"{supplier}_old.csv"

        if not csv_path.exists():
            print(f"⚠️  {csv_path.name} не знайдено — оптові ціни {supplier} пропущено")
            continue

        encoding = _detect_csv_encoding(csv_path)
        loaded = 0

        with csv_path.open(encoding=encoding, errors="replace", newline="") as f:
            first = f.readline()
            delimiter = ";" if ";" in first else ","
            f.seek(0)

            for row in csv.DictReader(f, delimiter=delimiter):
                code = (row.get(_COL_CODE) or "").strip()
                identifier = (row.get(_COL_IDENTIFIER) or "").strip()
                raw_price = (row.get(_COL_WHOLESALE) or "").strip().replace(",", ".")

                # Пропускаємо: немає коду, або prom_-ідентифікатор, або немає ціни
                if not code or identifier.startswith(_PROM_ID_PREFIX) or not raw_price:
                    continue

                try:
                    price = Decimal(raw_price)
                except Exception:
                    continue

                if price > 0:
                    index[code] = price
                    loaded += 1

        print(f"📦 {csv_path.name}: {loaded} оптових цін завантажено")

    if not index:
        print("ℹ️  Оптові ціни не знайдено — буде використано ціну з XML-фіду")
    else:
        print(f"✅ Індекс оптових цін: {len(index)} позицій (джерела: {WHOLESALE_SUPPLIERS})")

    return index


# ---------------------------------------------------------------------------
# Побудова мапи офферів
# ---------------------------------------------------------------------------

def build_offer_data_map(
    xml: str,
    coefficients: dict[str, Decimal],
    wholesale_index: dict[str, Decimal],
    default_coefficient: Decimal,
) -> OfferData:
    """
    Повертає {offer_id: {coefficient, fallback_coefficient, currency_id, wholesale_price}}
    для кожного оферу у XML.

    coefficient          — коефіцієнт категорії з CSV (для оптової ціни).
    fallback_coefficient — default_coefficient (для XML-ціни при відсутності оптової).
    wholesale_price = None → apply_prices використає ціну з XML (fallback).
    """
    offer_map: OfferData = {}

    for m in re.finditer(r'<offer\s+id="(\d+)"[^>]*>(.*?)</offer>', xml, re.DOTALL):
        offer_id = m.group(1)
        body = m.group(2)

        cat_match = re.search(r"<categoryId>(\d+)</categoryId>", body)
        cat_id = cat_match.group(1) if cat_match else ""

        cur_match = re.search(r"<currencyId>([^<]+)</currencyId>", body)
        currency_id = cur_match.group(1).strip().upper() if cur_match else "UAH"

        article_match = re.search(r"<article>(\d+)</article>", body)
        article = article_match.group(1).strip() if article_match else None

        offer_map[offer_id] = {
            # Коефіцієнт категорії з CSV — застосовується лише до оптової ціни.
            "coefficient": coefficients.get(cat_id, default_coefficient),
            # Для fallback (ціна з XML) завжди використовуємо default_coefficient,
            # незалежно від того, чи є категорія у CSV-файлі коефіцієнтів.
            "fallback_coefficient": default_coefficient,
            "currency_id": currency_id,
            # None якщо артикул відсутній у XML або не знайдено в оптовому індексі
            "wholesale_price": wholesale_index.get(article) if article else None,
        }

    return offer_map


# ---------------------------------------------------------------------------
# Застосування цін
# ---------------------------------------------------------------------------

def apply_prices(
    xml: str,
    offer_map: OfferData,
    currency_rates: dict[str, Decimal],
) -> str:
    """
    Замінює <price> у кожному офері:
      - Є wholesale_price → використовуємо оптову ціну (вже в UAH, конвертація не потрібна)
      - Інакше (fallback) → ціна з XML, конвертація з іноземної валюти якщо потрібно

    В обох випадках <currencyId> оновлюється на UAH.
    """
    wholesale_count = 0
    fallback_count = 0
    converted_count = 0

    def on_offer(m: re.Match) -> str:
        nonlocal wholesale_count, fallback_count, converted_count
        offer_id: str = m.group(1)
        tail_attrs: str = m.group(2)
        body: str = m.group(3)

        data = offer_map.get(offer_id)
        if data is None:
            return m.group(0)

        # Коефіцієнт залежить від джерела ціни:
        #   wholesale_price → коефіцієнт категорії з CSV
        #   fallback (XML)  → default_coefficient
        wholesale_price: Decimal | None = data["wholesale_price"]
        coeff: Decimal = (
            data["coefficient"] if wholesale_price is not None
            else data["fallback_coefficient"]
        )
        currency_id: str = data["currency_id"]

        def replace_price(pm: re.Match) -> str:
            nonlocal wholesale_count, fallback_count, converted_count
            raw = pm.group(1).strip()
            try:
                if wholesale_price is not None:
                    # Оптова ціна вже в UAH — конвертація не потрібна
                    base_price = wholesale_price
                    wholesale_count += 1
                else:
                    # Fallback: ціна з XML
                    base_price = Decimal(raw.replace(",", "."))
                    if currency_id != "UAH":
                        rate = currency_rates.get(currency_id)
                        if rate is None:
                            print(
                                f"⚠️  Курс для {currency_id} не знайдено, "
                                f"оффер {offer_id} — ціна без конвертації"
                            )
                        else:
                            base_price *= rate
                            converted_count += 1
                    fallback_count += 1

                new_price = (base_price * coeff).quantize(Decimal("1"), rounding=ROUND_CEILING)
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

    print(f"💰 Ціна з оптового прайсу: {wholesale_count} | Fallback (XML): {fallback_count}")
    if converted_count:
        print(f"💱 Конвертовано з іноземної валюти в UAH: {converted_count}")

    return xml


# ---------------------------------------------------------------------------
# XML-трансформації
# ---------------------------------------------------------------------------

def transform_prom_image_urls(xml: str) -> str:
    """Нормалізує URL зображень Prom.ua до формату w640_h640."""
    result, count = PROM_IMAGE_RE.subn(
        lambda m: f"https://images.prom.ua/{m.group(1)}_w640_h640_{m.group(1)}.jpg",
        xml,
    )
    print(f"🖼️  Нормалізовано URL зображень Prom.ua → w640_h640: {count}")
    return result


def replace_vendor_aliases(xml: str) -> str:
    """
    Нормалізує виробників-псевдоніми:
      - vendor містить «електрон» (case-insensitive) → замінюється на _VENDOR_ALIAS_TARGET
      - country_of_origin замінюється лише якщо значення є у _COUNTRY_ALIAS_MAP
        (тобто «Україна» → «Китай»; інші країни не чіпаємо)
    """
    replaced = 0

    def on_offer(m: re.Match) -> str:
        nonlocal replaced
        offer_id: str = m.group(1)
        tail_attrs: str = m.group(2)
        body: str = m.group(3)

        vendor_match = re.search(r"<vendor>(.*?)</vendor>", body, re.DOTALL)
        if not vendor_match or not _VENDOR_ALIAS_RE.search(vendor_match.group(1).strip()):
            return m.group(0)

        # Замінюємо vendor
        body = body.replace(
            vendor_match.group(0),
            f"<vendor>{_VENDOR_ALIAS_TARGET}</vendor>",
            1,
        )

        # Замінюємо country_of_origin лише якщо значення є у _COUNTRY_ALIAS_MAP
        country_match = re.search(r"<country_of_origin>(.*?)</country_of_origin>", body, re.DOTALL)
        if country_match:
            country_val = country_match.group(1).strip()
            new_country = _COUNTRY_ALIAS_MAP.get(country_val)
            if new_country:
                body = body.replace(
                    country_match.group(0),
                    f"<country_of_origin>{new_country}</country_of_origin>",
                    1,
                )

        replaced += 1
        return f'<offer id="{offer_id}"{tail_attrs}>{body}</offer>'

    xml = re.sub(
        r'<offer\s+id="(\d+)"([^>]*)>(.*?)</offer>',
        on_offer,
        xml,
        flags=re.DOTALL,
    )
    if replaced:
        print(f"🔄  Замінено псевдоніми виробників: {replaced} товарів")
    return xml


def fill_missing_vendor(xml: str) -> str:
    """
    Підставляє <vendor> і <country_of_origin> якщо вони відсутні або порожні.

    Prom стирає виробника якого немає в своїй базі → фід приходить з порожнім <vendor>.
    Маркетплейси відхиляють такі товари при валідації.
    Fallback: DEFAULT_VENDOR / DEFAULT_COUNTRY.
    """
    filled = 0

    def on_offer(m: re.Match) -> str:
        nonlocal filled
        offer_id: str = m.group(1)
        tail_attrs: str = m.group(2)
        body: str = m.group(3)

        vendor_match = re.search(r"<vendor>(.*?)</vendor>", body, re.DOTALL)
        if not vendor_match or not vendor_match.group(1).strip():
            if vendor_match:
                body = body.replace(vendor_match.group(0), f"<vendor>{DEFAULT_VENDOR}</vendor>", 1)
            else:
                price_end = re.search(r"</price>", body)
                pos = price_end.end() if price_end else 0
                body = body[:pos] + f"\n<vendor>{DEFAULT_VENDOR}</vendor>" + body[pos:]
            filled += 1

        country_match = re.search(r"<country_of_origin>(.*?)</country_of_origin>", body, re.DOTALL)
        if not country_match or not country_match.group(1).strip():
            if country_match:
                body = body.replace(
                    country_match.group(0),
                    f"<country_of_origin>{DEFAULT_COUNTRY}</country_of_origin>",
                    1,
                )
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
    """Видаляє оффери з available='false'."""
    before = len(re.findall(r'<offer\s', xml))
    xml = re.sub(
        r'<offer\s[^>]*available="false"[^>]*>.*?</offer>',
        "",
        xml,
        flags=re.DOTALL,
    )
    after = len(re.findall(r'<offer\s', xml))
    print(f"🗑️  Відфільтровано товарів не в наявності: {before - after} (залишилось {after})")
    return xml


def add_name_ua(xml: str) -> str:
    """
    Додає <name_ua> після <n> якщо відсутній.
    Використовується Kasta та Epicenter (не Rozetka).
    """
    def on_offer(m: re.Match) -> str:
        offer_id: str = m.group(1)
        tail_attrs: str = m.group(2)
        body: str = m.group(3)

        if "<name_ua>" in body:
            return m.group(0)

        name_match = re.search(r"<n>(.*?)</n>", body, re.DOTALL)
        if not name_match:
            return m.group(0)

        body = body.replace(
            name_match.group(0),
            f"{name_match.group(0)}\n<name_ua>{name_match.group(1)}</name_ua>",
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
