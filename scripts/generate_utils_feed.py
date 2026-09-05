"""
generate_utils_feed.py — спільні утиліти для генерації фідів маркетплейсів.

Підключається з generate_{market}_feed.py.
Кожна функція відповідає за одну конкретну задачу.

Ціноутворення → services/market_pricing.py (apply_market_prices)
URL фідів     → constants_feed_url.py
"""

import csv
import logging
import re
import time
from collections import Counter
from collections.abc import Callable
from decimal import Decimal, ROUND_CEILING
from pathlib import Path

import requests
from requests.exceptions import ChunkedEncodingError, ConnectionError as RequestsConnectionError

from services.pricing_rules import ArticlePrices

_logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Public config — змінюйте тут при додаванні нових постачальників
# ---------------------------------------------------------------------------

# Постачальники, чиї *_old.csv містять оптові ціни.
# При додаванні нового постачальника в update_products.py —
# додайте його і сюди, якщо він має стовпець Оптова_ціна.
WHOLESALE_SUPPLIERS: list[str] = ["viatec", "secur", "lp"]

# ---------------------------------------------------------------------------
# Private constants
# ---------------------------------------------------------------------------

# Назви стовпців у Prom.ua-форматному CSV (роздільник ';')
_COL_CODE: str = "Код_товару"             # відповідає <article> у XML
_COL_IDENTIFIER: str = "Ідентифікатор_товару"
_COL_RETAIL_SOURCE: str = "Мінімальний_обсяг_замовлення"  # raw РРЦ постачальника, збережена pipeline
_COL_RETAIL: str = "Ціна"                  # fallback для старих *_old.csv без raw РРЦ
_COL_WHOLESALE: str = "Оптова_ціна"        # дилерська ціна (dealer, ArticlePrices.dealer)
_PROM_ID_PREFIX: str = "prom_"

DEFAULT_VENDOR: str = "Anker"
DEFAULT_COUNTRY: str = "Китай"

# Виробники-псевдоніми: vendor містить «електрон» (case-insensitive) → замінюємо на Anker.
# Країну замінюємо лише якщо вона рівно «Україна» → «Китай».
_VENDOR_ALIAS_RE: re.Pattern[str] = re.compile(r"електрон|электрон", re.IGNORECASE)
_VENDOR_ALIAS_TARGET: str = "Anker"
_COUNTRY_ALIAS_MAP: dict[str, str] = {
    "Україна": "Китай",
}

# Матчать обидві форми тегу: самозакриваючий <vendor/> і звичайний <vendor>TEXT</vendor>.
# group(1) = текст всередині (або None для самозакриваючого).
_VENDOR_FULL_RE: re.Pattern[str] = re.compile(
    r"<vendor\s*/>|<vendor>(.*?)</vendor>", re.DOTALL
)
_COUNTRY_FULL_RE: re.Pattern[str] = re.compile(
    r"<country_of_origin\s*/>|<country_of_origin>(.*?)</country_of_origin>", re.DOTALL
)

# ---------------------------------------------------------------------------
# XML feed — завантаження та парсинг
# ---------------------------------------------------------------------------

_FETCH_RETRIES: int = 3
_FETCH_BACKOFF: float = 10.0
_FETCH_CHUNK_SIZE: int = 1024 * 1024  # 1 MB


def fetch_xml(url: str) -> str:
    """
    Завантажує XML-фід потоково (stream=True) з retry при передчасному обриві.

    Chunked-відповідь може обриватись на великих фідах (ProtocolError /
    ChunkedEncodingError).  stream=True + iter_content дозволяє зібрати
    байти по шматках і повторити спробу без завантаження всього тіла одразу.
    """
    print("⬇️  Завантаження фіду...")

    last_exc: Exception | None = None
    for attempt in range(1, _FETCH_RETRIES + 1):
        try:
            with requests.get(url, timeout=120, stream=True) as response:
                response.raise_for_status()
                chunks: list[bytes] = []
                for chunk in response.iter_content(chunk_size=_FETCH_CHUNK_SIZE):
                    if chunk:
                        chunks.append(chunk)
                raw = b"".join(chunks)
            break  # успішно завантажено
        except (ChunkedEncodingError, RequestsConnectionError) as exc:
            last_exc = exc
            if attempt == _FETCH_RETRIES:
                raise
            wait = _FETCH_BACKOFF * attempt
            print(f"⚠️  Спроба {attempt}/{_FETCH_RETRIES} невдала: {exc}. Повтор через {wait:.0f}с...")
            time.sleep(wait)

    match = re.search(rb'encoding=["\']([^"\']+)["\']', raw[:200])
    encoding = match.group(1).decode("ascii") if match else "utf-8"
    print(f"🔍 Кодування фіду: {encoding} | Розмір: {len(raw):,} байт")

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
# CSV — завантаження оптових цін
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


def load_article_price_index(root: Path) -> dict[str, ArticlePrices]:
    """
    Будує {Код_товару: ArticlePrices(retail, dealer, supplier)} з усіх supplier
    *_old.csv файлів (визначених у WHOLESALE_SUPPLIERS).

    ArticlePrices несе одразу три речі, потрібні pricing_rules/*.py:
      - dealer   — Оптова_ціна (нижня межа ціни каналу, resolve_channel_price)
      - retail   — Ціна постачальника (верхня частина формули, retail * coef)
      - supplier — джерело рядка ("viatec"/"secur"/"lp") — обирає coef_{supplier}
                   з SupplierCoefficients (per-supplier коефіцієнти у market_pricing).

    Правило вибору рядка:
      - Серед рядків з однаковим Код_товару беремо той,
        де Ідентифікатор_товару НЕ починається з 'prom_'.
      - Рядки з порожньою або нульовою Оптова_ціна пропускаємо.
      - Відсутня/некоректна Ціна (retail) не блокує рядок — retail=0
        (resolve_channel_price() має fallback: dealer * threshold).

    Повертає порожній dict, якщо файли відсутні (GitHub Actions:
    гілка data-latest не була відновлена) — market_pricing автоматично
    використає ціну з XML-фіду.
    """
    index: dict[str, ArticlePrices] = {}

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
                raw_dealer = (row.get(_COL_WHOLESALE) or "").strip().replace(",", ".")

                # Пропускаємо: немає коду, або prom_-ідентифікатор, або немає дилерської ціни
                if not code or identifier.startswith(_PROM_ID_PREFIX) or not raw_dealer:
                    continue

                try:
                    dealer_price = Decimal(raw_dealer)
                except Exception:
                    continue

                if dealer_price <= 0:
                    continue

                raw_retail = (row.get(_COL_RETAIL_SOURCE) or "").strip().replace(",", ".")
                if not raw_retail:
                    raw_retail = (row.get(_COL_RETAIL) or "").strip().replace(",", ".")
                try:
                    retail_price = Decimal(raw_retail) if raw_retail else Decimal("0")
                except Exception:
                    retail_price = Decimal("0")

                index[code] = ArticlePrices(
                    retail=retail_price,
                    dealer=dealer_price,
                    supplier=supplier,
                )
                loaded += 1

        print(f"📦 {csv_path.name}: {loaded} оптових цін завантажено")

    if not index:
        print("ℹ️  Оптові ціни не знайдено — буде використано ціну з XML-фіду")
    else:
        print(f"✅ Індекс оптових цін: {len(index)} позицій (джерела: {WHOLESALE_SUPPLIERS})")

    return index


# ---------------------------------------------------------------------------
# XML-трансформації
# ---------------------------------------------------------------------------

def replace_vendor_aliases(xml: str) -> str:
    """
    Нормалізує виробників-псевдоніми:
      - vendor містить «електрон» (case-insensitive) → замінюється на _VENDOR_ALIAS_TARGET
      - country_of_origin замінюється лише якщо значення є у _COUNTRY_ALIAS_MAP
        (тобто «Україна» → «Китай»; інші країни не чіпаємо)

    Виводить детальний звіт: яка назва → на яку і скільки разів.
    """
    replaced = 0
    alias_counter: Counter[str] = Counter()  # {original_vendor: count}

    def on_offer(m: re.Match) -> str:
        nonlocal replaced
        offer_id: str = m.group(1)
        tail_attrs: str = m.group(2)
        body: str = m.group(3)

        vendor_match = re.search(r"<vendor>(.*?)</vendor>", body, re.DOTALL)
        if not vendor_match or not _VENDOR_ALIAS_RE.search(vendor_match.group(1).strip()):
            return m.group(0)

        original_vendor = vendor_match.group(1).strip()
        alias_counter[original_vendor] += 1

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
        for original, count in alias_counter.most_common():
            print(f"    └─ {original!r} → {_VENDOR_ALIAS_TARGET!r}: {count} шт")
    return xml


def fill_missing_vendor(
    xml: str,
    on_vendor_filled: Callable[[str, str], None] | None = None,
) -> str:
    """
    Підставляє <vendor> і <country_of_origin> якщо вони відсутні або порожні.

    Обробляє три випадки для кожного тегу:
      1. Тег відсутній повністю       → вставляємо після </price> / </vendor>.
      2. Самозакриваючий <vendor/>    → замінюємо на <vendor>DEFAULT</vendor> in-place.
         (Prom.ua видає саме цю форму коли виробник не знайдений у їх базі.)
      3. Порожній <vendor></vendor>   → замінюємо on-place.

    Старий підхід не матчив <vendor/> через відсутність </vendor>,
    тому вставляв новий тег поруч із самозакриваючим → дублікат у XML.

    Args:
        xml: Повний XML-фід.
        on_vendor_filled: Необов'язковий callback для діагностики. Отримує
            ``(offer_id, reason)``, де reason — ``відсутній``, ``порожній``
            або ``<vendor/>``. Помилка callback не впливає на генерацію фіду.
    """
    cnt_missing: int = 0      # тег відсутній повністю
    cnt_empty: int = 0        # <vendor></vendor>
    cnt_self_close: int = 0   # <vendor/>

    def on_offer(m: re.Match) -> str:
        nonlocal cnt_missing, cnt_empty, cnt_self_close
        offer_id: str = m.group(1)
        tail_attrs: str = m.group(2)
        body: str = m.group(3)

        # --- vendor ---
        vendor_match = _VENDOR_FULL_RE.search(body)
        vendor_value: str = (vendor_match.group(1) or "").strip() if vendor_match else ""

        fill_reason: str | None = None
        if vendor_match is None:
            # Тег відсутній → вставляємо після </price>
            price_end = re.search(r"</price>", body)
            pos = price_end.end() if price_end else 0
            body = body[:pos] + f"\n<vendor>{DEFAULT_VENDOR}</vendor>" + body[pos:]
            cnt_missing += 1
            fill_reason = "відсутній"
        elif not vendor_value:
            # <vendor/> або <vendor></vendor> → замінюємо тег in-place
            is_self_close = vendor_match.group(0).endswith("/>")
            body = body.replace(vendor_match.group(0), f"<vendor>{DEFAULT_VENDOR}</vendor>", 1)
            if is_self_close:
                cnt_self_close += 1
                fill_reason = "<vendor/>"
            else:
                cnt_empty += 1
                fill_reason = "порожній"

        if fill_reason is not None and on_vendor_filled is not None:
            try:
                on_vendor_filled(offer_id, fill_reason)
            except Exception:
                _logger.warning(
                    "fill_missing_vendor: callback помилився для offer_id=%s",
                    offer_id,
                    exc_info=True,
                )

        # --- country_of_origin ---
        country_match = _COUNTRY_FULL_RE.search(body)
        country_value: str = (country_match.group(1) or "").strip() if country_match else ""

        if country_match is None:
            vendor_end = re.search(r"</vendor>", body)
            pos = vendor_end.end() if vendor_end else len(body)
            body = body[:pos] + f"\n<country_of_origin>{DEFAULT_COUNTRY}</country_of_origin>" + body[pos:]
        elif not country_value:
            body = body.replace(
                country_match.group(0),
                f"<country_of_origin>{DEFAULT_COUNTRY}</country_of_origin>",
                1,
            )

        return f'<offer id="{offer_id}"{tail_attrs}>{body}</offer>'

    xml = re.sub(
        r'<offer\s+id="(\d+)"([^>]*)>(.*?)</offer>',
        on_offer,
        xml,
        flags=re.DOTALL,
    )

    total = cnt_missing + cnt_empty + cnt_self_close
    parts: list[str] = []
    if cnt_self_close:
        parts.append(f"<vendor/>: {cnt_self_close}")
    if cnt_empty:
        parts.append(f"порожній: {cnt_empty}")
    if cnt_missing:
        parts.append(f"відсутній: {cnt_missing}")
    detail = f" ({', '.join(parts)})" if parts else ""
    print(
        f"🏭  Підставлено виробника за замовчуванням"
        f" ({DEFAULT_VENDOR} / {DEFAULT_COUNTRY}): {total} товарів{detail}"
    )
    return xml


def filter_unavailable_offers(xml: str) -> str:
    """Видаляє оффери з available='false'."""
    before = len(re.findall(r'<offer\s', xml))
    xml = re.sub(
        r'[ \t]*<offer\s[^>]*available="false"[^>]*>.*?</offer>[ \t]*\n?',
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
