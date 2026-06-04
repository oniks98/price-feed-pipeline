"""
generate_merchant_feed.py
-------------------------
Крок 2 пайплайну: обогащает Google Merchant Center XML-фид метками.

Читает:
  data/markets/merchant_rule.csv          → custom_label_0 (theme), custom_label_3 (schedule),
                                            google_product_category (google_cat_id)

Добавляет к каждому <item>:
  <g:google_product_category> = числовой ID из таксономии Google (из google_cat_id в rules CSV)
  <g:custom_label_0> = theme    (по g:product_type → product_type в rules CSV)
  <g:custom_label_1> = brand    (напрямую из g:brand, очищен от HTML-тегов и сущностей)
  <g:custom_label_2> = price_tier (по g:price → PRICE_BUCKETS)
  <g:custom_label_3> = schedule (по g:product_type → schedule в rules CSV)

Колонки google_cat_hint и notes в rules CSV — справочные, в пайплайн не попадают.

Цена: берётся из <g:price> as-is, только для классификации тира.
custom_label теги отсутствуют в исходном фиде — вставляются после <g:condition>.

Гарантии:
  - Один проход по XML (stream-safe regex).
  - html.unescape(): Prom кодирует '>' как '&gt;' внутри тегов → декодируем перед lookup.
  - Нормализация whitespace: все \n \t \r → один пробел.
  - Fallback при отсутствии матча: theme=other, segment=other, schedule=day.
  - Не изменяет цены, availability, images или другие теги.
  - Идемпотентен: повторный запуск перезаписывает выходной файл.

Запуск:
    python scripts/generate_merchant_feed.py
    python scripts/generate_merchant_feed.py --dry-run
"""

from __future__ import annotations

import argparse
import csv
import html
import logging
import re
import sys
import time
from pathlib import Path
from typing import Final, NamedTuple

import requests
from requests.exceptions import ChunkedEncodingError, ConnectionError as RequestsConnectionError

from constants_feed_url import FEED_URL_MERCHANT as FEED_URL

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------

BASE_DIR:    Final[Path] = Path(__file__).parent.parent
MARKETS_DIR: Final[Path] = BASE_DIR / "data" / "markets"

RULES_CSV:   Final[Path] = MARKETS_DIR / "merchant_rule.csv"
OUTPUT_PATH: Final[Path] = MARKETS_DIR / "merchant_feed.xml"

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

ENCODING:  Final[str] = "utf-8-sig"
DELIMITER: Final[str] = ";"

FALLBACK_THEME:    Final[str] = "other"
FALLBACK_SCHEDULE: Final[str] = "day"
FALLBACK_TIER:     Final[str] = "high"

# Price buckets: (upper_bound_exclusive, label)
# Price <  300   → low
# Price < 1000   → mid_low
# Price < 3000   → mid_high
# Price >= 3000  → high
PRICE_BUCKETS: Final[list[tuple[float, str]]] = [
    (300,          "low"),
    (1000,         "mid_low"),
    (3000,         "mid_high"),
    (float("inf"), "high"),
]

# ---------------------------------------------------------------------------
# Data structures
# ---------------------------------------------------------------------------

class RuleEntry(NamedTuple):
    theme:         str
    schedule:      str
    google_cat_id: str


# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------

log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_RE_HTML_TAG = re.compile(r"<[^>]+>")


def _normalize_text(raw: str) -> str:
    """
    Нормализует текст из XML-тега или CSV-ячейки для lookup:
      1. html.unescape(): '&gt;' → '>'  (Prom кодирует '>' как HTML-сущность)
      2. strip HTML-тегов: убирает <br>, <b> и т.п. (встречаются в g:brand)
      3. " ".join(split()): схлопывает все whitespace (\n \t \r \xa0) в один пробел

    Применяется и к значениям из XML, и к ключам из CSV —
    сравнение происходит в одном нормализованном пространстве.
    """
    unescaped = html.unescape(raw)
    stripped  = _RE_HTML_TAG.sub(" ", unescaped)
    return " ".join(stripped.split())


# ---------------------------------------------------------------------------
# CSV loaders — O(1) lookup dicts, loaded once
# ---------------------------------------------------------------------------

def load_rules(path: Path) -> dict[str, RuleEntry]:
    """
    Читает merchant_rule.csv.
    Колонки: product_type, theme, schedule, google_cat_id.
    Колонки google_cat_hint и notes — справочные, игнорируются.
    Возвращает {product_type: RuleEntry(theme, schedule, google_cat_id)}.
    """
    if not path.exists():
        log.error("rules CSV не знайдено: %s", path)
        return {}

    index: dict[str, RuleEntry] = {}
    with path.open(encoding=ENCODING, newline="") as f:
        for row in csv.DictReader(f, delimiter=DELIMITER):
            product_type  = _normalize_text(row.get("product_type") or "")
            theme         = _normalize_text(row.get("theme")        or "") or FALLBACK_THEME
            schedule      = _normalize_text(row.get("schedule")     or "") or FALLBACK_SCHEDULE
            google_cat_id = (row.get("google_cat_id") or "").strip()
            if product_type:
                index[product_type] = RuleEntry(
                    theme=theme,
                    schedule=schedule,
                    google_cat_id=google_cat_id,
                )

    log.info("Завантажено %d правил з %s", len(index), path.name)
    return index


# ---------------------------------------------------------------------------
# Classifiers
# ---------------------------------------------------------------------------

def classify_price_tier(price_str: str) -> str:
    """
    Парсит строку вида '1949.00 UAH' или '237.00' → возвращает tier label.
    Fallback при ошибке парсинга: FALLBACK_TIER.
    """
    numeric = re.match(r"[\d.,]+", price_str.strip())
    if not numeric:
        log.warning("Не вдалося розпарсити ціну: %r → fallback=%s", price_str, FALLBACK_TIER)
        return FALLBACK_TIER

    try:
        price = float(numeric.group().replace(",", "."))
    except ValueError:
        log.warning("Не вдалося конвертувати ціну: %r → fallback=%s", price_str, FALLBACK_TIER)
        return FALLBACK_TIER

    for upper_bound, label in PRICE_BUCKETS:
        if price < upper_bound:
            return label

    return FALLBACK_TIER


def resolve_labels(
    product_type: str,
    brand: str,
    price_str: str,
    rules_index: dict[str, RuleEntry],
) -> tuple[str, str, str, str, str]:
    """Возвращает (theme, brand, price_tier, schedule, google_cat_id)."""
    rule     = rules_index.get(product_type)
    theme    = rule.theme         if rule else FALLBACK_THEME
    schedule = rule.schedule      if rule else FALLBACK_SCHEDULE
    cat_id   = rule.google_cat_id if rule else ""
    tier     = classify_price_tier(price_str)

    if not rule:
        log.debug("rule miss: product_type=%r", product_type)

    return theme, brand, tier, schedule, cat_id


# ---------------------------------------------------------------------------
# XML fetch
# ---------------------------------------------------------------------------

_FETCH_RETRIES:    Final[int]   = 3
_FETCH_BACKOFF:    Final[float] = 10.0
_FETCH_CHUNK_SIZE: Final[int]   = 1024 * 1024  # 1 MB


def fetch_feed(url: str) -> str:
    """
    Завантажує XML-фід потоково (stream=True) з retry при обриві з'єднання.

    urllib IncompleteRead та requests ChunkedEncodingError мають одну причину:
    сервер закриває chunked-з'єднання до завершення передачі.
    stream=True + iter_content збирає байти шматками і дозволяє повторити спробу.
    """
    log.info("Завантаження фіду: %s", url.split("?")[0])

    headers = {"User-Agent": "MerchantFeedGenerator/1.0"}
    raw: bytes = b""

    for attempt in range(1, _FETCH_RETRIES + 1):
        try:
            with requests.get(url, headers=headers, timeout=120, stream=True) as resp:
                resp.raise_for_status()
                chunks: list[bytes] = []
                for chunk in resp.iter_content(chunk_size=_FETCH_CHUNK_SIZE):
                    if chunk:
                        chunks.append(chunk)
                raw = b"".join(chunks)
            break
        except (ChunkedEncodingError, RequestsConnectionError) as exc:
            if attempt == _FETCH_RETRIES:
                raise
            wait = _FETCH_BACKOFF * attempt
            log.warning(
                "Спроба %d/%d невдала: %s. Повтор через %.0f с...",
                attempt, _FETCH_RETRIES, exc, wait,
            )
            time.sleep(wait)

    match = re.search(rb'encoding=["\']([^"\']+)["\']', raw[:200])
    src_encoding = match.group(1).decode("ascii") if match else "utf-8"
    log.info("Кодування фіду: %s | Розмір: %d bytes", src_encoding, len(raw))

    return raw.decode(src_encoding)


# ---------------------------------------------------------------------------
# XML enrichment — один проход
# ---------------------------------------------------------------------------

_RE_PRODUCT_TYPE = re.compile(r"<g:product_type>(.*?)</g:product_type>",  re.DOTALL)
_RE_BRAND        = re.compile(r"<g:brand>(.*?)</g:brand>",                re.DOTALL)
_RE_PRICE        = re.compile(r"<g:price>(.*?)</g:price>",                re.DOTALL)
_RE_CONDITION    = re.compile(r"(<g:condition>.*?</g:condition>)",         re.DOTALL)

_RE_CUSTOM_LABELS = re.compile(
    r"\s*<g:custom_label_[0-3]>.*?</g:custom_label_[0-3]>",
    re.DOTALL,
)

_RE_GOOGLE_CAT = re.compile(
    r"\s*<g:google_product_category>.*?</g:google_product_category>",
    re.DOTALL,
)


def _xml_escape(value: str) -> str:
    """Экранирует &, <, > для вставки в XML-тег."""
    return html.escape(value, quote=False)


def _build_labels_block(theme: str, segment: str, tier: str, schedule: str, cat_id: str) -> str:
    """
    Строит блок тегов для вставки в <item>.
    <g:google_product_category> ставится первым (числовой ID таксономии Google),
    только если cat_id не пустой. custom_label_* идут следом.
    """
    labels = (
        f"<g:custom_label_0>{_xml_escape(theme)}</g:custom_label_0>\n"
        f"<g:custom_label_1>{_xml_escape(segment)}</g:custom_label_1>\n"
        f"<g:custom_label_2>{_xml_escape(tier)}</g:custom_label_2>\n"
        f"<g:custom_label_3>{_xml_escape(schedule)}</g:custom_label_3>"
    )
    if cat_id:
        # cat_id — числовой ID, xml_escape безвреден но явен
        return f"<g:google_product_category>{cat_id}</g:google_product_category>\n" + labels
    return labels


def enrich_xml(
    xml: str,
    rules_index: dict[str, RuleEntry],
) -> tuple[str, dict[str, int]]:
    """
    Один проход по всем <item>: вставляет/заменяет custom_label_0..3
    и <g:google_product_category>. Возвращает (обогащённый_xml, stats).
    """
    stats: dict[str, int] = {
        "total":        0,
        "rule_matched": 0,
        "rule_missed":  0,
        "no_brand":     0,
        "no_cat_id":    0,
    }

    def on_item(m: re.Match) -> str:
        body: str = m.group(1)
        stats["total"] += 1

        pt_match    = _RE_PRODUCT_TYPE.search(body)
        brand_match = _RE_BRAND.search(body)
        price_match = _RE_PRICE.search(body)

        # _normalize_text включает html.unescape() — ключевой fix для &gt;
        product_type = _normalize_text(pt_match.group(1))    if pt_match    else ""
        brand        = _normalize_text(brand_match.group(1)) if brand_match else ""
        price_str    = _normalize_text(price_match.group(1)) if price_match else "0"

        theme, segment, tier, schedule, cat_id = resolve_labels(
            product_type, brand, price_str, rules_index,
        )

        if product_type in rules_index:
            stats["rule_matched"] += 1
        else:
            stats["rule_missed"] += 1

        if not brand:
            stats["no_brand"] += 1
            log.debug("no brand for item #%d", stats["total"])

        if not cat_id:
            stats["no_cat_id"] += 1
            log.debug("no google_cat_id for product_type=%r item #%d", product_type, stats["total"])

        # Идемпотентность: вырезаем старые теги перед вставкой новых
        body = _RE_CUSTOM_LABELS.sub("", body)
        body = _RE_GOOGLE_CAT.sub("", body)

        labels_block = _build_labels_block(theme, segment, tier, schedule, cat_id)

        condition_match = _RE_CONDITION.search(body)
        if condition_match:
            insert_pos = condition_match.end()
            body = body[:insert_pos] + "\n" + labels_block + body[insert_pos:]
        else:
            body = body + "\n" + labels_block + "\n"

        return f"<item>{body}</item>"

    enriched = re.sub(r"<item>(.*?)</item>", on_item, xml, flags=re.DOTALL)
    return enriched, stats


# ---------------------------------------------------------------------------
# Pipeline
# ---------------------------------------------------------------------------

def run(*, dry_run: bool = False) -> None:
    rules_index = load_rules(RULES_CSV)

    if not rules_index:
        log.error("Правила не завантажено — перервано")
        sys.exit(1)

    xml = fetch_feed(FEED_URL)
    log.info("Отримано %d символів", len(xml))

    enriched, stats = enrich_xml(xml, rules_index)

    log.info(
        "Результат: total=%d | rule_matched=%d | rule_missed=%d | no_brand=%d | no_cat_id=%d",
        stats["total"],
        stats["rule_matched"],
        stats["rule_missed"],
        stats["no_brand"],
        stats["no_cat_id"],
    )

    if dry_run:
        log.info("--dry-run: файл не записано")
        return

    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    OUTPUT_PATH.write_text(enriched, encoding="utf-8")
    log.info("Збережено: %s", OUTPUT_PATH)


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description=(
            "Крок 2: обогащает Google Merchant Center XML-фид метками "
            "custom_label_0..3 из CSV-правил и ценовых бакетов."
        )
    )
    p.add_argument(
        "--dry-run", action="store_true",
        help="Показати статистику без запису файлу.",
    )
    return p.parse_args()


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(levelname)-8s %(message)s",
    )
    args = _parse_args()
    run(dry_run=args.dry_run)


if __name__ == "__main__":
    main()
