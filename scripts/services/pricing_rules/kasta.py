"""
Kasta pricing rules.

CSV schema (semicolon-delimited, utf-8-sig):
  A  prom_category_id
  B  prom_category_name
  C  Приналежність*:6
  D  Група*:13
  E  Вид*:21
  F  royalty_percent
  G  price_from
  H  price_to
  I  coef
  J  coef_uncategorized  — wholesale price exists, but no category rule found
  K  coef_no_base        — no wholesale price → XML price used as base
"""

from __future__ import annotations

import csv
import logging
import re
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from functools import lru_cache
from pathlib import Path
from typing import Final

from ._base import (
    PriceRule,
    PricingStats,
    ceil_uah,
    parse_decimal,
    rule_sort_key,
    select_rule,
    tag_text,
)

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------

_ROOT: Final[Path] = Path(__file__).resolve().parents[3]
COEFFICIENTS_PATH: Final[Path] = _ROOT / "data" / "markets" / "kasta_coefficients.csv"
DEFAULT_LOG_PATH: Final[Path] = _ROOT / "kasta_default_id.log"
_CSV_DELIMITER: Final[str] = ";"
_CSV_ENCODING: Final[str] = "utf-8-sig"


# ---------------------------------------------------------------------------
# Data structure
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class KastaPricingTable:
    coef_uncategorized: Decimal          # wholesale exists, no category rule → no_category_rule
    coef_no_base: Decimal                # no wholesale price → xml_fallback
    rules_by_category: dict[str, tuple[PriceRule, ...]]


# ---------------------------------------------------------------------------
# Logger
# ---------------------------------------------------------------------------

def _build_logger() -> logging.Logger:
    """
    Logger that writes to kasta_default_id.log.
    Overwrites on every run (mode='w') so the log always matches the current feed.
    """
    logger = logging.getLogger("kasta.default_offers")
    if logger.handlers:
        return logger

    logger.setLevel(logging.INFO)
    logger.propagate = False

    handler = logging.FileHandler(DEFAULT_LOG_PATH, mode="w", encoding="utf-8")
    handler.setFormatter(logging.Formatter("%(message)s"))
    logger.addHandler(handler)
    return logger


# ---------------------------------------------------------------------------
# CSV loading
# ---------------------------------------------------------------------------

@lru_cache(maxsize=1)
def _load_pricing() -> KastaPricingTable:
    if not COEFFICIENTS_PATH.exists():
        raise FileNotFoundError(f"Kasta coefficients not found: {COEFFICIENTS_PATH}")

    coef_uncategorized: Decimal | None = None
    coef_no_base: Decimal | None = None
    rules_by_category: dict[str, list[PriceRule]] = {}

    with COEFFICIENTS_PATH.open(encoding=_CSV_ENCODING, errors="replace", newline="") as f:
        reader = csv.DictReader(f, delimiter=_CSV_DELIMITER)
        for row in reader:
            if coef_uncategorized is None:
                raw = (row.get("coef_uncategorized") or "").strip()
                if raw:
                    coef_uncategorized = parse_decimal(raw)

            if coef_no_base is None:
                raw = (row.get("coef_no_base") or "").strip()
                if raw:
                    coef_no_base = parse_decimal(raw)

            category_id = (row.get("prom_category_id") or "").strip().strip("\ufeff")
            raw_coef = (row.get("coef") or "").strip()
            if not category_id or not raw_coef:
                continue

            try:
                rule = PriceRule(
                    category_id=category_id,
                    price_from=parse_decimal(row.get("price_from"), Decimal("0")),
                    price_to=parse_decimal(row.get("price_to"), Decimal("Infinity")),
                    royalty_percent=parse_decimal(row.get("royalty_percent")),
                    coefficient=parse_decimal(raw_coef),
                )
            except InvalidOperation:
                continue

            rules_by_category.setdefault(category_id, []).append(rule)

    if coef_uncategorized is None:
        raise ValueError("coef_uncategorized missing in kasta_coefficients.csv (column J)")
    if coef_no_base is None:
        raise ValueError("coef_no_base missing in kasta_coefficients.csv (column K)")

    frozen_rules: dict[str, tuple[PriceRule, ...]] = {
        cid: tuple(sorted(rules, key=rule_sort_key))
        for cid, rules in rules_by_category.items()
    }
    total = sum(len(v) for v in frozen_rules.values())
    print(
        f"Kasta pricing: loaded {total} rules, "
        f"coef_uncategorized={coef_uncategorized}, coef_no_base={coef_no_base}"
    )
    return KastaPricingTable(coef_uncategorized, coef_no_base, frozen_rules)


# ---------------------------------------------------------------------------
# Public API (consumed by market_pricing.py facade)
# ---------------------------------------------------------------------------

def get_default_coefficient() -> Decimal:
    return _load_pricing().coef_uncategorized


def apply_prices(
    xml: str,
    wholesale_index: dict[str, Decimal],
    currency_rates: dict[str, Decimal],
) -> str:
    pricing = _load_pricing()
    stats = PricingStats()
    log = _build_logger()

    def on_offer(match: re.Match) -> str:
        offer_id: str = match.group(1)
        tail_attrs: str = match.group(2)
        body: str = match.group(3)
        stats.offers += 1

        category_id = tag_text(body, "categoryId") or ""
        article = tag_text(body, "article")
        currency_id = (tag_text(body, "currencyId") or "UAH").upper()
        wholesale_price = wholesale_index.get(article) if article else None

        def replace_price(price_match: re.Match) -> str:
            raw_price = price_match.group(1).strip()
            try:
                reason: str
                if wholesale_price is not None:
                    base_price = wholesale_price
                    stats.wholesale_prices += 1

                    rule = select_rule(
                        pricing.rules_by_category.get(category_id, ()),
                        base_price,
                    )
                    if rule is None:
                        coefficient = pricing.coef_uncategorized
                        stats.no_category_rules += 1
                        reason = "no_category_rule"
                    else:
                        coefficient = rule.coefficient
                        stats.category_rules += 1
                        reason = ""
                else:
                    base_price = parse_decimal(raw_price)
                    if currency_id != "UAH":
                        rate = currency_rates.get(currency_id)
                        if rate is None:
                            print(
                                f"Currency rate for {currency_id} not found, "
                                f"offer {offer_id}: price left unconverted"
                            )
                        else:
                            base_price *= rate
                            stats.converted_prices += 1

                    coefficient = pricing.coef_no_base
                    stats.xml_fallback_prices += 1
                    reason = "xml_fallback"

                new_price = ceil_uah(base_price * coefficient)

                if reason:
                    log.info(
                        "article=%-12s  offer_id=%-14s  base=%-8s  coef=%s  price=%-8s  reason=%s",
                        article or "—",
                        offer_id,
                        base_price,
                        coefficient,
                        new_price,
                        reason,
                    )

                return f"<price>{new_price}</price>"
            except Exception:
                return price_match.group(0)

        new_body = re.sub(r"<price>(.*?)</price>", replace_price, body)
        new_body = re.sub(
            r"<currencyId>[^<]+</currencyId>",
            "<currencyId>UAH</currencyId>",
            new_body,
        )
        return f'<offer id="{offer_id}"{tail_attrs}>{new_body}</offer>'

    updated_xml = re.sub(
        r'<offer\s+id="(\d+)"([^>]*)>(.*?)</offer>',
        on_offer,
        xml,
        flags=re.DOTALL,
    )

    print(f"Kasta offers: {stats.offers}")
    print(
        "Kasta coefficients: "
        f"category_rules={stats.category_rules} | "
        f"xml_fallback={stats.xml_fallback_prices} | "
        f"no_category_rules={stats.no_category_rules}"
    )
    if stats.converted_prices:
        print(f"Kasta currency conversions: {stats.converted_prices}")

    return updated_xml
