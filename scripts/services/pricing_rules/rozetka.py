"""
Rozetka pricing rules.

CSV schema (semicolon-delimited, utf-8-sig):
  A  prom_category_id
  B  prom_category_name
  C  rozetka_category_id
  D  rozetka_category_name
  E  matched_royalty_id
  F  matched_royalty_name
  G  match_level
  H  brand              — "" = any brand; non-empty = brand-specific override
  I  royalty_percent
  J  price_from
  K  price_to
  L  coef
  M  coef_uncategorized — wholesale exists, no category rule found
  N  coef_no_base       — no wholesale price → XML price as base

Rule priority (highest → lowest):
  1. brand matches  AND  price range matches  — brand + range
  2. brand matches, no price range            — brand-only
  3. no brand AND price range matches         — range-only
  4. no brand AND no price range              — category default

For range tiers (1 and 3) the best candidate is chosen via sale-price
self-consistency (same strategy as kasta.py): compute sale_price = ceil(base * coef)
and prefer rules whose bracket contains that sale_price. Falls back to
checking whether the bracket contains base_price directly.
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
    PricingStats,
    ceil_uah,
    parse_decimal,
    tag_text,
)

# ---------------------------------------------------------------------------
# Paths & constants
# ---------------------------------------------------------------------------

_ROOT: Final[Path] = Path(__file__).resolve().parents[3]
COEFFICIENTS_PATH: Final[Path] = _ROOT / "data" / "markets" / "rozetka_coefficients.csv"
DEFAULT_LOG_PATH: Final[Path] = _ROOT / "rozetka_default_id.log"
_CSV_DELIMITER: Final[str] = ";"
_CSV_ENCODING: Final[str] = "utf-8-sig"

# Tier constants — lower value = higher priority
_TIER_BRAND_RANGE: Final[int] = 0
_TIER_BRAND_ONLY: Final[int] = 1
_TIER_RANGE_ONLY: Final[int] = 2
_TIER_CATEGORY_DEFAULT: Final[int] = 3


# ---------------------------------------------------------------------------
# Data structures
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class RozetkaRule:
    """
    Single pricing entry from rozetka_coefficients.csv.

    brand:       "" → applies to any brand.
    price_from:  Decimal("0")        → no lower bound.
    price_to:    Decimal("Infinity") → no upper bound.
    """

    category_id: str
    brand: str
    price_from: Decimal
    price_to: Decimal
    royalty_percent: Decimal
    coefficient: Decimal

    @property
    def has_brand(self) -> bool:
        return bool(self.brand)

    @property
    def has_range(self) -> bool:
        return self.price_from > Decimal("0") or self.price_to < Decimal("Infinity")

    def range_contains(self, price: Decimal) -> bool:
        return self.price_from <= price < self.price_to

    @property
    def range_width(self) -> Decimal:
        return self.price_to - self.price_from


@dataclass(frozen=True)
class RozetkaPricingTable:
    coef_uncategorized: Decimal                         # wholesale exists, no rule match
    coef_no_base: Decimal                               # no wholesale price → xml_fallback
    rules_by_category: dict[str, tuple[RozetkaRule, ...]]


# ---------------------------------------------------------------------------
# Logger
# ---------------------------------------------------------------------------

def _build_logger() -> logging.Logger:
    """File logger that overwrites on each run so it always reflects the current feed."""
    logger = logging.getLogger("rozetka.default_offers")
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

def _tier(rule: RozetkaRule) -> int:
    if rule.has_brand and rule.has_range:
        return _TIER_BRAND_RANGE
    if rule.has_brand:
        return _TIER_BRAND_ONLY
    if rule.has_range:
        return _TIER_RANGE_ONLY
    return _TIER_CATEGORY_DEFAULT


def _sort_key(rule: RozetkaRule) -> tuple[int, Decimal]:
    """Sort by tier (asc), then by range width (asc → narrowest first)."""
    return _tier(rule), rule.range_width


@lru_cache(maxsize=1)
def _load_pricing() -> RozetkaPricingTable:
    if not COEFFICIENTS_PATH.exists():
        raise FileNotFoundError(f"Rozetka coefficients not found: {COEFFICIENTS_PATH}")

    coef_uncategorized: Decimal | None = None
    coef_no_base: Decimal | None = None
    rules_by_category: dict[str, list[RozetkaRule]] = {}

    with COEFFICIENTS_PATH.open(encoding=_CSV_ENCODING, errors="replace", newline="") as f:
        reader = csv.DictReader(f, delimiter=_CSV_DELIMITER)
        for row in reader:
            # Global defaults — stored on first non-empty occurrence
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

            brand = (row.get("brand") or "").strip()

            try:
                rule = RozetkaRule(
                    category_id=category_id,
                    brand=brand,
                    price_from=parse_decimal(row.get("price_from"), Decimal("0")),
                    price_to=parse_decimal(row.get("price_to"), Decimal("Infinity")),
                    royalty_percent=parse_decimal(row.get("royalty_percent"), Decimal("0")),
                    coefficient=parse_decimal(raw_coef),
                )
            except InvalidOperation:
                continue

            rules_by_category.setdefault(category_id, []).append(rule)

    if coef_uncategorized is None:
        raise ValueError("coef_uncategorized missing in rozetka_coefficients.csv (column M)")
    if coef_no_base is None:
        raise ValueError("coef_no_base missing in rozetka_coefficients.csv (column N)")

    frozen_rules: dict[str, tuple[RozetkaRule, ...]] = {
        cid: tuple(sorted(rules, key=_sort_key))
        for cid, rules in rules_by_category.items()
    }

    total = sum(len(v) for v in frozen_rules.values())
    brand_count = sum(1 for rules in frozen_rules.values() for r in rules if r.has_brand)
    print(
        f"Rozetka pricing: loaded {total} rules "
        f"({brand_count} brand-specific, {total - brand_count} generic), "
        f"coef_uncategorized={coef_uncategorized}, coef_no_base={coef_no_base}"
    )
    return RozetkaPricingTable(coef_uncategorized, coef_no_base, frozen_rules)


# ---------------------------------------------------------------------------
# Rule selection
# ---------------------------------------------------------------------------

def _best_range_match(
    candidates: list[RozetkaRule],
    base_price: Decimal,
) -> RozetkaRule | None:
    """
    From a list of range-bearing rules, return the best match.

    Preference order:
      1. Self-consistent: bracket contains ceil(base * coef)  → smallest width wins.
      2. Fallback: bracket contains base_price               → smallest width wins.
      3. None if no bracket matches.
    """
    self_consistent = [
        r for r in candidates
        if r.range_contains(ceil_uah(base_price * r.coefficient))
    ]
    if self_consistent:
        return min(self_consistent, key=lambda r: r.range_width)

    base_matches = [r for r in candidates if r.range_contains(base_price)]
    if base_matches:
        return min(base_matches, key=lambda r: r.range_width)

    return None


def _select_rule(
    rules: tuple[RozetkaRule, ...],
    base_price: Decimal,
    brand: str,
) -> RozetkaRule | None:
    """
    Pick the best RozetkaRule for a given price and vendor brand.

    Priority (1 = highest):
      1. brand matches  AND  range matches  (brand+range)
      2. brand matches, no range            (brand-only)
      3. no brand AND range matches         (range-only)
      4. no brand AND no range              (category default)
    """
    if not rules:
        return None

    brand_norm = brand.strip().lower()

    brand_range: list[RozetkaRule] = []
    brand_only: list[RozetkaRule] = []
    range_only: list[RozetkaRule] = []
    category_default: list[RozetkaRule] = []

    for rule in rules:
        is_brand_match = rule.has_brand and rule.brand.strip().lower() == brand_norm

        if rule.has_brand and rule.has_range:
            if is_brand_match:
                brand_range.append(rule)
        elif rule.has_brand:
            if is_brand_match:
                brand_only.append(rule)
        elif rule.has_range:
            range_only.append(rule)
        else:
            category_default.append(rule)

    # Tier 1 — brand + range (self-consistent preferred)
    if brand_range:
        match = _best_range_match(brand_range, base_price)
        if match:
            return match

    # Tier 2 — brand only (no range) — take narrowest / only entry
    if brand_only:
        return min(brand_only, key=lambda r: r.range_width)

    # Tier 3 — range only (self-consistent preferred)
    if range_only:
        match = _best_range_match(range_only, base_price)
        if match:
            return match

    # Tier 4 — category default
    if category_default:
        return category_default[0]

    return None


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
    brand_rule_hits: int = 0
    log = _build_logger()

    def on_offer(match: re.Match) -> str:
        nonlocal brand_rule_hits

        offer_id: str = match.group(1)
        tail_attrs: str = match.group(2)
        body: str = match.group(3)
        stats.offers += 1

        category_id = tag_text(body, "categoryId") or ""
        article = tag_text(body, "article")
        vendor = (tag_text(body, "vendor") or "").strip()
        currency_id = (tag_text(body, "currencyId") or "UAH").upper()
        wholesale_price = wholesale_index.get(article) if article else None

        def replace_price(price_match: re.Match) -> str:
            nonlocal brand_rule_hits

            raw_price = price_match.group(1).strip()
            try:
                reason: str
                if wholesale_price is not None:
                    base_price = wholesale_price
                    stats.wholesale_prices += 1

                    rule = _select_rule(
                        pricing.rules_by_category.get(category_id, ()),
                        base_price,
                        vendor,
                    )
                    if rule is None:
                        coefficient = pricing.coef_uncategorized
                        stats.no_category_rules += 1
                        reason = "no_category_rule"
                    else:
                        coefficient = rule.coefficient
                        stats.category_rules += 1
                        if rule.has_brand:
                            brand_rule_hits += 1
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
                        "article=%-12s  offer_id=%-14s  base=%-8s  coef=%s  "
                        "price=%-8s  vendor=%-18s  reason=%s",
                        article or "—",
                        offer_id,
                        base_price,
                        coefficient,
                        new_price,
                        vendor or "—",
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

    print(f"Rozetka offers: {stats.offers}")
    print(
        "Rozetka coefficients: "
        f"category_rules={stats.category_rules} (brand={brand_rule_hits}) | "
        f"xml_fallback={stats.xml_fallback_prices} | "
        f"no_category_rules={stats.no_category_rules}"
    )
    if stats.converted_prices:
        print(f"Rozetka currency conversions: {stats.converted_prices}")

    if stats.no_category_rules:
        # Prom автоматично перемістив товари у нові категорії без правил.
        # Фід згенеровано з coef_uncategorized — ціни некоректні.
        # Потрібно додати правила у rozetka_coefficients.csv.
        # Деталі у rozetka_default_id.log
        raise SystemExit(
            f"❌ Rozetka: {stats.no_category_rules} товарів без правил категорії "
            f"(no_category_rules). Додайте правила у rozetka_coefficients.csv. "
            f"Деталі: rozetka_default_id.log"
        )

    return updated_xml
