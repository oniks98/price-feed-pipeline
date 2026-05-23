"""
Shared primitives for market-specific pricing modules.

Each market module (kasta.py, rozetka.py, …) imports from here.
Nothing in this file is market-specific.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation, ROUND_CEILING
from typing import Iterable


# ---------------------------------------------------------------------------
# Data structures
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class PriceRule:
    """Single price bracket: [price_from, price_to) → coefficient."""

    category_id: str
    price_from: Decimal
    price_to: Decimal
    royalty_percent: Decimal
    coefficient: Decimal

    def contains(self, price: Decimal) -> bool:
        return self.price_from <= price < self.price_to


@dataclass
class PricingStats:
    """Counters accumulated during a single feed generation run."""

    offers: int = 0
    wholesale_prices: int = 0
    xml_fallback_prices: int = 0
    converted_prices: int = 0
    category_rules: int = 0
    no_category_rules: int = 0


# ---------------------------------------------------------------------------
# Decimal helpers
# ---------------------------------------------------------------------------

def parse_decimal(value: object, default: Decimal | None = None) -> Decimal:
    """Parse a decimal from a CSV/XML value with graceful fallback."""
    if value is None or value == "":
        if default is not None:
            return default
        raise InvalidOperation("empty decimal")

    text = str(value).replace("\xa0", "").replace(" ", "").replace(",", ".").strip()
    if not text:
        if default is not None:
            return default
        raise InvalidOperation("empty decimal")

    return Decimal(text)


def ceil_uah(value: Decimal) -> Decimal:
    """Round up to the nearest whole hryvnia."""
    return value.quantize(Decimal("1"), rounding=ROUND_CEILING)


# ---------------------------------------------------------------------------
# XML helpers
# ---------------------------------------------------------------------------

def tag_text(body: str, tag_name: str) -> str | None:
    """Extract inner text of the first matching XML tag, or None."""
    match = re.search(rf"<{tag_name}>(.*?)</{tag_name}>", body, flags=re.DOTALL)
    if not match:
        return None
    return match.group(1).strip()


# ---------------------------------------------------------------------------
# Rule selection
# ---------------------------------------------------------------------------

def rule_sort_key(rule: PriceRule) -> tuple[Decimal, Decimal, Decimal]:
    width = rule.price_to - rule.price_from
    return rule.price_from, width, rule.royalty_percent


def select_rule(rules: Iterable[PriceRule], base_price: Decimal) -> PriceRule | None:
    """
    Pick the best PriceRule for a given base price.

    Strategy:
    1. Compute the sale price each rule would produce and find rules whose
       bracket contains that sale price (self-consistent match).
    2. If none self-consistent, fall back to the rule whose bracket contains
       the raw base price (handles edge cases near bracket boundaries).
    3. Return None if no rule matches at all → caller uses the market default.
    """
    rules_tuple = tuple(rules)
    if not rules_tuple:
        return None

    sale_price_matches: list[PriceRule] = []
    for rule in rules_tuple:
        sale_price = ceil_uah(base_price * rule.coefficient)
        if rule.contains(sale_price):
            sale_price_matches.append(rule)

    if sale_price_matches:
        return sorted(sale_price_matches, key=rule_sort_key)[0]

    for rule in rules_tuple:
        if rule.contains(base_price):
            return rule

    return None
