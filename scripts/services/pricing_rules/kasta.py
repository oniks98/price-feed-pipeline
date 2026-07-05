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
  I  threshold           — авторахований коефіцієнт з формули роялті (market_formula_coef.py)
  J  coef_uncategorized  — wholesale price exists, but no category rule found
  K  coef_no_base        — no wholesale price → XML price used as base
  L  coef_viatec         — РУЧНИЙ коефіцієнт діапазону для постачальника viatec.
  M  coef_secur          — РУЧНИЙ коефіцієнт діапазону для постачальника secur.
  N  coef_lp             — РУЧНИЙ коефіцієнт діапазону для постачальника lp.
                            Якщо для діапазону є threshold, але coef_{supplier}
                            порожній для постачальника конкретного товару — це
                            КРИТИЧНА помилка (пор. apply_prices).

Ціна для офера з відомим category rule (threshold+coef знайдені для
price-бракету) рахується за формулою resolve_channel_price() з _base.py:
    Ціна = max(retail * coef, dealer * threshold)
де dealer = Оптова_ціна постачальника, retail = Ціна постачальника
(суплаєр *_old.csv). Вибір price-бракету (як і раніше) — за threshold,
незалежно від того, чи заповнений coef.
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
    ArticlePrices,
    PriceRule,
    PricingStats,
    SUPPLIERS,
    SupplierCoefficients,
    ceil_uah,
    parse_decimal,
    parse_supplier_coefficients,
    resolve_channel_price,
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
    filled_coef_counts: dict[str, int] = {s: 0 for s in SUPPLIERS}

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
            raw_threshold = (row.get("threshold") or "").strip()
            if not category_id or not raw_threshold:
                continue

            try:
                threshold = parse_decimal(raw_threshold)
            except InvalidOperation:
                continue

            coef = parse_supplier_coefficients(
                row,
                context=f"Kasta: category={category_id}",
            )
            for supplier in coef.filled_suppliers:
                filled_coef_counts[supplier] += 1

            try:
                rule = PriceRule(
                    category_id=category_id,
                    price_from=parse_decimal(row.get("price_from"), Decimal("0")),
                    price_to=parse_decimal(row.get("price_to"), Decimal("Infinity")),
                    royalty_percent=parse_decimal(row.get("royalty_percent")),
                    threshold=threshold,
                    coef=coef,
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
    filled_str = ", ".join(f"{s}={filled_coef_counts[s]}" for s in SUPPLIERS)
    print(
        f"Kasta pricing: loaded {total} rules (manual coef filled: {filled_str}), "
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
    price_index: dict[str, ArticlePrices],
    currency_rates: dict[str, Decimal],
) -> str:
    pricing = _load_pricing()
    stats = PricingStats()
    log = _build_logger()
    no_rule_offer_ids: list[str] = []
    missing_coef_brackets: dict[tuple[str, Decimal, Decimal, str], int] = {}

    def on_offer(match: re.Match) -> str:
        offer_id: str = match.group(1)
        tail_attrs: str = match.group(2)
        body: str = match.group(3)
        stats.offers += 1

        category_id = tag_text(body, "categoryId") or ""
        article = tag_text(body, "article")
        currency_id = (tag_text(body, "currencyId") or "UAH").upper()
        article_prices = price_index.get(article) if article else None

        def replace_price(price_match: re.Match) -> str:
            raw_price = price_match.group(1).strip()
            try:
                reason: str
                new_price: Decimal | None

                if article_prices is not None:
                    dealer = article_prices.dealer
                    retail = article_prices.retail
                    stats.wholesale_prices += 1

                    rule = select_rule(
                        pricing.rules_by_category.get(category_id, ()),
                        dealer,
                    )
                    if rule is None:
                        coefficient = pricing.coef_uncategorized
                        stats.no_category_rules += 1
                        reason = "no_category_rule"
                        no_rule_offer_ids.append(offer_id)
                        new_price = ceil_uah(dealer * coefficient)
                    else:
                        supplier = article_prices.supplier
                        supplier_coef = rule.coef.get(supplier)
                        if supplier_coef is None:
                            stats.missing_manual_coef += 1
                            key = (rule.category_id, rule.price_from, rule.price_to, supplier)
                            missing_coef_brackets[key] = missing_coef_brackets.get(key, 0) + 1
                            reason = ""
                            new_price = None
                        else:
                            stats.category_rules += 1
                            reason = ""
                            new_price = ceil_uah(
                                resolve_channel_price(
                                    retail=retail,
                                    dealer=dealer,
                                    coef=supplier_coef,
                                    threshold=rule.threshold,
                                    logger=log,
                                    product_name=article or offer_id,
                                )
                            )
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

                if new_price is None:
                    return price_match.group(0)

                if reason:
                    log.info(
                        "article=%-12s  offer_id=%-14s  base=%-8s  price=%-8s  reason=%s",
                        article or "—",
                        offer_id,
                        (article_prices.dealer if article_prices is not None else raw_price),
                        new_price,
                        reason,
                    )

                return f"<price>{new_price}</price>"
            except Exception as exc:
                stats.price_exceptions += 1
                log.warning(
                    "article=%-12s  offer_id=%-14s  price не змінено (виняток): %s: %s",
                    article or "—", offer_id, type(exc).__name__, exc,
                )
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
        + (f" | missing_manual_coef={stats.missing_manual_coef}" if stats.missing_manual_coef else "")
    )
    if stats.converted_prices:
        print(f"Kasta currency conversions: {stats.converted_prices}")
    if stats.price_exceptions:
        print(f"⚠️  Kasta: {stats.price_exceptions} оферів з винятком при розрахунку ціни (ціна з вхідного XML залишена без змін) — деталі: {DEFAULT_LOG_PATH.name}")

    errors: list[str] = []

    if stats.no_category_rules:
        # Prom автоматично перемістив товари у нові категорії без правил.
        # Фід згенеровано з coef_uncategorized — ціни некоректні.
        # Потрібно додати правила у kasta_coefficients.csv.
        # Деталі у kasta_default_id.log
        ids_str = ", ".join(no_rule_offer_ids)
        errors.append(
            f"{stats.no_category_rules} товарів без правил категорії (no_category_rules). "
            f"Додайте правила у kasta_coefficients.csv. Деталі: kasta_default_id.log\n"
            f"Offer IDs: {ids_str}"
        )

    if missing_coef_brackets:
        details = "\n".join(
            f"  category_id={cat_id}  price_from={pf}  price_to={pt}  supplier={supplier}  "
            f"({count} офферів)"
            for (cat_id, pf, pt, supplier), count in sorted(missing_coef_brackets.items())
        )
        errors.append(
            f"{stats.missing_manual_coef} офферів мають threshold, але ручний "
            f"coef_{{supplier}} не заповнений у kasta_coefficients.csv. Заповніть коефіцієнт для цих "
            f"діапазонів/постачальників перед генерацією фіду:\n{details}"
        )

    if errors:
        raise SystemExit("❌ Kasta:\n\n" + "\n\n".join(errors))

    return updated_xml
