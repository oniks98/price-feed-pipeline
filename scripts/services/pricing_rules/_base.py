"""
Shared primitives for market-specific pricing modules.

Each market module (kasta.py, rozetka.py, epicenter.py) imports from here.
Nothing in this file is market-specific.
"""

from __future__ import annotations

import logging
import re
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation, ROUND_CEILING
from typing import Final, Iterable


# ---------------------------------------------------------------------------
# Data structures
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class ArticlePrices:
    """
    Роздрібна (retail) і оптова/дилерська (dealer) ціна постачальника для одного артикулу,
    разом з ідентифікатором постачальника (потрібен щоб обрати правильний coef_{supplier}
    з SupplierCoefficients — див. нижче).

    Джерело: стовпці «Ціна» (retail) і «Оптова_ціна» (dealer) у {supplier}_old.csv
    (generate_utils_feed.py::load_article_price_index).

    retail   — РРЦ постачальника (може бути 0, якщо постачальник її не вказав).
    dealer   — дилерська/оптова ціна (завжди > 0 — рядки без неї не потрапляють в індекс).
    supplier — постачальник ("viatec" / "secur" / "lp"), визначається файлом-джерелом
               рядка. Код_товару унікальний для кожного постачальника (діапазони не
               перетинаються — suppliers/constants.py::SUPPLIER_CODE_RANGES), тому
               конфліктів між постачальниками для одного коду не буває.
    """

    retail: Decimal
    dealer: Decimal
    supplier: str


# ---------------------------------------------------------------------------
# Per-supplier manual coefficients (coef_viatec / coef_secur / coef_lp)
# ---------------------------------------------------------------------------

SUPPLIERS: Final[tuple[str, ...]] = ("viatec", "secur", "lp")

_COEF_COLUMNS: Final[dict[str, str]] = {
    "viatec": "coef_viatec",
    "secur": "coef_secur",
    "lp": "coef_lp",
}


@dataclass(frozen=True)
class SupplierCoefficients:
    """
    Ручний коефіцієнт coef, окремо для кожного постачальника (viatec/secur/lp).

    Джерело: стовпці coef_viatec / coef_secur / coef_lp у {market}_coefficients.csv
    (замінюють колишню єдину колонку coef).

    None для конкретного постачальника → ще не заповнено вручну в CSV для цього
    правила + цього постачальника. Використання такого поєднання для розрахунку
    ціни офера цього постачальника — критична помилка (перевіряється у apply_prices
    кожного market-модуля, відповідно до article_prices.supplier).
    """

    viatec: Decimal | None
    secur: Decimal | None
    lp: Decimal | None

    def get(self, supplier: str) -> Decimal | None:
        """Повертає coef для конкретного постачальника ("viatec"/"secur"/"lp")."""
        if supplier not in SUPPLIERS:
            raise ValueError(f"Unknown supplier: {supplier!r}. Expected one of {SUPPLIERS}")
        return getattr(self, supplier)

    @property
    def filled_suppliers(self) -> tuple[str, ...]:
        """Постачальники, для яких coef вже заповнено вручну."""
        return tuple(s for s in SUPPLIERS if getattr(self, s) is not None)


def parse_supplier_coefficients(row: dict[str, str], *, context: str = "") -> SupplierCoefficients:
    """
    Читає coef_viatec / coef_secur / coef_lp з рядка CSV (csv.DictReader).

    Некоректне значення (не парситься як Decimal) трактується як незаповнене,
    з попередженням у консоль (context — короткий опис рядка для повідомлення,
    напр. "Kasta: category=518").
    """
    values: dict[str, Decimal | None] = {}
    for supplier, column in _COEF_COLUMNS.items():
        raw = (row.get(column) or "").strip()
        value: Decimal | None = None
        if raw:
            try:
                value = parse_decimal(raw)
            except InvalidOperation:
                print(
                    f"⚠️  {context}: некоректний {column}={raw!r} — трактується як незаповнений"
                )
        values[supplier] = value
    return SupplierCoefficients(**values)


@dataclass(frozen=True)
class PriceRule:
    """
    Одне цінове правило-діапазон: [price_from, price_to) для категорії.

    threshold — авторахований коефіцієнт з формули роялті (market_formula_coef.py).
                Використовується (а) для самоузгодженого вибору діапазону
                (rule_sort_key/select_rule) і (б) як нижня межа ціни каналу
                у resolve_channel_price.
    coef      — РУЧНІ коефіцієнти окремо для кожного постачальника
                (SupplierCoefficients). Конкретне значення для офера обирається
                через coef.get(article_prices.supplier). None для потрібного
                постачальника — використання такого правила для розрахунку ціни
                офера цього постачальника є критичною помилкою (пор. apply_prices).
    """

    category_id: str
    price_from: Decimal
    price_to: Decimal
    royalty_percent: Decimal
    threshold: Decimal
    coef: SupplierCoefficients

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
    missing_manual_coef: int = 0
    price_exceptions: int = 0   # непередбачені винятки в replace_price — див. apply_prices


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
    Вибрати найкраще правило `PriceRule` для заданої базової ціни (дилерської / гуртової ціни).

Вибір діапазону виконується ВИКЛЮЧНО за полем `threshold` (незалежно від того,
 чи заповнений вручну `coef`) — це зберігає точні межі діапазонів,
   які існували до розділення `coef` і `threshold`.

Стратегія:

1. Обчислити ціну продажу, яку дасть кожне правило (`ceil(base_price * threshold)`),
 і знайти правила, діапазон яких містить цю ціну продажу (самоузгоджений збіг).
2. Якщо самоузгоджених збігів немає — використати правило, діапазон якого 
містить вихідну базову ціну (для обробки крайових випадків біля меж діапазонів).
3. Повернути `None`, якщо не підійшло жодне правило → тоді викликаючий 
код використовує ринкове значення за замовчуванням.

    """
    rules_tuple = tuple(rules)
    if not rules_tuple:
        return None

    sale_price_matches: list[PriceRule] = []
    for rule in rules_tuple:
        sale_price = ceil_uah(base_price * rule.threshold)
        if rule.contains(sale_price):
            sale_price_matches.append(rule)

    if sale_price_matches:
        return sorted(sale_price_matches, key=rule_sort_key)[0]

    for rule in rules_tuple:
        if rule.contains(base_price):
            return rule

    return None


# ---------------------------------------------------------------------------
# Channel price formula
# ---------------------------------------------------------------------------

def resolve_channel_price(
    retail: Decimal,
    dealer: Decimal,
    coef: Decimal,
    threshold: Decimal,
    *,
    logger: logging.Logger | None = None,
    product_name: str = "",
) -> Decimal:
    """
    Ціна каналу маркетплейсу — формула, ІДЕНТИЧНА
    suppliers/services/dealer_price_service.py::DealerPriceService.channel_price.

    Дублюється тут навмисно, а не імпортується: scripts/ і suppliers/ — окремі
    точки входу (їх скрипти запускаються з різним sys.path[0]), тому прямий
    імпорт між пакетами був би неявним і крихким (працював би лише випадково,
    залежно від CWD/PYTHONPATH). Будь-яка зміна формули має синхронно
    повторюватись в обох місцях.

    Формула:
        X    = retail / dealer * coef
        Ціна = dealer * X          якщо X > threshold  (= retail * coef)
        Ціна = dealer * threshold  якщо X <= threshold
        Еквівалентно: Ціна = max(retail * coef, dealer * threshold)

    Особливий випадок (помилка постачальника): retail < dealer → swap + warning.
    Fallback: retail <= 0 або dealer <= 0 → Ціна = dealer * threshold.
    """
    if retail <= 0 or dealer <= 0:
        return dealer * threshold

    if retail < dealer:
        if logger is not None:
            logger.warning(
                "retail < dealer — постачальник переплутав ціни: "
                "retail=%s dealer=%s | %s",
                retail, dealer, product_name or "—",
            )
        retail, dealer = dealer, retail

    return max(retail * coef, dealer * threshold)
