"""
Facade / router for market-specific pricing.

Public API consumed by generate_*_feed.py scripts:
    apply_market_prices(market, xml, wholesale_index, currency_rates) -> str
    get_market_default_coefficient(market) -> Decimal

Adding a new market:
    1. Create scripts/services/pricing_rules/<market>.py
       with apply_prices() and get_default_coefficient().
    2. Import it below and add it to _APPLY_ROUTERS / _COEF_ROUTERS.
"""

from __future__ import annotations

from decimal import Decimal
from typing import Callable, Final

from .pricing_rules import epicenter as _epicenter
from .pricing_rules import kasta as _kasta

# ---------------------------------------------------------------------------
# Registry — add new markets here
# ---------------------------------------------------------------------------

_ApplyFn = Callable[[str, dict[str, Decimal], dict[str, Decimal]], str]
_CoefFn = Callable[[], Decimal]

_APPLY_ROUTERS: Final[dict[str, _ApplyFn]] = {
    "kasta":     _kasta.apply_prices,
    "epicenter": _epicenter.apply_prices,
}

_COEF_ROUTERS: Final[dict[str, _CoefFn]] = {
    "kasta":     _kasta.get_default_coefficient,
    "epicenter": _epicenter.get_default_coefficient,
}


def _assert_supported(market: str) -> None:
    if market not in _APPLY_ROUTERS:
        supported = sorted(_APPLY_ROUTERS)
        raise ValueError(f"Unsupported market: {market!r}. Supported: {supported}")


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def apply_market_prices(
    market: str,
    xml: str,
    wholesale_index: dict[str, Decimal],
    currency_rates: dict[str, Decimal],
) -> str:
    _assert_supported(market)
    return _APPLY_ROUTERS[market](xml, wholesale_index, currency_rates)


def get_market_default_coefficient(market: str) -> Decimal:
    _assert_supported(market)
    return _COEF_ROUTERS[market]()
