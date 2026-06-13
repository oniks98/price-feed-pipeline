"""
Фасад / маршрутизатор ціноутворення для конкретних майданчиків.

Публічне API, яке використовують скрипти generate_*_feed.py:
    apply_market_prices(market, xml, wholesale_index, currency_rates) -> str
    get_market_default_coefficient(market) -> Decimal

Додавання нового майданчика:
    1. Створити scripts/services/pricing_rules/<market>.py
       з функціями apply_prices() та get_default_coefficient().
    2. Імпортувати нижче та додати до _APPLY_ROUTERS / _COEF_ROUTERS.
"""

from __future__ import annotations

from decimal import Decimal
from typing import Callable, Final

from .pricing_rules import epicenter as _epicenter
from .pricing_rules import kasta as _kasta
from .pricing_rules import rozetka as _rozetka

# ---------------------------------------------------------------------------
# Реєстр — додавайте нові майданчики тут
# ---------------------------------------------------------------------------

_ApplyFn = Callable[[str, dict[str, Decimal], dict[str, Decimal]], str]
_CoefFn = Callable[[], Decimal]

_APPLY_ROUTERS: Final[dict[str, _ApplyFn]] = {
    "kasta":     _kasta.apply_prices,
    "epicenter": _epicenter.apply_prices,
    "rozetka":   _rozetka.apply_prices,
}

_COEF_ROUTERS: Final[dict[str, _CoefFn]] = {
    "kasta":     _kasta.get_default_coefficient,
    "epicenter": _epicenter.get_default_coefficient,
    "rozetka":   _rozetka.get_default_coefficient,
}


def _assert_supported(market: str) -> None:
    if market not in _APPLY_ROUTERS:
        supported = sorted(_APPLY_ROUTERS)
        raise ValueError(f"Unsupported market: {market!r}. Supported: {supported}")


# ---------------------------------------------------------------------------
# Публічне API
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
