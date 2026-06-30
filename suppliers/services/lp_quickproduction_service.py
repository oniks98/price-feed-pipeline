"""
Сервіс визначення наявності / кількості для товарів LogicPower B2B API.

Проблема:
  LP API розрізняє два дозволених статуси:
    - inStock         → товар є на складі
    - quickProduction → товар буде виготовлено, типово за 7 днів

  Prom.ua формат колонки «Наявність»:
    - "+"  → в наявності
    - ""   → немає
    - "7"  → Під замовлення, 7 днів   ← потрібно для quickProduction

Рішення:
  Spider викликає LpQuickProductionService.resolve(status, avail_symbol)
  і передає результат у Scrapy item безпосередньо.

  Pipeline (AvailabilityService.normalize_availability) потім:
    - "В наявності" → ("+", "10000") — звичайний inStock
    - "7"           → ("7", "")      — числовий fast-path (дні)

  Таким чином pipeline залишається supplier-agnostic.
"""
from __future__ import annotations

__all__ = ["LpQuickProductionService"]


class LpQuickProductionService:
    """
    Prom.ua-сумісні значення (Наявність, Кількість) для LP API товарів.

    Використання в spider (_build_item):
        avail_val, qty_val = LpQuickProductionService.resolve(
            status, product.get("availability")
        )
        item["Наявність"] = avail_val
        item["Кількість"] = qty_val
    """

    # Кількість днів виробництва для quickProduction.
    # Prom.ua: числовий рядок у «Наявність» → "Під замовлення, N днів".
    PRODUCTION_DAYS: int = 7

    # LP API: символ наявності на складі → орієнтовна кількість одиниць.
    # Символи: +/- (мало), + (є), ++ (достатньо), +++ (багато).
    AVAILABILITY_QTY: dict[str | None, str] = {
        "+/-": "1000",
        "+":   "1000",
        "++":  "1000",
        "+++": "1000",
    }

    # Текстова мітка "в наявності" для inStock.
    # AvailabilityService.normalize_availability() перетворить її у "+".
    _IN_STOCK_LABEL: str = "В наявності"

    @classmethod
    def resolve(
        cls,
        status: str,
        avail_symbol: str | None,
    ) -> tuple[str, str]:
        """
        Повертає (Наявність, Кількість) для запису в Scrapy item.

        Args:
            status:       product.status з LP API ("inStock" / "quickProduction")
            avail_symbol: product.availability з LP API ("+", "++", "+/-", "+++", None)

        Returns:
            Tuple[avail, qty] готовий до запису в item:
              quickProduction → ("7", "")
                  → pipeline normalize_availability("7")  → ("7", "")
                  → Prom.ua: "Під замовлення, 7 днів"
              inStock → ("В наявності", "10")
                  → pipeline normalize_availability("В наявності") → ("+", "10000")
                  → qty "10" перезаписується зі spider_qty (пріоритет pipeline)
                  → Prom.ua: в наявності, кількість = qty

        Note:
            Кількість для quickProduction завжди "", бо Prom.ua використовує
            поле «Наявність» (дні) як єдину ознаку статусу "під замовлення".
        """
        if status == "quickProduction":
            return (str(cls.PRODUCTION_DAYS), "")
        qty = cls.AVAILABILITY_QTY.get(avail_symbol, "10")
        return (cls._IN_STOCK_LABEL, qty)
