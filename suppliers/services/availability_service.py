"""
Сервіс для перевірки наявності товарів.
Визначає чи товар в наявності на основі ключових слів.
"""
from typing import Set


class AvailabilityService:
    """Сервіс для перевірки наявності товарів"""
    
    # Ключові слова, що вказують на відсутність товару
    OUT_OF_STOCK_KEYWORDS: Set[str] = {
        "немає",
        "немає в наявності",
        "нет в наличии",
        "нет на складе",
        "відсутній",
        "відсутня",
        "закінчився",
        "закінчилась",
        "out of stock",
        "unavailable",
        "под заказ",
        "під замовлення",
    }
    
    def __init__(self, logger=None):
        """
        Args:
            logger: Scrapy logger (опціонально)
        """
        self.logger = logger
    
    def is_available(self, availability_str: str) -> bool:
        """
        Перевіряє чи товар в наявності
        
        Args:
            availability_str: Рядок з інформацією про наявність
        
        Returns:
            True якщо товар в наявності або статус невизначений,
            False якщо товар відсутній
        """
        if not availability_str:
            return True
        
        availability_lower = str(availability_str).lower().strip()
        
        for keyword in self.OUT_OF_STOCK_KEYWORDS:
            if keyword in availability_lower:
                if self.logger:
                    self.logger.debug(f"❌ Товар не в наявності: '{availability_str}' (знайдено '{keyword}')")
                return False
        
        return True
    
    def normalize_availability(self, availability_str: str) -> tuple[str, str]:
        """
        Нормалізує статус наявності для PROM формату
        
        Args:
            availability_str: Оригінальний рядок наявності
        
        Returns:
            Tuple (availability, quantity):
            - availability: "+" якщо в наявності
            - quantity: кількість або "10000" за замовчуванням
        
        Examples:
            "В наличии" → ("+", "10000")
            "В наличии 5 шт" → ("+", "5")
        """
        if not self.is_available(availability_str):
            return ("", "")
        
        # За замовчуванням
        availability = "+"
        quantity = "10000"
        
        # Спроба витягнути кількість з рядка типу "В наличии 5 шт"
        import re
        match = re.search(r'(\d+)\s*(?:шт|штук|pcs)', str(availability_str), re.IGNORECASE)
        if match:
            quantity = match.group(1)
        
        return (availability, quantity)
