"""
Допоміжні класи для роботи з характеристиками та ключовими словами.
"""

from typing import List, Optional, Dict, Set


class SpecAccessor:
    """Строгий доступ до характеристик без хибних співпадінь"""

    def __init__(self, specs: List[Dict[str, str]]):
        """
        Args:
            specs: Список характеристик товару
        """
        self._map = {
            s["name"].strip().lower(): s
            for s in specs
            if s.get("name")
        }

    def get(self, name: str) -> Optional[Dict]:
        """Отримати характеристику за назвою"""
        return self._map.get(name.lower())

    def value(self, name: str) -> Optional[str]:
        """Отримати значення характеристики"""
        s = self.get(name)
        return s["value"] if s else None

    def unit(self, name: str) -> Optional[str]:
        """Отримати одиницю виміру характеристики"""
        s = self.get(name)
        return s.get("unit") if s else None


class KeywordBucket:
    """Контейнер для ключових слів з автоматичною дедуплікацією та лімітами"""

    def __init__(self, limit: int):
        """
        Args:
            limit: Максимальна кількість ключових слів
        """
        self.limit = limit
        self.items: List[str] = []
        self.seen: Set[str] = set()

    def add(self, value: Optional[str]) -> None:
        """Додати одне ключове слово"""
        if not value:
            return

        v = value.strip().lower()
        if v and v not in self.seen and len(self.items) < self.limit:
            self.seen.add(v)
            self.items.append(value.strip())

    def extend(self, values: List[str]) -> None:
        """Додати декілька ключових слів"""
        for v in values:
            self.add(v)

    def to_list(self) -> List[str]:
        """Отримати список ключових слів"""
        return self.items
