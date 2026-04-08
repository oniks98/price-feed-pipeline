"""
Базовий клас для процесорів категорій.
"""

from abc import ABC, abstractmethod
from typing import List, Dict
import logging

from keywords.core.models import Spec
from keywords.core.loaders import CategoryConfig


class BaseProcessor(ABC):
    """Базовий процесор для генерації ключових слів"""

    @abstractmethod
    def generate(
        self,
        name: str,
        config: CategoryConfig,
        specs: List[Spec],
        lang: str,
        manufacturers: Dict[str, str],
        logger: logging.Logger
    ) -> List[str]:
        """
        Генерація ключових слів.

        Args:
            name: Назва товару
            config: Конфігурація категорії
            specs: Список характеристик
            lang: Мова (ru/ua)
            manufacturers: Словник виробників
            logger: Логгер

        Returns:
            Список ключових слів
        """
        pass
