"""
Головний генератор ключових слів (тонкий оркестратор).
"""

from typing import List, Dict, Optional
import logging

from keywords.core.models import Spec, MAX_TOTAL_KEYWORDS
from keywords.core.helpers import KeywordBucket
from keywords.core.loaders import ConfigLoader, CategoryConfig
from keywords.processors.viatec.generic import GenericProcessor as ViatecGenericProcessor
from keywords.processors.eserver.generic import GenericProcessor as EServerGenericProcessor
from keywords.processors.secur.generic import GenericProcessor as SecurGenericProcessor


class ProductKeywordsGenerator:
    """Генератор ключових слів для товарів (оркестратор)"""

    def __init__(
        self,
        keywords_csv_path: str,
        manufacturers_csv_path: str,
        supplier: str = "viatec",  # viatec, secur, eserver
        logger: Optional[logging.Logger] = None
    ):
        """
        Args:
            keywords_csv_path: Шлях до CSV з налаштуваннями категорій
            manufacturers_csv_path: Шлях до CSV з виробниками
            supplier: Назва постачальника (viatec, secur, eserver)
            logger: Опціональний логгер
        """
        self.logger = logger or logging.getLogger(__name__)
        self.supplier = supplier.lower()
        self.categories: Dict[str, List[CategoryConfig]] = {}
        self.manufacturers: Dict[str, str] = {}

        # Ініціалізуємо процесори для кожного постачальника
        self.processors = {
            "viatec": ViatecGenericProcessor(),
            "secur": SecurGenericProcessor(),
            "eserver": EServerGenericProcessor(),
        }

        # Завантажуємо конфігурацію
        self.categories = ConfigLoader.load_keywords_mapping(keywords_csv_path, self.logger)
        self.manufacturers = ConfigLoader.load_manufacturers(manufacturers_csv_path, self.logger)

    def generate_keywords(
        self,
        product_name: str,
        category_id: str,
        specs_list: Optional[List[Spec]] = None,
        lang: str = "ru",
    ) -> str:
        """
        Генерація ключових слів для товару.

        Args:
            product_name: Назва товару
            category_id: ID категорії
            specs_list: Список характеристик
            lang: Мова (ru/ua)

        Returns:
            Рядок з ключовими словами через кому
        """
        # Валідація вхідних даних
        if not isinstance(specs_list, list):
            specs_list = []
        if lang not in {"ru", "ua"}:
            lang = "ru"

        # Отримуємо конфігурацію категорії
        configs = self.categories.get(category_id)
        if not configs:
            self.logger.warning(f"No config for category {category_id}")
            return ""

        # Вибираємо конфігурацію на основі "Тип устройства" (може повернути None)
        config = self._select_config(configs, specs_list)
        if config is None:
            return ""

        # Отримуємо процесор для цього постачальника
        processor = self.processors.get(self.supplier)
        if not processor:
            self.logger.warning(f"No processor for supplier {self.supplier}")
            return ""

        # Генеруємо ключові слова через процесор
        keywords = processor.generate(
            name=product_name,
            config=config,
            specs=specs_list,
            lang=lang,
            manufacturers=self.manufacturers,
            logger=self.logger
        )

        # Дедуплікація і фінальне об'єднання
        return self._merge_keywords(keywords)

    @staticmethod
    def _merge_keywords(keywords: List[str]) -> str:
        """Об'єднання ключових слів з дедуплікацією"""
        bucket = KeywordBucket(MAX_TOTAL_KEYWORDS)
        bucket.extend(keywords)
        return ", ".join(bucket.to_list())

    @staticmethod
    def _select_config(
        configs: List[CategoryConfig],
        specs_list: List[Spec]
    ) -> Optional[CategoryConfig]:
        """
        Вибір конфігурації на основі характеристики "Тип устройства".

        Логіка:
          - Якщо всі рядки категорії мають порожній device_type → загальна категорія,
            повертаємо configs[0].
          - Якщо хоча б один рядок має заповнений device_type → специфічна категорія:
            шукаємо точний збіг (case-insensitive) з характеристикою товару.
            Не знайшли збіг → повертаємо None (порожній блок 3 = сигнал про помилку в CSV).
        """
        is_specific = any(c.device_type for c in configs)

        if not is_specific:
            # Загальна категорія — одна конфігурація для всіх товарів
            return configs[0]

        # Специфічна категорія — беремо "Тип устройства" з характеристик товару
        device_type_value = ""
        for spec in specs_list:
            if spec.get("name", "").strip() == "Тип устройства":
                device_type_value = spec.get("value", "").strip()
                break

        if not device_type_value:
            # Товар без "Тип устройства" в специфічній категорії → порожній блок 3
            return None

        # Точний збіг (case-insensitive)
        for config in configs:
            if config.device_type.lower() == device_type_value.lower():
                return config

        # Значення не знайдено в keywords CSV → порожній блок 3
        return None
