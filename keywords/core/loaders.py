"""
Завантажувачі конфігурацій з CSV файлів.
"""

import csv
from pathlib import Path
from typing import Dict, List, Set, NamedTuple
import logging


class CategoryConfig(NamedTuple):
    """Конфігурація категорії з CSV"""
    category_id: str
    base_keyword_ru: str
    base_keyword_ua: str
    universal_phrases_ru: List[str]
    universal_phrases_ua: List[str]
    allowed_specs: Set[str]
    device_type: str = ""  # значення колонки "Тип устройства" (порожнє = загальна категорія)


class ConfigLoader:
    """Завантажувач конфігурацій категорій"""

    @staticmethod
    def load_keywords_mapping(csv_path: str, logger: logging.Logger) -> Dict[str, List[CategoryConfig]]:
        """
        Завантаження налаштувань категорій з CSV.

        Args:
            csv_path: Шлях до CSV з налаштуваннями категорій
            logger: Логгер

        Returns:
            Словник категорій (кожна категорія може мати кілька конфігурацій)
        """
        categories: Dict[str, List[CategoryConfig]] = {}

        try:
            path = Path(csv_path)
            if not path.exists():
                raise FileNotFoundError(f"Keywords CSV not found: {csv_path}")

            with open(csv_path, "r", encoding="utf-8-sig") as f:
                reader = csv.DictReader(f, delimiter=";")
                for row in reader:
                    category_id = row.get("Ідентифікатор_підрозділу", "").strip()
                    if not category_id:
                        continue

                    config = CategoryConfig(
                        category_id=category_id,
                        base_keyword_ru=row.get("base_keyword_ru", "").strip(),
                        base_keyword_ua=row.get("base_keyword_ua", "").strip(),
                        universal_phrases_ru=ConfigLoader._split_phrases(
                            row.get("universal_phrases_ru", "")
                        ),
                        universal_phrases_ua=ConfigLoader._split_phrases(
                            row.get("universal_phrases_ua", "")
                        ),
                        allowed_specs=ConfigLoader._parse_allowed_specs(
                            row.get("allowed_specs", "")
                        ),
                        device_type=row.get("Тип устройства", "").strip()
                    )

                    # Додаємо конфігурацію до списку для цієї категорії
                    if category_id not in categories:
                        categories[category_id] = []
                    categories[category_id].append(config)

            total_configs = sum(len(configs) for configs in categories.values())
            logger.info(f"Loaded {len(categories)} categories with {total_configs} configurations")
            return categories

        except Exception as e:
            logger.error(f"Failed to load keywords CSV: {e}")
            raise

    @staticmethod
    def load_manufacturers(csv_path: str, logger: logging.Logger) -> Dict[str, str]:
        """
        Завантаження маппінгу виробників з CSV.

        Ключі сортуються від довших до коротших — щоб довгий ключ ("uniview")
        завжди мав пріоритет над коротким ("ua", "cr", "ml") під час пошуку.

        Args:
            csv_path: Шлях до CSV з виробниками
            logger: Логгер

        Returns:
            Словник виробників {keyword_lower: manufacturer}, відсортований
            від найдовшого ключа до найкоротшого.
        """
        raw: Dict[str, str] = {}

        try:
            path = Path(csv_path)
            if not path.exists():
                raise FileNotFoundError(f"Manufacturers CSV not found: {csv_path}")

            with open(csv_path, "r", encoding="utf-8-sig") as f:
                reader = csv.DictReader(f, delimiter=";")
                for row in reader:
                    keyword = row.get("Слово в названии продукта", "").strip()
                    manufacturer = row.get("Производитель (виробник)", "").strip()
                    if keyword and manufacturer:
                        raw[keyword.lower()] = manufacturer

            # Сортуємо: довгі ключі мають пріоритет (як у ManufacturersDB)
            manufacturers = dict(
                sorted(raw.items(), key=lambda x: len(x[0]), reverse=True)
            )

            logger.info(f"Loaded {len(manufacturers)} manufacturers")
            return manufacturers

        except Exception as e:
            logger.error(f"Failed to load manufacturers CSV: {e}")
            raise

    @staticmethod
    def _split_phrases(value: str) -> List[str]:
        """Розділення фраз з CSV"""
        cleaned = value.strip().strip('"')
        if not cleaned:
            return []
        return [phrase.strip() for phrase in cleaned.split(",") if phrase.strip()]

    @staticmethod
    def _parse_allowed_specs(value: str) -> Set[str]:
        """
        Парсинг дозволених характеристик.
        
        Конвертує всі характеристики в lower case для строгої перевірки.
        """
        cleaned = value.strip().strip('"')
        if not cleaned:
            return set()
        return {spec.strip().lower() for spec in cleaned.split(",") if spec.strip()}
