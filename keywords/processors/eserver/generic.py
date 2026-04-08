"""
Процесор для загальних категорій (шафи, стійки) - eServer.
"""

from typing import List, Dict, Set
import logging
import re

from keywords.processors.eserver.base import EServerBaseProcessor
from keywords.core.models import Spec, MAX_MODEL_KEYWORDS, MAX_SPEC_KEYWORDS
from keywords.core.loaders import CategoryConfig
from keywords.core.helpers import SpecAccessor, KeywordBucket
from keywords.utils.name_helpers import extract_brand, extract_model, extract_technology
from keywords.categories.eserver.router import get_category_handler
from keywords.utils.spec_helpers import is_spec_allowed


class GenericProcessor(EServerBaseProcessor):
    """
    Процесор для загальних категорій (шафи, стійки, бокси).
    
    Структура ідентична ViatecGenericProcessor:
    - Блок 1: Модель і бренд (для шаф - висота U замість моделі)
    - Блок 2: Характеристики (специфічні для кожної категорії)
    - Блок 3: Універсальні фрази
    """

    def generate(
        self,
        name: str,
        config: CategoryConfig,
        specs: List[Spec],
        lang: str,
        manufacturers: Dict[str, str],
        logger: logging.Logger
    ) -> List[str]:
        """Генерація ключових слів для загальних категорій"""
        base = getattr(config, f"base_keyword_{lang}")
        if not base:
            return []

        accessor = SpecAccessor(specs)
        bucket = KeywordBucket(MAX_MODEL_KEYWORDS + MAX_SPEC_KEYWORDS)
        allowed = config.allowed_specs

        # Блок 1: Модель і бренд (для шаф - висота U)
        model_keywords = self._generate_model_keywords(name, manufacturers, accessor)
        bucket.extend(model_keywords)

        # Блок 2: Характеристики (специфічні для категорії)
        spec_keywords = self._generate_spec_keywords(
            name, base, accessor, lang, allowed, config.category_id, manufacturers
        )
        bucket.extend(spec_keywords)

        # Блок 3: Універсальні фрази
        universal_keywords = self._generate_universal_keywords(config, lang)
        bucket.extend(universal_keywords)

        return bucket.to_list()

    def _generate_model_keywords(
        self,
        name: str,
        manufacturers: Dict[str, str],
        accessor: SpecAccessor
    ) -> List[str]:
        """
        Генерація ключових слів на основі моделі та бренду.
        
        Для шаф/стійок eServer: замість моделі використовуємо висоту (6U, 42U).
        """
        keywords = []
        
        # Витягуємо висоту з назви (6U, 42U)
        height_match = re.search(r'(\d+)\s*U\b', name, re.IGNORECASE)
        if height_match:
            height = f"{height_match.group(1)}U"
            keywords.append(height)
        
        # Якщо висоти немає в назві - шукаємо в характеристиках
        if not height_match:
            height_value = accessor.value("Робоча висота (U)")
            if height_value:
                # Очищаємо від "U" якщо є
                height_clean = height_value.replace("U", "").replace("u", "").strip()
                match = re.search(r'(\d+)', height_clean)
                if match:
                    height = f"{match.group(1)}U"
                    keywords.append(height)
        
        return keywords[:MAX_MODEL_KEYWORDS]

    def _generate_spec_keywords(
        self,
        name: str,
        base: str,
        accessor: SpecAccessor,
        lang: str,
        allowed: Set[str],
        category_id: str,
        manufacturers: Dict[str, str]
    ) -> List[str]:
        """
        Генерація ключових слів з характеристик.
        
        Використовує категорійні обробники для витягування специфічних характеристик:
        - Шафи серверні: "шкаф серверный напольный", "серверный шкаф настенный"
        - Шафи телекомунікаційні: "шкаф телекоммуникационный настенный"
        - Бокси електромонтажні: "бокс электромонтажный настенный"
        """
        bucket = KeywordBucket(MAX_SPEC_KEYWORDS)

        # Витягуємо бренд з характеристик або назви
        brand = self._get_brand_from_accessor(accessor, allowed) or extract_brand(name, manufacturers)

        # Бренд + base (з маленької букви)
        if brand:
            bucket.add(f"{base} {brand.lower()}")
            bucket.add(f"{brand.lower()} {base}")

        # Специфічні характеристики категорії через роутер
        category_handler = get_category_handler(category_id)
        if category_handler:
            category_keywords = category_handler(accessor, lang, base, allowed)
            bucket.extend(category_keywords)

        return bucket.to_list()

    def _get_brand_from_accessor(
        self,
        accessor: SpecAccessor,
        allowed: Set[str]
    ) -> str | None:
        """Витягування бренду з характеристик"""
        if not is_spec_allowed("Виробник", allowed):
            return None

        return accessor.value("Виробник")
