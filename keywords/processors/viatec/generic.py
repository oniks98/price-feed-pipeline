"""
Процесор для загальних категорій (HDD, SD, USB та інші) - Viatec.
"""

from typing import List, Dict, Set
import logging
import inspect

from keywords.processors.viatec.base import ViatecBaseProcessor
from keywords.core.models import Spec, MAX_MODEL_KEYWORDS, MAX_SPEC_KEYWORDS
from keywords.core.loaders import CategoryConfig
from keywords.core.helpers import SpecAccessor, KeywordBucket
from keywords.utils.name_helpers import extract_brand, extract_model
from keywords.categories.viatec.router import get_category_handler
from keywords.utils.spec_helpers import is_spec_allowed


class GenericProcessor(ViatecBaseProcessor):
    """
    Процесор для загальних категорій (HDD, SD карти, USB, кронштейни, коробки).
    
    Структура:
    - Блок 1: Модель і бренд
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

        # Блок 1: Модель і бренд
        model_keywords = self._generate_model_keywords(name, manufacturers)
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
        manufacturers: Dict[str, str]
    ) -> List[str]:
        """
        Генерація ключових слів на основі моделі та бренду.
        
        Приклади:
        - "Жесткий диск Seagate SkyHawk ST1000VX013 1Тб" 
          -> ["SkyHawk ST1000VX013", "Seagate SkyHawk ST1000VX013"]
          
        - "Карта памяти MicroSD Imou ST2-128-S1" 
          -> ["ST2-128-S1", "Imou ST2-128-S1"]
          
        - "Карта памяти Ezviz CS-CMT-CARDT128G-D 128Гб" 
          -> ["CS-CMT-CARDT128G-D", "Ezviz CS-CMT-CARDT128G-D"]
        """
        brand = extract_brand(name, manufacturers)
        model = extract_model(name, brand=brand)

        keywords = []

        # 1. Модель (без бренду)
        if model:
            keywords.append(model)
            
            # 2. Бренд + Модель та Модель + Бренд (якщо бренд не входить до моделі)
            if brand and brand.lower() not in model.lower():
                keywords.append(f"{brand} {model}")
                keywords.append(f"{model} {brand}")

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
        - HDD: "жесткий диск 2tb", "жесткий диск sata3", "hdd 7200 об/мин"
        - SD карти: "sd карта 64gb", "sd карта class 10"
        - USB: "флешка 32gb", "флешка usb 3.0"
        - Кронштейни: "кронштейн для камеры", "кронштейн поворотный"
        - Коробки: "коробка ip65", "коробка металлическая"
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
            # Передаємо name для категорій, яким це потрібно (DVR для AI)
            sig = inspect.signature(category_handler)
            if 'name' in sig.parameters:
                category_keywords = category_handler(accessor, lang, base, allowed, name=name)
            else:
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
