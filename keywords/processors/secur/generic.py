"""
Процесор для загальних категорій (сигналізація Ajax, датчики, сирени) - Secur.
"""

from typing import List, Dict, Set
import logging
import re

from keywords.processors.secur.base import SecurBaseProcessor
from keywords.core.models import Spec, MAX_MODEL_KEYWORDS, MAX_SPEC_KEYWORDS
from keywords.core.loaders import CategoryConfig
from keywords.core.helpers import SpecAccessor, KeywordBucket
from keywords.utils.name_helpers import extract_brand
from keywords.categories.secur.router import get_category_handler
from keywords.utils.spec_helpers import is_spec_allowed


class GenericProcessor(SecurBaseProcessor):
    """
    Процесор для загальних категорій (сигналізація Ajax, датчики, сирени, кнопки).

    Структура:
    - Блок 1: Модель і бренд (тільки латиниця)
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

        # Блок 1: Модель і бренд (латиниця)
        # brand береться з характеристики Виробник (точно) або extract_brand (fallback).
        # НЕ обчислюємо brand повторно всередині _generate_model_keywords,
        # щоб уникнути хибних збігів підрядком (напр. "bracket" -> Slinex в назві Ajax товару).
        brand = accessor.value("Виробник") or extract_brand(name, manufacturers)
        model_keywords = self._generate_model_keywords(name, brand)
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
        brand: str | None,
    ) -> List[str]:
        """
        БЛОК 1: Модель і бренд (тільки латиниця).

        brand передається ззовні — не обчислюється повторно всередині,
        щоб уникнути хибних збігів підрядком у manufacturers dict.

        Приклади:
        - "Ajax StarterKit 2 White"
          -> "StarterKit 2 White", "Ajax StarterKit 2 White", "StarterKit 2 White Ajax"
        - "Ajax MotionCam Outdoor (PhOD) White"
          -> "MotionCam Outdoor (PhOD) White", ...
        - "Ajax EN54 FireProtect (Sounder/VAD) Jeweller"
          -> "EN54 FireProtect (Sounder/VAD) Jeweller", ...
        - "Кріпильна панель Ajax SmartBracket для Hub White"
          -> "SmartBracket", "Ajax SmartBracket", "SmartBracket Ajax"
        """
        keywords = []

        def is_latin_only(text: str) -> bool:
            """True якщо рядок містить лише латиницю, цифри та службові символи."""
            if not text:
                return False
            return bool(re.match(r'^[A-Za-z0-9\s\-\(\)\/\.]+$', text))

        # Витягуємо латинську частину після слова Ajax.
        # Зупиняємось на першому кириличному символі або кінці рядка.
        ajax_match = re.search(
            r'\bAjax\b\s*([A-Za-z][A-Za-z0-9\s\-\(\)\/\.]+?)(?=[A-Za-z0-9\s\-\(\)\/\.]*[А-ЯҐЄІЇа-яґєії]|$)',
            name,
            re.IGNORECASE,
        )

        if ajax_match:
            model = ajax_match.group(1).strip()
            if model and is_latin_only(model):
                keywords.append(model)

                if brand and is_latin_only(brand):
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
        manufacturers: Dict[str, str],
    ) -> List[str]:
        """
        БЛОК 2: Характеристики (специфічні для категорії).

        Використовує категорійні обробники через роутер.
        """
        bucket = KeywordBucket(MAX_SPEC_KEYWORDS)

        brand = self._get_brand_from_accessor(accessor, allowed) or extract_brand(name, manufacturers)

        if brand:
            bucket.add(f"{base} {brand.lower()}")
            bucket.add(f"{brand.lower()} {base}")

        category_handler = get_category_handler(category_id)
        if category_handler:
            bucket.extend(category_handler(accessor, lang, base, allowed))

        return bucket.to_list()

    def _get_brand_from_accessor(
        self,
        accessor: SpecAccessor,
        allowed: Set[str],
    ) -> str | None:
        """Витягування бренду з характеристики Виробник."""
        if not is_spec_allowed("Виробник", allowed):
            return None
        return accessor.value("Виробник")
