"""
LP (LogicPower / GreenVision) keywords processor.

Структура (3 блоки — єдиний стандарт pipeline):
  Блок 1: модель + бренд  (extract_model / extract_brand)
  Блок 2: характеристики  (allowed_specs з lp_keywords.csv)
  Блок 3: універсальні фрази (universal_phrases_{lang})

NOTE: LP-специфічних category-handlers ще немає (keywords/categories/lp/ відсутня).
      Блок 2 використовує тільки allowed_specs з CSV — повністю config-driven.
      Коли з'являться category-handlers → додати router за аналогією з Viatec.
"""
from __future__ import annotations

from typing import Dict, List, Set
import logging

from keywords.core.helpers import KeywordBucket, SpecAccessor
from keywords.core.loaders import CategoryConfig
from keywords.core.models import (
    MAX_MODEL_KEYWORDS,
    MAX_SPEC_KEYWORDS,
    MAX_UNIVERSAL_KEYWORDS,
    Spec,
)
from keywords.processors.base import BaseProcessor
from keywords.utils.name_helpers import extract_brand, extract_model
from keywords.utils.spec_helpers import is_spec_allowed


class GenericProcessor(BaseProcessor):
    """
    LP keywords processor (LogicPower + GreenVision).

    Успадковує BaseProcessor — той самий контракт, що й Viatec / EServer / Secur.
    """

    def generate(
        self,
        name: str,
        config: CategoryConfig,
        specs: List[Spec],
        lang: str,
        manufacturers: Dict[str, str],
        logger: logging.Logger,
    ) -> List[str]:
        base = getattr(config, f"base_keyword_{lang}", "")
        if not base:
            return []

        accessor = SpecAccessor(specs)
        bucket = KeywordBucket(MAX_MODEL_KEYWORDS + MAX_SPEC_KEYWORDS + MAX_UNIVERSAL_KEYWORDS)
        allowed = config.allowed_specs

        # ── Блок 1: модель + бренд ────────────────────────────────────────────
        bucket.extend(self._model_keywords(name, manufacturers))

        # ── Блок 2: характеристики ────────────────────────────────────────────
        bucket.extend(self._spec_keywords(base, accessor, allowed))

        # ── Блок 3: універсальні фрази ────────────────────────────────────────
        bucket.extend(self._universal_keywords(config, lang))

        return bucket.to_list()

    # ── Блок 1 ────────────────────────────────────────────────────────────────

    def _model_keywords(
        self,
        name: str,
        manufacturers: Dict[str, str],
    ) -> List[str]:
        """
        Модель та бренд з назви товару.

        Приклади:
          "ИБП LogicPower LPM-L625VA" → ["LPM-L625VA", "LogicPower LPM-L625VA"]
          "LED лампа GreenVision GV-HAL-G53-7W" → ["GV-HAL-G53-7W", "GreenVision GV-HAL-G53-7W"]
        """
        brand = extract_brand(name, manufacturers)
        model = extract_model(name, brand=brand)

        keywords: List[str] = []

        if model:
            keywords.append(model)
            if brand and brand.lower() not in model.lower():
                keywords.append(f"{brand} {model}")
                keywords.append(f"{model} {brand}")

        return keywords[:MAX_MODEL_KEYWORDS]

    # ── Блок 2 ────────────────────────────────────────────────────────────────

    def _spec_keywords(
        self,
        base: str,
        accessor: SpecAccessor,
        allowed: Set[str],
    ) -> List[str]:
        """
        Ключові слова з характеристик (тільки allowed_specs з lp_keywords.csv).

        Приклади для ІБП:
          base="іbп", allowed={"потужність", "тип акумулятора"}
          → ["іbп 625va", "іbп agm"]
        """
        bucket = KeywordBucket(MAX_SPEC_KEYWORDS)

        # Виробник завжди першим (якщо дозволено)
        if is_spec_allowed("виробник", allowed):
            brand_spec = accessor.value("Виробник")
            if brand_spec:
                bucket.add(f"{base} {brand_spec.lower()}")

        # Решта allowed_specs у довільному порядку: "{base} {value}"
        for spec_name in allowed:
            if spec_name == "виробник":
                continue
            value = accessor.value(spec_name)
            if value:
                bucket.add(f"{base} {value.lower()}")

        return bucket.to_list()

    # ── Блок 3 ────────────────────────────────────────────────────────────────

    def _universal_keywords(self, config: CategoryConfig, lang: str) -> List[str]:
        """Фрази з CategoryConfig.universal_phrases_{lang}."""
        phrases = getattr(config, f"universal_phrases_{lang}", [])
        return phrases[:MAX_UNIVERSAL_KEYWORDS]
