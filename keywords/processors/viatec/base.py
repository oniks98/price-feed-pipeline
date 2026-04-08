"""
Базовий клас для процесорів Viatec.
Містить специфічну логіку, яка стосується саме цього постачальника.
"""

from typing import List

from keywords.processors.base import BaseProcessor
from keywords.core.models import MAX_UNIVERSAL_KEYWORDS
from keywords.core.loaders import CategoryConfig


class ViatecBaseProcessor(BaseProcessor):
    """Базовий процесор для Viatec з можливістю додавання специфічної логіки"""
    
    def _generate_universal_keywords(
        self,
        config: CategoryConfig,
        lang: str
    ) -> List[str]:
        """Генерація універсальних ключових слів"""
        phrases = getattr(config, f"universal_phrases_{lang}", [])
        return phrases[:MAX_UNIVERSAL_KEYWORDS]
