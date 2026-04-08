"""
Процесори для різних типів товарів.
"""

from keywords.processors.base import BaseProcessor
from keywords.processors.viatec.generic import GenericProcessor

# ЗАСТАРІЛЕ: get_processor більше не використовується
# Використовуйте ProductKeywordsGenerator(supplier="...") замість цього

__all__ = [
    "BaseProcessor",
    "GenericProcessor",
]
