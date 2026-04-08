"""
Ядро генератора ключових слів.
"""

from keywords.core.generator import ProductKeywordsGenerator
from keywords.core.models import (
    Spec,
    MAX_MODEL_KEYWORDS,
    MAX_SPEC_KEYWORDS,
    MAX_UNIVERSAL_KEYWORDS,
    MAX_TOTAL_KEYWORDS,
)
from keywords.core.helpers import SpecAccessor, KeywordBucket
from keywords.core.loaders import ConfigLoader, CategoryConfig

__all__ = [
    "ProductKeywordsGenerator",
    "Spec",
    "CategoryConfig",
    "SpecAccessor",
    "KeywordBucket",
    "ConfigLoader",
    "MAX_MODEL_KEYWORDS",
    "MAX_SPEC_KEYWORDS",
    "MAX_UNIVERSAL_KEYWORDS",
    "MAX_TOTAL_KEYWORDS",
]
