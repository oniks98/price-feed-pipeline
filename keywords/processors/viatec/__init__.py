"""
Процесори для постачальника Viatec.
"""

from keywords.processors.viatec.base import ViatecBaseProcessor
from keywords.processors.viatec.generic import GenericProcessor

__all__ = [
    "ViatecBaseProcessor",
    "GenericProcessor",
]
