"""
Утиліти для роботи з характеристиками та назвами товарів.
"""

from keywords.utils.spec_helpers import (
    extract_capacity,
    extract_speed,
    extract_interface,
    extract_rpm,
    is_spec_allowed,
)
from keywords.utils.name_helpers import (
    extract_brand,
    extract_model,
    extract_technology,
    check_wifi,
)

__all__ = [
    "extract_capacity",
    "extract_speed",
    "extract_interface",
    "extract_rpm",
    "is_spec_allowed",
    "extract_brand",
    "extract_model",
    "extract_technology",
    "check_wifi",
]
