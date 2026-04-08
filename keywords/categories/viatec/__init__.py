"""
Модуль категорій для постачальника Viatec.
"""

from keywords.categories.viatec import (
    hdd, 
    sd_card, 
    usb_flash, 
    mounts, 
    boxes, 
    intercom, 
    lock, 
    battery,
    camera,
    dvr,
    kommutatory
)
from keywords.categories.viatec.router import get_category_handler

__all__ = [
    "camera",
    "dvr",
    "hdd",
    "sd_card",
    "usb_flash",
    "mounts",
    "boxes",
    "intercom",
    "lock",
    "battery",
    "kommutatory",
    "get_category_handler",
]
