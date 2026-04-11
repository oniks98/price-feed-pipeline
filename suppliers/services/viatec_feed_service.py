"""
Сервіс для завантаження виробників з XML-фіду viatec.ua.

Фід: https://viatec.ua/files/product_info_yml.xml

Теги-джерела виробника (в порядку пріоритету):
  1. <vendor>...</vendor>                     — пряма назва вендора
  2. <param name="Виробник">...</param>       — характеристика товару

Сервіс будує dict {sku: vendor} один раз при старті паука.
При парсингу товару — лукап за артикулом (O(1)).

КАСКАД у пайплайні (нічого не змінюється):
  item["Виробник"] заповнений  → pipeline бере його, лукапить тільки країну
  item["Виробник"] порожній    → pipeline робить lookup() по назві (CSV-словарик)
  ні те ні інше               → pipeline викликає no_brand()
"""

from __future__ import annotations

import urllib.request
import xml.etree.ElementTree as ET
from typing import Optional


FEED_URL = "https://viatec.ua/files/product_info_yml.xml"

# Таймаут на завантаження фіду (секунди)
_FETCH_TIMEOUT = 60


class ViatecFeedService:
    """
    Завантажує XML-фід і надає lookup виробника за артикулом (SKU).

    Використання в павуку:
        self.feed_service = ViatecFeedService(logger=self.logger)
        vendor = self.feed_service.get_vendor(sku)   # "" якщо не знайдено
    """

    def __init__(self, logger=None) -> None:
        self._logger = logger
        # {артикул_нижній_регістр: назва_виробника}
        self._vendor_map: dict[str, str] = {}
        self._load()

    # ------------------------------------------------------------------ #
    # PUBLIC API
    # ------------------------------------------------------------------ #

    def get_vendor(self, sku: str) -> str:
        """
        Повертає назву виробника за артикулом або "" якщо не знайдено.

        Пошук case-insensitive: артикул нормалізується до нижнього регістру.
        """
        if not sku:
            return ""
        return self._vendor_map.get(sku.strip().lower(), "")

    @property
    def loaded(self) -> bool:
        """True якщо фід успішно завантажено і є хоча б один запис."""
        return bool(self._vendor_map)

    def __len__(self) -> int:
        return len(self._vendor_map)

    # ------------------------------------------------------------------ #
    # PRIVATE
    # ------------------------------------------------------------------ #

    def _load(self) -> None:
        """Завантажує фід і будує _vendor_map."""
        try:
            xml_bytes = self._fetch_feed()
        except Exception as exc:
            self._log_warning(f"Не вдалося завантажити фід: {exc}")
            return

        try:
            self._parse(xml_bytes)
        except Exception as exc:
            self._log_warning(f"Не вдалося розпарсити фід: {exc}")
            return

        self._log_info(
            f"✅ ViatecFeedService: {len(self._vendor_map)} артикулів з виробником завантажено з фіду"
        )

    def _fetch_feed(self) -> bytes:
        """HTTP-завантаження фіду. Повертає сирі байти XML."""
        req = urllib.request.Request(
            FEED_URL,
            headers={"User-Agent": "Mozilla/5.0 (compatible; ViatecFeedLoader/1.0)"},
        )
        with urllib.request.urlopen(req, timeout=_FETCH_TIMEOUT) as resp:
            return resp.read()

    def _parse(self, xml_bytes: bytes) -> None:
        """
        Парсить YML-фід і заповнює _vendor_map.

        Структура фіду (YML-формат):
            <offer id="..." available="...">
                <vendorCode>АРТИКУЛ</vendorCode>
                <vendor>Назва виробника</vendor>           ← пріоритет 1
                <param name="Виробник">Назва</param>       ← пріоритет 2
                ...
            </offer>
        """
        root = ET.fromstring(xml_bytes)

        # Шукаємо всі <offer> — вони можуть бути вкладені в <shop><offers>
        offers = root.iter("offer")
        count = 0

        for offer in offers:
            sku = self._extract_sku(offer)
            if not sku:
                continue

            vendor = self._extract_vendor(offer)
            if not vendor:
                continue

            self._vendor_map[sku.lower()] = vendor
            count += 1

        self._log_info(f"   └─ розпарсено {count} офферів з виробником")

    # ------------------------------------------------------------------ #
    # EXTRACTORS
    # ------------------------------------------------------------------ #

    @staticmethod
    def _extract_sku(offer: ET.Element) -> str:
        """
        Повертає артикул офера.

        Перевіряє у порядку:
          1. <vendorCode> — офіційний артикул постачальника
          2. атрибут id=""  — внутрішній ID офера (запасний варіант)
        """
        vc = offer.findtext("vendorCode")
        if vc and vc.strip():
            return vc.strip()

        offer_id = offer.get("id", "").strip()
        return offer_id

    @staticmethod
    def _extract_vendor(offer: ET.Element) -> str:
        """
        Повертає назву виробника з офера.

        Пріоритет:
          1. <vendor> — пряма назва вендора (найнадійніше)
          2. <param name="Виробник"> — характеристика товару
        """
        # Пріоритет 1
        vendor_tag = offer.findtext("vendor")
        if vendor_tag and vendor_tag.strip():
            return vendor_tag.strip()

        # Пріоритет 2
        for param in offer.iter("param"):
            name_attr = param.get("name", "").strip().lower()
            if name_attr in ("виробник", "производитель"):
                val = (param.text or "").strip()
                if val:
                    return val

        return ""

    # ------------------------------------------------------------------ #
    # LOGGING
    # ------------------------------------------------------------------ #

    def _log_info(self, msg: str) -> None:
        if self._logger:
            self._logger.info(msg)

    def _log_warning(self, msg: str) -> None:
        if self._logger:
            self._logger.warning(f"⚠️ ViatecFeedService: {msg}")
        else:
            print(f"[ViatecFeedService WARNING] {msg}")
