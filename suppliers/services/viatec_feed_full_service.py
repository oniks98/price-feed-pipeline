"""
Сервіс для завантаження повного фіду viatec.ua.

Фід: https://viatec.ua/files/product_info_yml.xml

Будує два словники при старті:
  1. {url: {name_ua, description_ua, image, available, params}}
     де url — канонічний UA-URL товару (без /ru/)
  2. {sku: vendor}  — перекочовано з ViatecFeedService для self-contained роботи

Паук ViatecFeedFullSpider використовує цей сервіс замість category crawling:
  - get_all_urls()          → ітерація по всіх товарах фіду
  - get_product_data(url)   → UA-дані для мерджу в parse_product_ru
  - get_vendor(sku)         → виробник за артикулом
"""

from __future__ import annotations

import urllib.request
import xml.etree.ElementTree as ET
from dataclasses import dataclass, field
from decimal import Decimal, InvalidOperation
from typing import Iterator, Optional


FEED_URL = "https://viatec.ua/files/product_info_yml.xml"
_FETCH_TIMEOUT = 60

# Базовий URL сайту (UA-версія без /ru/)
_BASE_URL = "https://viatec.ua"


@dataclass(slots=True)
class FeedProduct:
    """UA-дані товару з фіду."""
    url: str
    name_ua: str
    description_ua: str
    image: str
    available: bool
    params: list[dict[str, str]] = field(default_factory=list)


class ViatecFeedFullService:
    """
    Завантажує XML-фід і надає два лукапи:

    1. get_all_urls()        → Iterator[str]          — всі UA-URL товарів
    2. get_product_data(url) → FeedProduct | None     — UA-дані за URL
    3. get_vendor(sku)       → str                    — виробник за артикулом
    4. usd_rate              → Decimal | None         — USD курс з <currencies>
    """

    def __init__(self, logger=None) -> None:
        self._logger = logger
        # {canonical_ua_url: FeedProduct}
        self._product_map: dict[str, FeedProduct] = {}
        # {sku_lower: vendor}
        self._vendor_map: dict[str, str] = {}
        # USD курс з <currencies> фіду
        self.usd_rate: Optional["Decimal"] = None
        self._load()

    # ------------------------------------------------------------------ #
    # PUBLIC API
    # ------------------------------------------------------------------ #

    def get_all_urls(self) -> Iterator[str]:
        """Повертає всі UA-URL товарів з фіду."""
        yield from self._product_map.keys()

    def get_product_data(self, url: str) -> Optional[FeedProduct]:
        """
        Повертає UA-дані товару за URL або None якщо не знайдено.

        URL нормалізується: /ru/ видаляється перед лукапом.
        """
        return self._product_map.get(self._canonical(url))

    def get_vendor(self, sku: str) -> str:
        """Повертає виробника за артикулом або "" якщо не знайдено."""
        if not sku:
            return ""
        return self._vendor_map.get(sku.strip().lower(), "")

    @property
    def loaded(self) -> bool:
        return bool(self._product_map)

    def __len__(self) -> int:
        return len(self._product_map)

    # ------------------------------------------------------------------ #
    # LOAD
    # ------------------------------------------------------------------ #

    def _load(self) -> None:
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

        rate_info = f", USD rate={self.usd_rate}" if self.usd_rate else ""
        self._log_info(
            f"✅ ViatecFeedFullService: {len(self._product_map)} товарів, "
            f"{len(self._vendor_map)} з виробником{rate_info}"
        )

    def _fetch_feed(self) -> bytes:
        req = urllib.request.Request(
            FEED_URL,
            headers={"User-Agent": "Mozilla/5.0 (compatible; ViatecFeedFullLoader/1.0)"},
        )
        with urllib.request.urlopen(req, timeout=_FETCH_TIMEOUT) as resp:
            return resp.read()

    def _parse(self, xml_bytes: bytes) -> None:
        """
        Парсить YML-фід і заповнює _product_map та _vendor_map.

        Структура оферу (YML):
            <offer id="..." available="true/false">
                <url>https://viatec.ua/product/...</url>
                <name>Назва UA</name>
                <description>Опис UA</description>
                <picture>https://...</picture>
                <vendorCode>АРТИКУЛ</vendorCode>
                <vendor>Виробник</vendor>
                <param name="...">Значення</param>
            </offer>
        """
        root = ET.fromstring(xml_bytes)
        product_count = 0
        vendor_count = 0

        # ── USD курс з <currencies> ───────────────────────────────────────
        self.usd_rate = self._parse_usd_rate(root)

        for offer in root.iter("offer"):
            url_raw = (offer.findtext("url") or "").strip()
            if not url_raw:
                continue

            url = self._canonical(url_raw)
            name_ua = (offer.findtext("name") or "").strip()
            description_ua = (offer.findtext("description") or "").strip()
            image = (offer.findtext("picture") or "").strip()
            available_attr = offer.get("available", "false").strip().lower()
            available = available_attr == "true"

            params = [
                {"name": p.get("name", "").strip(), "value": (p.text or "").strip()}
                for p in offer.iter("param")
                if p.get("name", "").strip() and (p.text or "").strip()
            ]

            self._product_map[url] = FeedProduct(
                url=url,
                name_ua=name_ua,
                description_ua=description_ua,
                image=image,
                available=available,
                params=params,
            )
            product_count += 1

            # Vendor map
            sku = self._extract_sku(offer)
            vendor = self._extract_vendor(offer)
            if sku and vendor:
                self._vendor_map[sku.lower()] = vendor
                vendor_count += 1

        self._log_info(f"   └─ розпарсено {product_count} офферів ({vendor_count} з виробником)")

    # ------------------------------------------------------------------ #
    # USD RATE
    # ------------------------------------------------------------------ #

    @staticmethod
    def _parse_usd_rate(root: ET.Element) -> Optional[Decimal]:
        """
        Парсить USD курс з блоку <currencies>:
            <currency id="USD" rate="44.00"/>
        Повертає Decimal або None якщо не знайдено / некоректний формат.
        """
        for currency in root.iter("currency"):
            if currency.get("id", "").upper() == "USD":
                raw = (currency.get("rate") or "").strip()
                try:
                    rate = Decimal(raw)
                    if rate > 0:
                        return rate
                except InvalidOperation:
                    pass
        return None

    # ------------------------------------------------------------------ #
    # EXTRACTORS
    # ------------------------------------------------------------------ #

    @staticmethod
    def _extract_sku(offer: ET.Element) -> str:
        vc = offer.findtext("vendorCode")
        if vc and vc.strip():
            return vc.strip()
        return offer.get("id", "").strip()

    @staticmethod
    def _extract_vendor(offer: ET.Element) -> str:
        vendor_tag = offer.findtext("vendor")
        if vendor_tag and vendor_tag.strip():
            return vendor_tag.strip()
        for param in offer.iter("param"):
            if param.get("name", "").strip().lower() in ("виробник", "производитель"):
                val = (param.text or "").strip()
                if val:
                    return val
        return ""

    @staticmethod
    def _canonical(url: str) -> str:
        """Нормалізує URL до UA-версії (без /ru/)."""
        return url.replace("/ru/", "/")

    # ------------------------------------------------------------------ #
    # LOGGING
    # ------------------------------------------------------------------ #

    def _log_info(self, msg: str) -> None:
        if self._logger:
            self._logger.info(msg)

    def _log_warning(self, msg: str) -> None:
        if self._logger:
            self._logger.warning(f"⚠️ ViatecFeedFullService: {msg}")
        else:
            print(f"[ViatecFeedFullService WARNING] {msg}")
