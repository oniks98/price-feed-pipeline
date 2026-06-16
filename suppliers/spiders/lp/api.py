"""
suppliers/spiders/lp/api.py
══════════════════════════════════════════════════════════════════════
LogicPower B2B REST API spider  (spider name: lp_api)

Послідовність запитів:
  1. GET /external/finance/currencyRates
       → знаходимо paymentType=businessEntity, USD→UAH
       → зберігаємо self.usd_rate

  2. GET /external/catalog/product/list/all?pageSize=500&pageNum=1
       → отримуємо totalItems → запускаємо сторінки 2..N паралельно
       → фільтруємо status == "inStock" (client-side, API не підтримує)
       → yield item → SuppliersPipeline

Ціни в item (pipeline Viatec-path):
  Ціна       = personal price (USD)   → pipeline: × usd_rate → Оптова_ціна → канальна ціна
  usd_rate   = businessEntity курс    → pipeline: DealerPriceService.dealer_uah(...)
  price_rrp_uah = recommendedRetail (UAH)

Category matching:
  product.categories[].code → перший збіг з lp_category.csv (category id)

Resume:
  Читає data/output/lp_new.csv → set вже оброблених кодів товарів

.env:
  LP_API_TOKEN   — Bearer токен
  LP_API_BASE_URL — https://api.b2b.logicpower.ua
"""
from __future__ import annotations

import csv
import math
import os
from decimal import Decimal, InvalidOperation
from pathlib import Path

import scrapy
from dotenv import load_dotenv
from scrapy.http import Response

from suppliers.spiders.base import BaseDealerSpider

# ─────────────────────────────────────────────────────────────────────
# Константи
# ─────────────────────────────────────────────────────────────────────
PAGE_SIZE = 500

# Виробники, яких обробляємо (case-insensitive)
TARGET_MANUFACTURERS: frozenset[str] = frozenset({
    "logicpower",
    "greenvision",
})

# Символ наявності → орієнтовна кількість
AVAILABILITY_QTY: dict[str | None, str] = {
    "+/-": "3",
    "+":   "10",
    "++":  "50",
    "+++": "100",
}


class LpApiSpider(BaseDealerSpider):
    """LogicPower B2B API — dealer spider (USD, businessEntity)."""

    name        = "lp_api"
    supplier_id = "lp"
    output_filename = "lp_new.csv"

    custom_settings = {
        "ITEM_PIPELINES": {
            "suppliers.pipelines.SuppliersPipeline": 300,
        },
        "CONCURRENT_REQUESTS":            4,
        "CONCURRENT_REQUESTS_PER_DOMAIN": 4,
        "DOWNLOAD_DELAY":                 0,
        "RETRY_ENABLED":                  True,
        "RETRY_TIMES":                    3,
        "RETRY_HTTP_CODES":               [500, 502, 503, 504],
        "HTTPERROR_ALLOW_ALL":            True,
        "DOWNLOAD_TIMEOUT":               60,
    }

    # ──────────────────────────────────────────────────────────────────
    # INIT
    # ──────────────────────────────────────────────────────────────────

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        _root = Path(os.environ.get("PROJECT_ROOT", r"C:\FullStack\PriceFeedPipeline"))
        load_dotenv(_root / "suppliers" / ".env")

        self.api_token = os.getenv("LP_API_TOKEN", "").strip()
        self.base_url  = os.getenv("LP_API_BASE_URL", "").strip()

        if not self.api_token:
            raise ValueError("❌ LP_API_TOKEN не знайдено в suppliers/.env")
        if not self.base_url:
            raise ValueError("❌ LP_API_BASE_URL не знайдено в suppliers/.env")

        self._root: Path = _root

        # Курс USD/UAH (встановлюється в parse_rates)
        self.usd_rate: Decimal = Decimal("0")

        # Set кодів категорій з lp_category.csv
        self.category_codes: set[str] = self._load_category_codes()

        # Set вже оброблених кодів товарів (resume)
        self.processed_codes: set[str] = self._load_processed_codes()

        # product_code → leaf_category_code (генерується lp_export_categories.py)
        self._product_cat_map: dict[str, str] = self._load_product_category_map()

        # Статистика
        self._stats: dict[str, int] = {
            "yielded":              0,
            "skip_not_target_mfr": 0,
            "skip_not_instock":    0,
            "skip_resume":         0,
            "skip_no_category":    0,
            "skip_no_price":       0,
        }

    # ──────────────────────────────────────────────────────────────────
    # INIT HELPERS
    # ──────────────────────────────────────────────────────────────────

    def _load_category_codes(self) -> set[str]:
        """Завантажує set<code> з lp_category.csv (колонка 'category id')."""
        codes: set[str] = set()
        path = self._root / "data" / "lp" / "lp_category.csv"

        if not path.exists():
            self.logger.warning(f"⚠️ lp_category.csv не знайдено: {path}")
            return codes

        with open(path, encoding="utf-8-sig") as f:
            for row in csv.DictReader(f, delimiter=";"):
                code = row.get("category id", "").strip()
                if code:
                    codes.add(code)

        self.logger.info(f"📂 Category codes завантажено: {len(codes)}")
        return codes

    def _load_product_category_map(self) -> dict[str, str]:
        """
        Зчитує lp_product_categories.json → dict[product_code → leaf_cat_code].
        Файл генерується scripts/lp_export_categories.py.
        Якщо відсутній — fallback на product.categories[].
        """
        import json
        path = self._root / "data" / "lp" / "lp_product_categories.json"
        if not path.exists():
            self.logger.warning(
                "⚠️ lp_product_categories.json не знайдено — "
                "запустіть scripts/lp_export_categories.py"
            )
            return {}
        with open(path, encoding="utf-8") as f:
            data: dict[str, str] = json.load(f)
        self.logger.info(f"🗺️  Product→Category map: {len(data)} товарів")
        return data

    def _load_processed_codes(self) -> set[str]:
        """Читає output CSV → set кодів для resume-mode."""
        codes: set[str] = set()
        path = self._root / "data" / "output" / self.output_filename

        if not path.exists():
            return codes

        with open(path, encoding="utf-8-sig") as f:
            for row in csv.DictReader(f, delimiter=";"):
                code = row.get("Ідентифікатор_товару", "").strip()
                if code:
                    codes.add(code)

        self.logger.info(f"🔁 Resume: пропускаємо {len(codes)} вже оброблених товарів")
        return codes

    # ──────────────────────────────────────────────────────────────────
    # HTTP HELPERS
    # ──────────────────────────────────────────────────────────────────

    def _headers(self) -> dict[str, str]:
        return {
            "X-Api-Key": self.api_token,
            "Accept":    "application/json",
        }

    def _url(self, path: str) -> str:
        return f"{self.base_url.rstrip('/')}{path}"

    def _products_url(self, page: int) -> str:
        return self._url(
            f"/external/catalog/product/list/all"
            f"?pageSize={PAGE_SIZE}&pageNum={page}"
        )

    # ──────────────────────────────────────────────────────────────────
    # START → RATES → PRODUCTS
    # ──────────────────────────────────────────────────────────────────

    async def start(self):
        yield scrapy.Request(
            url=self._url("/external/finance/currencyRates"),
            headers=self._headers(),
            callback=self.parse_rates,
            dont_filter=True,
            errback=self._on_error,
        )

    def parse_rates(self, response: Response):
        if not self._check_response(response, "currencyRates"):
            return

        for entry in response.json().get("data", []):
            if (
                entry.get("paymentType")  == "businessEntity"
                and entry.get("sourceCurrency") == "USD"
                and entry.get("targetCurrency") == "UAH"
            ):
                try:
                    self.usd_rate = Decimal(str(entry["amount"]))
                except (InvalidOperation, KeyError):
                    self.logger.error("❌ Не вдалося розпарсити 'amount' з курсу")
                    return
                self.logger.info(f"💱 USD (businessEntity / ФОП): {self.usd_rate} UAH")
                break

        if self.usd_rate == Decimal("0"):
            self.logger.error("❌ Курс USD/businessEntity не знайдено — зупинка")
            return

        yield scrapy.Request(
            url=self._products_url(page=1),
            headers=self._headers(),
            callback=self.parse_products,
            meta={"page": 1},
            dont_filter=True,
            errback=self._on_error,
        )

    def parse_products(self, response: Response):
        page = response.meta["page"]

        if not self._check_response(response, f"products page={page}"):
            return

        data       = response.json().get("data", {})
        items      = data.get("items", [])
        total      = data.get("totalItems", 0)

        self.logger.info(f"📄 Сторінка {page}: {len(items)} товарів (всього {total})")

        for product in items:
            item = self._build_item(product)
            if item is not None:
                yield item

        # Пагінація: запускаємо сторінки 2..N після першої
        if page == 1 and total > PAGE_SIZE:
            total_pages = math.ceil(total / PAGE_SIZE)
            self.logger.info(f"📑 Пагінація: {total_pages} сторінок")
            for next_page in range(2, total_pages + 1):
                yield scrapy.Request(
                    url=self._products_url(next_page),
                    headers=self._headers(),
                    callback=self.parse_products,
                    meta={"page": next_page},
                    dont_filter=True,
                    errback=self._on_error,
                )

    # ──────────────────────────────────────────────────────────────────
    # ITEM BUILDER
    # ──────────────────────────────────────────────────────────────────

    def _build_item(self, product: dict) -> dict | None:
        code   = str(product.get("code", "")).strip()
        status = product.get("status", "")

        # 0. Тільки цільові виробники
        mfr = ((product.get("manufacturer") or {}).get("name") or "").strip().lower()
        if mfr not in TARGET_MANUFACTURERS:
            self._stats["skip_not_target_mfr"] += 1
            return None

        # 1. Тільки inStock
        if status != "inStock":
            self._stats["skip_not_instock"] += 1
            return None

        # 2. Resume
        if code in self.processed_codes:
            self._stats["skip_resume"] += 1
            return None

        # 3. Ціна (personal USD обов'язкова)
        personal_usd, rrp_uah = self._extract_prices(product.get("prices", []))
        if not personal_usd:
            self._stats["skip_no_price"] += 1
            self.logger.debug(f"⚠️ Немає personal USD ціни: code={code}")
            return None

        # 4. Category
        category_code = self._resolve_category(product.get("categories", []), code)
        if category_code is None:
            self._stats["skip_no_category"] += 1
            raw_codes = [c.get("code") for c in product.get("categories", [])]
            self.logger.warning(
                f"⚠️ Категорія не в lp_category.csv: code={code} cats={raw_codes}"
            )
            return None

        # 5. Наявність і кількість
        avail_symbol = product.get("availability")
        quantity     = AVAILABILITY_QTY.get(avail_symbol, "10")

        self._stats["yielded"] += 1
        name_uk = self._loc(product, "name", "uk")
        self.logger.info(
            f"✅ [{code}] {name_uk[:60]} | "
            f"USD {personal_usd} | cat={category_code} | qty={quantity}"
        )

        return {
            # ── Ідентифікація ──────────────────────────────────────────
            "supplier_id":          self.supplier_id,
            "output_file":          self.output_filename,
            "source":               "api",

            "Ідентифікатор_товару": code,
            "Штрих_код_товару":     str(product.get("barcode") or "").strip(),

            # ── Назва та опис ─────────────────────────────────────────
            "Назва_позиції":        self._loc(product, "name", "ru"),
            "Назва_позиції_укр":    name_uk,
            "Опис":                 self._loc(product, "description", "ru"),
            "Опис_укр":             self._loc(product, "description", "uk"),

            "Тип_товару":           "r",
            "Одиниця_виміру":       "шт.",

            # ── Ціна (Viatec-path: pipeline робить dealer_uah = Ціна × usd_rate) ──
            "Ціна":                 personal_usd,   # USD dealer
            "Валюта":               "USD",
            "usd_rate":             str(self.usd_rate),
            "price_rrp_uah":        rrp_uah,        # UAH recommendedRetail

            # ── Наявність ─────────────────────────────────────────────
            "Наявність":            "В наявності",
            "Кількість":            quantity,

            # ── Виробник ──────────────────────────────────────────────
            "Виробник":             (product.get("manufacturer") or {}).get("name", ""),
            "Країна_виробник":      "",

            # ── Медіа ─────────────────────────────────────────────────
            "Посилання_зображення": self._extract_images(product.get("images", [])),
            "Продукт_на_сайті":     product.get("externalUrl") or "",

            # ── Channel-service lookup ────────────────────────────────
            # Пустий category_url → channel_service іде по category_id
            "category_url":         "",
            "category_id":          category_code,
            "feed_id":              "",

            # ── Пошукові запити (заповнює pipeline через KeywordsGenerator) ──
            "Пошукові_запити":      "",
            "Пошукові_запити_укр":  "",

            # ── Характеристики ────────────────────────────────────────
            "specifications_list":  self._extract_specs(product.get("specifications", [])),
        }

    # ──────────────────────────────────────────────────────────────────
    # HELPERS
    # ──────────────────────────────────────────────────────────────────

    def _resolve_category(self, categories: list[dict], product_code: str = "") -> str | None:
        """
        Пріоритети пошуку категорії:
          1. lp_product_categories.json — точний leaf маппінг (генерує lp_export_categories.py)
          2. product.categories[] × lp_category.csv (reversed — deepest first)
          3. Fallback (якщо CSV порожній): cats[-1]
        """
        # 1. JSON map (найточніший — leaf категорія з дерева категорій)
        if product_code and product_code in self._product_cat_map:
            return self._product_cat_map[product_code]

        # 2. Fallback через product.categories[] (зазвичай повертає батьківський код)
        if not categories:
            return None

        if not self.category_codes:
            # CSV ще не заповнений — беремо найглибший код без валідації
            return str(categories[-1].get("code", "")).strip() or None

        for cat in reversed(categories):
            code = str(cat.get("code", "")).strip()
            if code in self.category_codes:
                return code

        return None

    @staticmethod
    def _loc(obj: dict, field: str, lang: str) -> str:
        """Безпечно повертає локалізоване поле. uk → fallback ru."""
        inner = obj.get(field) or {}
        if isinstance(inner, dict):
            fallback = "ru" if lang == "uk" else "uk"
            return (inner.get(lang) or inner.get(fallback) or "").strip()
        return str(inner).strip()

    @staticmethod
    def _extract_prices(prices: list[dict]) -> tuple[str, str]:
        """
        Повертає (personal_usd, rrp_uah).
          personal          → тип "personal", валюта USD  (dealer price)
          recommendedRetail → тип "recommendedRetail", UAH
        """
        personal_usd = ""
        rrp_uah      = ""

        for p in prices:
            ptype    = p.get("type", "")
            money    = p.get("money") or {}
            amount   = money.get("amount")
            currency = money.get("currency", "")

            if amount is None:
                continue

            amount_str = str(amount)

            if ptype == "personal" and currency == "USD":
                personal_usd = amount_str
            elif ptype == "recommendedRetail" and currency == "UAH":
                rrp_uah = amount_str

        return personal_usd, rrp_uah

    @staticmethod
    def _extract_specs(specs_raw: list[dict]) -> list[dict]:
        """
        Безпечний парсинг specifications.
        Підтримує обидва формати API:
          {name: {uk, ru}, value: {uk, ru}, unit: str}
          {name: str,      value: str,      unit: str}
        Повертає тільки uk назви/значення.
        """
        result: list[dict] = []

        for s in specs_raw:
            name_obj  = s.get("name")  or {}
            value_obj = s.get("value") or {}

            if isinstance(name_obj, dict):
                name = (name_obj.get("uk") or name_obj.get("ru") or "").strip()
            else:
                name = str(name_obj).strip()

            if isinstance(value_obj, dict):
                value = (value_obj.get("uk") or value_obj.get("ru") or "").strip()
            else:
                value = str(value_obj).strip()

            unit = str(s.get("unit") or "").strip()

            if name and value:
                result.append({"name": name, "unit": unit, "value": value})

        return result

    @staticmethod
    def _extract_images(images_raw: list[dict]) -> str:
        """Витягує URL зображень → рядок через ', '."""
        urls: list[str] = []

        for img in images_raw:
            url = (
                img.get("url")
                or img.get("src")
                or img.get("path")
                or img.get("link")
                or ""
            )
            if url:
                urls.append(str(url).replace(",", "%2C"))

        return ", ".join(urls)

    # ──────────────────────────────────────────────────────────────────
    # RESPONSE CHECK / ERROR
    # ──────────────────────────────────────────────────────────────────

    def _check_response(self, response: Response, label: str) -> bool:
        if response.status == 401:
            self.logger.error(f"❌ HTTP 401 [{label}] — перевірте LP_API_TOKEN")
            return False
        if response.status != 200:
            self.logger.error(f"❌ HTTP {response.status} [{label}]")
            return False
        payload = response.json()
        if not payload.get("status"):
            self.logger.error(f"❌ API status=false [{label}]: {payload}")
            return False
        return True

    def _on_error(self, failure):
        self.logger.error(f"❌ Request failed: {failure.value}")

    # ──────────────────────────────────────────────────────────────────
    # CLOSE
    # ──────────────────────────────────────────────────────────────────

    def closed(self, reason: str) -> None:
        s = self._stats
        self.logger.info(
            f"🎉 lp_api завершено ({reason})\n"
            f"   yielded={s['yielded']}  "
            f"skip_not_target_mfr={s['skip_not_target_mfr']}  "
            f"skip_not_instock={s['skip_not_instock']}  "
            f"skip_resume={s['skip_resume']}  "
            f"skip_no_category={s['skip_no_category']}  "
            f"skip_no_price={s['skip_no_price']}"
        )
        try:
            import winsound
            for _ in range(3):
                winsound.Beep(1000, 300)
        except Exception:
            pass
