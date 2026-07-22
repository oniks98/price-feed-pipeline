"""
python scripts/ultra_clean_run.py lp_api
══════════════════════════════════════════════════════════════════════
LogicPower B2B REST API spider  (spider name: lp_api)

Послідовність запитів:
  1. GET /external/finance/currencyRates
       → знаходимо paymentType=businessEntity, USD→UAH
       → зберігаємо self.usd_rate

  2. GET /external/catalog/product/list/all?pageSize=500&pageNum=1
       → отримуємо totalItems → запускаємо сторінки 2..N паралельно
       → фільтруємо status in {"inStock", "quickProduction"} (client-side, API не підтримує)
  → quickProduction: Наявність="7" (дні), Кількість="" → Prom.ua: "Під замовлення, 7 днів"
       → yield item → SuppliersPipeline

Ціни в item (pipeline Viatec-path):
  Ціна          = personal price (USD)   → pipeline: × usd_rate → Оптова_ціна → канальна ціна
  usd_rate      = businessEntity курс    → pipeline: DealerPriceService.dealer_uah(...)
  price_rrp_uah = recommendedRetail (UAH)

Category matching:
  product.categories[].code → перший збіг з lp_category.csv (category id)
  Обхід у зворотному порядку (deepest first).
  Виключення: EXCLUDED_CATEGORY_CODES (Уцінка, Рекламна продукція).

Характеристики (_extract_specs):
  LP B2B API не має окремого поля unit на рівні spec.
  Одиниця вбудована у рядок значення: "3,60 кг", "260 мм".
  _SPEC_UNIT_RE розбиває рядок на (value, unit) перед передачею в pipeline.

Resume:
  Читає data/output/lp_new.csv → set вже оброблених кодів товарів.

.env:
  LP_API_TOKEN    — токен авторизації (передається в заголовку X-Api-Key, НЕ Authorization: Bearer)
  LP_API_BASE_URL — https://api.b2b.logicpower.ua
"""
from __future__ import annotations

import csv
import math
import os
import re
import sys
from decimal import Decimal, InvalidOperation
from pathlib import Path

import scrapy
from dotenv import load_dotenv
from scrapy.http import Response

from suppliers.spiders.base import BaseDealerSpider
from suppliers.services.lp_quickproduction_service import LpQuickProductionService

# ─────────────────────────────────────────────────────────────────────
# Константи
# ─────────────────────────────────────────────────────────────────────

PAGE_SIZE = 500

# Виробники, яких обробляємо. Matching за manufacturer.slug (стабільний,
# url-safe ідентифікатор: "logicpower"), НЕ за manufacturer.name
# ("LogicPower" — лише для відображення, формат не гарантований).
# Той самий slug використовує scripts/lp_export_categories.py:is_target_manufacturer().
TARGET_MANUFACTURERS: frozenset[str] = frozenset({
    "logicpower",
    "greenvision",
})

# Службові leaf-категорії LP — ніколи не є товарними, пропускаються одразу.
# Синхронізувати вручну з scripts/lp_export_categories.py:EXCLUDED_CATEGORY_CODES.
EXCLUDED_CATEGORY_CODES: frozenset[str] = frozenset({
    "12261",  # Уцінка
    "12864",  # Рекламна продукція
    "12356",  # Системи безпеки > Витратні матеріали
    # Системи безпеки > СКУД
    "12358",  # > Ключі
    # Комп'ютерні комплектуючі та периферія
    "11265",  # > Клавіатури
    "12271",  # > Комп'ютерні корпуси
    # Електроніка та аксесуари
    "11887",  # > Кабелі та перехідники
    # Мережеве обладнання
    "12345",  # > Інструмент
    "12363",  # > Пасивне мережеве обладнання > Мережеві конектори, розетки, модулі
    "12362",  # > Пасивне мережеве обладнання > Патч-корди
    "12335",  # > Електроустаткування > Стабілізатори напруги
})

# Статуси товарів, які допускаються до обробки.
# inStock         — В наявності
# quickProduction — Швидке виробництво
ALLOWED_STATUSES: frozenset[str] = frozenset({
    "inStock",          # В наявності
    "quickProduction",  # Швидке виробництво
})

# Паттерн для відокремлення числового значення та одиниці з рядка LP API.
# LP B2B API не має окремого поля unit — одиниця вбудована у рядок значення.
#
# Приклади:
#   "3,60 кг"  → group(1)="3,60",  group(2)="кг"
#   "260 мм"   → group(1)="260",   group(2)="мм"
#   "625 ВА"   → group(1)="625",   group(2)="ВА"
#   "циліндр"  → no match → value="циліндр", unit=""
#
# Порядок одиниць: довші перед коротшими (Вт·год перед Вт, мм перед м, кг перед г).
_SPEC_UNIT_RE = re.compile(
    r'^([\d][0-9\s,\.]*)\s+'
    r'(Вт·год|кВт·год|А·год|А·г|мА·год|мА·г|ВА|кВт|Вт|мА|кВ|МГц|кГц|ТБ|ГБ|МБ|кг|мм|см|А|В|Гц|м|г|%|°C)$',
    re.IGNORECASE,
)


# ─────────────────────────────────────────────────────────────────────
# Spider
# ─────────────────────────────────────────────────────────────────────

class LpApiSpider(BaseDealerSpider):
    """LogicPower B2B API — dealer spider (USD, businessEntity)."""

    name            = "lp_api"
    supplier_id     = "lp"
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
        "ROBOTSTXT_OBEY":                 False,
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
        self.usd_rate: Decimal = Decimal("0")

        self.category_codes: set[str]   = self._load_category_codes()
        self.processed_codes: set[str]  = self._load_processed_codes()

        self._stats: dict[str, int] = {
            "yielded":                0,
            "skip_not_target_mfr":    0,
            "skip_unavailable":       0,
            "skip_resume":            0,
            "skip_excluded_category": 0,
            "skip_no_category":       0,
            "skip_no_price":          0,
        }

        # Групування відсутніх категорій для компактного звіту в closed()
        self._missing_categories: dict[str, dict] = {}

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
        return {"X-Api-Key": self.api_token, "Accept": "application/json"}

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
                entry.get("paymentType")    == "businessEntity"
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
        data  = response.json().get("data", {})
        items = data.get("items", [])
        total = data.get("totalItems", 0)
        self.logger.info(f"📄 Сторінка {page}: {len(items)} товарів (всього {total})")
        for product in items:
            item = self._build_item(product)
            if item is not None:
                yield item
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

        # 0. Тільки цільові виробники (matching за slug — див. коментар біля TARGET_MANUFACTURERS)
        mfr = ((product.get("manufacturer") or {}).get("slug") or "").strip().lower()
        if mfr not in TARGET_MANUFACTURERS:
            self._stats["skip_not_target_mfr"] += 1
            return None

        # 1. Тільки допустимі статуси: inStock + quickProduction
        if status not in ALLOWED_STATUSES:
            self._stats["skip_unavailable"] += 1
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
        categories = product.get("categories", [])

        # 4a. Службові leaf-категорії — пропускаємо одразу
        leaf_code = str(categories[-1].get("code", "")).strip() if categories else ""
        if leaf_code in EXCLUDED_CATEGORY_CODES:
            self._stats["skip_excluded_category"] += 1
            return None

        # 4b. Пошук у lp_category.csv
        category_code = self._resolve_category(categories)
        if category_code is None:
            self._stats["skip_no_category"] += 1
            self._record_missing_category(code, categories)
            return None

        # 5. Наявність і кількість
        avail_val, qty_val = LpQuickProductionService.resolve(
            status, product.get("availability")
        )

        self._stats["yielded"] += 1
        name_uk = self._loc(product, "name", "uk")
        self.logger.info(
            f"✅ [{code}] {name_uk[:60]} | "
            f"USD {personal_usd} | cat={category_code} | "
            f"avail={avail_val!r} qty={qty_val!r}"
        )

        return {
            "supplier_id":          self.supplier_id,
            "output_file":          self.output_filename,
            "source":               "api",
            "Ідентифікатор_товару": code,
            "Назва_позиції":        self._loc(product, "name", "ru"),
            "Назва_позиції_укр":    name_uk,
            "Опис":                 self._loc(product, "description", "ru"),
            "Опис_укр":             self._loc(product, "description", "uk"),
            "Тип_товару":           "r",
            "Одиниця_виміру":       "шт.",
            "Ціна":                 personal_usd,
            "Валюта":               "USD",
            "usd_rate":             str(self.usd_rate),
            "price_rrp_uah":        rrp_uah,
            "Наявність":            avail_val,
            "Кількість":            qty_val,
            "Виробник":             (product.get("manufacturer") or {}).get("name", ""),
            "Країна_виробник":      "",
            "Посилання_зображення": self._extract_images(product.get("images", [])),
            "Продукт_на_сайті":     product.get("externalUrl") or "",
            "category_url":         "",
            "category_id":          category_code,
            "feed_id":              "",
            "Пошукові_запити":      "",
            "Пошукові_запити_укр":  "",
            "specifications_list":  self._extract_specs(product.get("specifications", [])),
        }

    # ──────────────────────────────────────────────────────────────────
    # HELPERS
    # ──────────────────────────────────────────────────────────────────

    def _resolve_category(self, categories: list[dict]) -> str | None:
        """
        Знаходить category code за product.categories[] × lp_category.csv.
        Обхід у зворотному порядку (deepest first).
        Bootstrap-режим: якщо CSV порожній — повертає leaf без валідації.
        """
        if not categories:
            return None
        if not self.category_codes:
            return str(categories[-1].get("code", "")).strip() or None
        for cat in reversed(categories):
            code = str(cat.get("code", "")).strip()
            if code in self.category_codes:
                return code
        return None

    def _record_missing_category(self, product_code: str, categories: list[dict]) -> None:
        """Групує відсутні категорії для звіту в closed()."""
        raw_codes = [str(c.get("code", "")).strip() for c in categories]
        raw_names = [self._category_label(c) for c in categories]
        leaf_code = raw_codes[-1] if raw_codes else "—"
        leaf_name = raw_names[-1] if raw_names and raw_names[-1] else ""
        self.logger.warning(
            f"⚠️ Категорія не в lp_category.csv: code={product_code} cats={raw_codes}"
        )
        bucket = self._missing_categories.setdefault(
            leaf_code,
            {"count": 0, "name": leaf_name, "chain": raw_codes, "examples": []},
        )
        bucket["count"] += 1
        if not bucket["name"] and leaf_name:
            bucket["name"] = leaf_name
        if len(bucket["examples"]) < 3:
            bucket["examples"].append(product_code)

    @staticmethod
    def _loc(obj: dict, field: str, lang: str) -> str:
        """Безпечно повертає локалізоване поле. uk → fallback ru."""
        inner = obj.get(field) or {}
        if isinstance(inner, dict):
            fallback = "ru" if lang == "uk" else "uk"
            return (inner.get(lang) or inner.get(fallback) or "").strip()
        return str(inner).strip()

    @staticmethod
    def _category_label(cat: dict) -> str:
        """Читабельна назва категорії (best-effort, поле не гарантоване API)."""
        raw = cat.get("name") or cat.get("title") or ""
        if isinstance(raw, dict):
            return (raw.get("uk") or raw.get("ru") or "").strip()
        return str(raw).strip()

    @staticmethod
    def _extract_prices(prices: list[dict]) -> tuple[str, str]:
        """
        Повертає (personal_usd, rrp_uah).
          personal          → тип "personal",          валюта USD
          recommendedRetail → тип "recommendedRetail",  валюта UAH
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
        Парсинг specifications LP B2B API.

        Структура відповіді:
          {
            "id": 1,
            "name":   {"ru": "Тип корпуса", "uk": "Тип корпусу"},
            "option": {
              "id": 2,
              "value": {"ru": "цилиндрический", "uk": "циліндричний"}
            }
          }

        Значення ЗАВЖДИ в spec["option"]["value"], НЕ в spec["value"].
        LP API не має окремого поля unit — витягуємо його з рядка значення
        через _SPEC_UNIT_RE: "3,60 кг" → value="3,60", unit="кг".
        Це необхідно для коректної роботи FieldProcessor.extract_dimensions_from_specs().
        """
        result: list[dict] = []
        for s in specs_raw:
            # name — локалізований dict на верхньому рівні
            name_obj = s.get("name") or {}
            if isinstance(name_obj, dict):
                name = (name_obj.get("uk") or name_obj.get("ru") or "").strip()
            else:
                name = str(name_obj).strip()

            # value — ВКЛАДЕНО в option.value (НЕ spec.value)
            option    = s.get("option") or {}
            value_obj = option.get("value") or {}
            if isinstance(value_obj, dict):
                value_raw = (value_obj.get("uk") or value_obj.get("ru") or "").strip()
            else:
                value_raw = str(value_obj).strip()

            # Витягуємо одиницю з рядка: "3,60 кг" → value="3,60", unit="кг"
            m = _SPEC_UNIT_RE.match(value_raw)
            if m:
                value = m.group(1).strip()
                unit  = m.group(2)
            else:
                value = value_raw
                unit  = ""

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
        if not response.json().get("status"):
            self.logger.error(f"❌ API status=false [{label}]")
            return False
        return True

    def _on_error(self, failure) -> None:
        self.logger.error(
            f"❌ Request failed: {failure.value} | URL: {failure.request.url}"
        )

    # ──────────────────────────────────────────────────────────────────
    # CLOSE
    # ──────────────────────────────────────────────────────────────────

    def closed(self, reason: str) -> None:
        s = self._stats
        self.logger.info(
            f"🎉 lp_api завершено ({reason})\n"
            f"   yielded={s['yielded']}  "
            f"skip_not_target_mfr={s['skip_not_target_mfr']}  "
            f"skip_unavailable={s['skip_unavailable']}  "
            f"skip_resume={s['skip_resume']}  "
            f"skip_excluded_category={s['skip_excluded_category']}  "
            f"skip_no_category={s['skip_no_category']}  "
            f"skip_no_price={s['skip_no_price']}"
        )
        if self._missing_categories:
            rows = sorted(
                self._missing_categories.items(),
                key=lambda kv: kv[1]["count"],
                reverse=True,
            )
            lines = "\n".join(
                f"   \u2022 code={leaf}"
                + (f'  \u043dазва="{data["name"]}"' if data["name"] else "")
                + f"  товарів={data['count']}  "
                f"ланцюжок={data['chain']}  приклади={data['examples']}"
                for leaf, data in rows
            )
            border = "=" * 70
            self.logger.critical(
                f"\n{border}\n"
                f"\U0001f6a8  КРИТИЧНА ПОМИЛКА: паук знайшов {len(rows)} нових категорій LP,\n"
                f"   яких НЕМАЄ у файлі маппінгу data/lp/lp_category.csv.\n"
                f"   Пропущено товарів: {s['skip_no_category']}\n\n"
                f"   Виконайте маппінг для цих категорій і перезапустіть паука:\n"
                f"     1. python scripts/lp_export_categories.py\n"
                f"     2. Заповніть колонки маппінгу в data/lp/lp_category.csv\n"
                f"     3. Перезапустіть паука\n\n"
                f"   Нові коди категорій:\n{lines}\n"
                f"{border}"
            )
            sys.exit(1)
        try:
            import winsound
            for _ in range(3):
                winsound.Beep(1000, 300)
        except Exception:
            pass
