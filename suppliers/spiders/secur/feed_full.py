"""
Spider об'єднаний: Feed (ціни) + Playwright (зображення + хар-ки якщо потрібно)

ЛОГІКА:
  1. UA фід (XML) → кешує: назви, описи, <param> хар-ки (УКРАЇНСЬКОЮ)
  2. RU фід (XML) → збирає дані, формує чергу Playwright-запитів
  3. Playwright (UA-сторінка товару) → завжди: зображення
                                      умовно: хар-ки (тільки якщо XML не мав <param>)
  4. CategorySpecsEnricher → доповнення категорійними хар-ками з CSV

ПОТІК ЗАПИТІВ:
  start()
    └─ parse_ua_feed()     [Playwright, XML — кешує назви/описи/<param>]
         └─ parse_ru_feed()     [Playwright, XML — збирає ціни + формує чергу]
              └─ parse_product_page()  [Playwright — зображення + хар-ки за потреби]
                   └─ yield item

  ПРИМІТКА: Фіди завантажуються через Playwright (браузер) щоб обійти
  Cloudflare, який блокує прямі HTTP-запити з GitHub Actions IP.

ФІДИ ТА ЛОГІКА ЦІН:
  Фід 50 (Ajax, крупний опт):
    → ціна з <price> (роздрібна), множиться на coefficient_feed у pipeline
  Фід 52 (Імпорт, крупний опт):
    → ціна з <dealerPrice> (дилерська), множиться на coefficient_feed
    → захист: якщо dealerPrice > price → беремо меншу (price)

ХАРАКТЕРИСТИКИ (ЗАВЖДИ УКРАЇНСЬКОЮ):
  XML має <param> → використовуємо їх (з UA фіду = українська мова)
  XML без <param> → парсимо зі сторінки (UA URL = українська мова)
  У обох випадках → доповнюємо через CategorySpecsEnricher

  ВАЖЛИВО: URL з RU фіду може містити /ru/ → стрипаємо в parse_ru_feed
  щоб Playwright завжди відкривав UA-версію сторінки.

ЗОБРАЖЕННЯ:
  Фід дає лише 1 фото → Playwright завжди відкриває сторінку і парсить
  усі великі фото (/images/big/) ідентично retail.py

ГАБАРИТИ (Вага,кг / Ширина,см / Висота,см / Довжина,см):
  Pipeline автоматично витягує через field_processor.extract_dimensions_from_specs()
  Працює коректно бо хар-ки подаються УКРАЇНСЬКОЮ (pipeline шукає укр. назви)

ДЕДУПЛІКАЦІЯ:
  processed_seen: set[tuple[feed_id, product_url]] — ключ містить feed_id,
  тому однаковий URL з різних фідів НЕ вважається дублем.
  Дублі відсіюються тільки всередині одного фіду.

BAN / RETRY:
  BAN в parse-колбеку → _ban_retry_or_next() → повертає Request (retry) або None.
    НЕ викликаємо errback_feed() — він повертає Deferred, який Scrapy не приймає
    як item від генератора → "Error processing <Deferred>".
  BAN/мережева помилка в errback → errback_feed() → Deferred (коректно для errback).

АНТИБОТ (3 шари):
  1. --disable-blink-features=AutomationControlled (Chrome CLI arg)
  2. playwright-stealth via StealthPlaywrightDownloadHandler (context-level init scripts)
     → патчить navigator.webdriver, navigator.plugins, window.chrome та ~15 інших
  3. Реальний UA, мовні заголовки, затримки між запитами

ПРИМІТКИ:
  - Фід 54 (Україна) видалено.
  - errback_product: Playwright впав → yield з фід-зображенням без хар-к зі сторінки.
  - CategorySpecsEnricher підключений безпосередньо в пауку (специфічний для secur).
"""

import re
import csv
from pathlib import Path

import scrapy
from scrapy.selector import Selector
from scrapy_playwright.page import PageMethod
from twisted.internet import reactor, defer

from suppliers.services.category_specs_enricher import CategorySpecsEnricher


# ─────────────────────────────────────────────────────────────
# Константи Playwright (ті ж що і в retail.py)
# ─────────────────────────────────────────────────────────────
GOTO_TIMEOUT_MS = 15_000   # page.goto() timeout — 15 сек (fail fast на банах)
DOWNLOAD_TIMEOUT = 18      # Scrapy-рівень — 18 сек (трохи більше goto)

# Два сценарії:
#   1. Товар з хар-ками → Vue рендерить div.item (може бути повільно → 10с)
#   2. Товар без хар-к  → div.item немає, але h1 є завжди → не таймаутимо
_WAIT_SELECTOR = "div.item, h1"
_WAIT_TIMEOUT_MS = 10_000

# Сигнали бану — перевіряємо перші 500 символів відповіді
BAN_SIGNALS = frozenset({
    "captcha",
    "access denied",
    "cloudflare",
    "403 forbidden",
    "too many requests",
    "just a moment",       # Cloudflare challenge page
    "enable javascript",   # Cloudflare JS-fallback
    "checking your browser",
})

# Retry-налаштування для XML-фідів
FEED_MAX_RETRIES = 3
FEED_RETRY_DELAY = 180  # секунд між спробами


def _is_banned(text: str) -> bool:
    """Перевіряє перші 500 символів на сигнали бану. O(1) по кількості сигналів."""
    preview = text[:500].lower()
    return any(signal in preview for signal in BAN_SIGNALS)


def _playwright_meta(extra: dict | None = None) -> dict:
    """Playwright-мета з жорсткими timeout-ами. Тільки для сторінок товарів."""
    base = {
        "playwright": True,
        "download_timeout": DOWNLOAD_TIMEOUT,
        "playwright_page_goto_kwargs": {
            "wait_until": "domcontentloaded",
            "timeout": GOTO_TIMEOUT_MS,
        },
        "playwright_page_methods": [
            PageMethod(
                "wait_for_selector",
                _WAIT_SELECTOR,
                timeout=_WAIT_TIMEOUT_MS,
                state="attached",
            ),
        ],
    }
    if extra:
        base.update(extra)
    return base


def _playwright_feed_meta(extra: dict | None = None) -> dict:
    """
    Playwright-мета для XML-фідів.
    domcontentloaded замість networkidle — networkidle зависає на CI
    (Cloudflare challenge тримає відкриті з'єднання → networkidle ніколи
    не настає → timeout → 0 товарів без помилки).
    """
    base = {
        "playwright": True,
        "download_timeout": 60,
        "playwright_page_goto_kwargs": {
            "wait_until": "domcontentloaded",
            "timeout": 45_000,
        },
    }
    if extra:
        base.update(extra)
    return base


def _extract_xml_from_browser_response(text: str) -> str:
    """
    Браузер відкриває XML і огортає його в HTML: <html><body><pre>...xml...</pre></body></html>
    Або повертає сирий XML якщо вже є <?xml декларація.
    """
    stripped = text.strip()
    if stripped.startswith("<?xml") or stripped.startswith("<yml_catalog"):
        return stripped
    # Chrome огортає XML у <pre id="webkit-xml-viewer-source-xml">
    m = re.search(r'<pre[^>]*>(.*?)</pre>', stripped, re.DOTALL)
    if m:
        return m.group(1).strip()
    # Витягуємо від першого XML-тегу
    for marker in ("<?xml", "<yml_catalog"):
        start = stripped.find(marker)
        if start != -1:
            return stripped[start:]
    return text


class SecurFeedFullSpider(scrapy.Spider):
    """
    Об'єднаний паук: ціни з фіду + зображення і хар-ки зі сторінки товару.

    Ajax (крупний опт):   https://secur.ua/feed/export/50  → ціна з <price>
    Імпорт (крупний опт): https://secur.ua/feed/export/52  → ціна з <dealerPrice>
    """

    name = "secur_feed_full"
    supplier_id = "secur"
    allowed_domains = ["secur.ua"]

    ALL_FEED_IDS: list[str] = ["50", "52"]  # Фід 54 (Україна) видалено

    custom_settings = {
        "ITEM_PIPELINES": {
            "suppliers.pipelines.SuppliersPipeline": 300,
        },
        # ── Stealth handler замінює стандартний scrapy-playwright ──────────
        # StealthPlaywrightDownloadHandler.`_create_browser_context` застосовує
        # playwright-stealth до кожного нового BrowserContext одразу після створення.
        # context.add_init_script → спрацьовує ДО будь-якого JS (включно з CF).
        "DOWNLOAD_HANDLERS": {
            "http": "suppliers.middlewares.StealthPlaywrightDownloadHandler",
            "https": "suppliers.middlewares.StealthPlaywrightDownloadHandler",
        },
        "TWISTED_REACTOR": "twisted.internet.asyncioreactor.AsyncioSelectorReactor",
        # ── Антибот: 2 паралельні Playwright-вкладки, затримка 1.5–4.5 сек ──
        "CONCURRENT_REQUESTS": 4,
        "CONCURRENT_REQUESTS_PER_DOMAIN": 2,
        "DOWNLOAD_DELAY": 3,
        "RANDOMIZE_DOWNLOAD_DELAY": True,        # 3 × 0.5..1.5 → 1.5–4.5 сек
        # ── AutoThrottle ────────────────────────────────────────────────────
        "AUTOTHROTTLE_ENABLED": True,
        "AUTOTHROTTLE_START_DELAY": 3,
        "AUTOTHROTTLE_MAX_DELAY": 60,
        "AUTOTHROTTLE_TARGET_CONCURRENCY": 2.0,
        "AUTOTHROTTLE_DEBUG": False,
        # ── Блокуємо зайві ресурси — нам потрібен тільки DOM ────────────────
        "PLAYWRIGHT_ABORT_REQUEST": lambda req: req.resource_type in {
            "image", "media", "font", "stylesheet"
        },
        "DOWNLOAD_TIMEOUT": DOWNLOAD_TIMEOUT,
        "PLAYWRIGHT_DEFAULT_NAVIGATION_TIMEOUT": GOTO_TIMEOUT_MS,
        "PLAYWRIGHT_LAUNCH_OPTIONS": {
            "args": [
                "--disable-blink-features=AutomationControlled",  # ← шар 1 антибот
                "--disable-dev-shm-usage",
                "--no-sandbox",
                "--disable-gpu",
                "--js-flags=--max-old-space-size=512",
            ],
        },
        "PLAYWRIGHT_CONTEXTS": {
            "default": {
                "user_agent": (
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/133.0.0.0 Safari/537.36"
                ),
                "extra_http_headers": {
                    "Accept-Language": "uk-UA,uk;q=0.9,en-US;q=0.8,en;q=0.7",
                },
            }
        },
        "HTTPERROR_ALLOW_ALL": True,
        "RETRY_ENABLED": False,
    }

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.output_filename = "secur_new.csv"
        self.currency = "UAH"
        self.price_type = "retail"

        self.category_mapping = self._load_category_mapping()

        import os as _os
        _root = Path(_os.environ.get("PROJECT_ROOT", r"C:\FullStack\PriceFeedPipeline"))
        csv_path = str(_root / "data" / "secur" / "secur_category.csv")
        self.category_enricher = CategorySpecsEnricher(csv_path, self.supplier_id)

        # Кеш UA-даних: назви, описи, хар-ки з <param> (УКРАЇНСЬКОЮ)
        self.products_ua: dict = {}

        # Дедуплікація всередині кожного фіду.
        # Ключ: (feed_id, product_url) — однаковий URL у різних фідах НЕ є дублем.
        self.processed_seen: set[tuple[str, str]] = set()

        # ── RESUME: відновлюємо вже оброблені URL з попереднього запуску ──
        already_scraped = self._load_already_scraped_urls()
        for url in already_scraped:
            for fid in self.ALL_FEED_IDS:
                self.processed_seen.add((fid, url))

    # ──────────────────────────────────────────────────────────────────
    # RESUME
    # ──────────────────────────────────────────────────────────────────

    def _load_already_scraped_urls(self) -> set[str]:
        """
        Читає вже збережений secur_new.csv і повертає set URL.
        Якщо файл не існує — порожній set (перший запуск).
        """
        urls: set[str] = set()
        import os as _os
        out_path = (
            Path(_os.environ.get("PROJECT_ROOT", r"C:\FullStack\PriceFeedPipeline"))
            / "data" / "output" / self.output_filename
        )
        if not out_path.exists():
            return urls
        try:
            with open(out_path, encoding="utf-8-sig") as f:
                reader = csv.DictReader(f, delimiter=";")
                for row in reader:
                    url = row.get("Продукт_на_сайті", "").strip()
                    if url:
                        urls.add(url)
            self.logger.info(
                f"📋 Resume: знайдено {len(urls)} вже спарсених товарів — пропускаємо"
            )
        except Exception as e:
            self.logger.error(f"⚠️ Не вдалося завантажити прогрес resume: {e}")
        return urls

    # ──────────────────────────────────────────────────────────────────
    # CATEGORY MAPPING
    # ──────────────────────────────────────────────────────────────────

    def _load_category_mapping(self) -> dict:
        # Ключ: (feed_id, category_id)
        # Delete-рядки мають порожній feed → ключ: ("", category_id)
        mapping: dict = {}
        import os as _os
        csv_path = (
            Path(_os.environ.get("PROJECT_ROOT", r"C:\FullStack\PriceFeedPipeline"))
            / "data" / "secur" / "secur_category.csv"
        )
        try:
            with open(csv_path, encoding="utf-8-sig") as f:
                reader = csv.DictReader(f, delimiter=";")
                for row in reader:
                    if row.get("channel", "").strip() != "site":
                        continue
                    category_id = row.get("category id", "").strip()
                    if not category_id:
                        continue
                    allowed_feed = row.get("feed", "").strip()
                    key = (allowed_feed, category_id)
                    if key in mapping:
                        continue
                    mapping[key] = {
                        "category_name":    row.get("Назва_групи", ""),
                        "group_number":     row.get("Номер_групи", ""),
                        "subdivision_id":   row.get("Ідентифікатор_підрозділу", "").strip(),
                        "subdivision_link": row.get("Посилання_підрозділу", ""),
                        "category_url":     row.get("Линк категории поставщика", "").strip().strip('"'),
                        "feed":             allowed_feed,
                    }
            self.logger.info(f"✅ Завантажено {len(mapping)} category mappings")
        except Exception as e:
            self.logger.error(f"❌ Помилка завантаження category mappings: {e}")
        return mapping

    def _is_deleted_category(self, category_id: str) -> bool:
        return self.category_mapping.get(("", category_id), {}).get("subdivision_id", "").lower() == "delete"

    def _is_allowed_for_feed(self, category_id: str, feed_id: str) -> bool:
        return (
            (feed_id, category_id) in self.category_mapping
            or ("", category_id) in self.category_mapping
        )

    # ──────────────────────────────────────────────────────────────────
    # BAN RETRY (parse-контекст)
    # ──────────────────────────────────────────────────────────────────

    def _ban_retry_or_next(
        self,
        response,
        feed_id: str,
        feed_index: int,
        label: str,
    ) -> scrapy.Request | None:
        """
        Викликається коли BAN виявлено всередині parse-колбеку (НЕ в errback).

        ВАЖЛИВО: errback_feed() не можна викликати з parse-генератора, бо
        він повертає Deferred — Scrapy приймає його тільки з errback-ланцюжка,
        але НЕ як item від генератора (→ "Error processing <Deferred>").

        Повертає:
          - scrapy.Request (retry) — якщо ліміт спроб не вичерпано
          - scrapy.Request (наступний фід) — якщо всі спроби вичерпано і є наступний фід
          - None — якщо всі фіди вичерпано
        """
        retry_count = response.meta.get("feed_retry_count", 0)

        self.logger.error(
            f"🚫 БАН {label} ФІДУ {feed_id} → Cloudflare замість XML | URL: {response.url}"
        )

        if retry_count < FEED_MAX_RETRIES:
            self.logger.warning(
                f"⚠️ Фід {feed_id}: спроба {retry_count + 1}/{FEED_MAX_RETRIES} "
                f"— повтор через {FEED_RETRY_DELAY}с"
            )
            req = response.request.copy()
            req.meta["feed_retry_count"] = retry_count + 1
            req.dont_filter = True
            return req

        self.logger.error(
            f"❌ Фід {feed_id} недоступний після {FEED_MAX_RETRIES} спроб — пропускаємо"
        )
        next_index = feed_index + 1
        if next_index < len(self.ALL_FEED_IDS):
            self.logger.info(
                f"➡️  Пропускаємо до фіду [{next_index + 1}/{len(self.ALL_FEED_IDS)}]: "
                f"{self.ALL_FEED_IDS[next_index]}"
            )
            return self._ua_request(feed_index=next_index)
        return None

    # ──────────────────────────────────────────────────────────────────
    # CRAWLING
    # ──────────────────────────────────────────────────────────────────

    async def start(self):
        self.logger.info(
            f"🚀 Починаю обробку {len(self.ALL_FEED_IDS)} фідів: {self.ALL_FEED_IDS}"
        )
        yield self._ua_request(feed_index=0)

    def _ua_request(self, feed_index: int) -> scrapy.Request:
        fid = self.ALL_FEED_IDS[feed_index]
        return scrapy.Request(
            url=f"https://secur.ua/feed/export/{fid}",
            callback=self.parse_ua_feed,
            errback=self.errback_feed,
            meta=_playwright_feed_meta({"feed_id": fid, "feed_index": feed_index}),
            dont_filter=True,
        )

    def parse_ua_feed(self, response):
        """
        UA фід (XML, Playwright) — кешує для кожного товару:
          - name_ua, description_ua
          - specs_from_feed: хар-ки з <param> УКРАЇНСЬКОЮ мовою

        BAN → _ban_retry_or_next() (НЕ errback_feed — він повертає Deferred,
        який не можна yield з parse-генератора).
        """
        feed_id = response.meta["feed_id"]
        feed_index = response.meta["feed_index"]

        self.logger.info(
            f"📂 [{feed_index + 1}/{len(self.ALL_FEED_IDS)}] UA фід {feed_id} "
            f"| len={len(response.text)} | preview: {response.text[:200]!r}"
        )

        if _is_banned(response.text):
            req = self._ban_retry_or_next(response, feed_id, feed_index, "UA")
            if req:
                yield req
            return

        xml_text = _extract_xml_from_browser_response(response.text)
        selector = Selector(text=xml_text, type="xml")
        selector.remove_namespaces()

        self.products_ua = {}
        with_params = 0

        for offer in selector.xpath("//offer"):
            product_id = offer.xpath("@id").get()
            if not product_id:
                continue
            category_id = offer.xpath("categoryId/text()").get() or ""
            if self._is_deleted_category(category_id):
                continue

            name_ua = offer.xpath("name/text()").get() or ""
            name_ua = re.sub(r'\s*\([A-Z0-9\.]+\)\s*$', '', name_ua).strip()

            specs_from_feed = self._parse_feed_specs(offer)
            if specs_from_feed:
                with_params += 1

            self.products_ua[product_id] = {
                "name_ua":         name_ua,
                "description_ua":  self._clean_description(
                    offer.xpath("description/text()").get()
                ),
                "specs_from_feed": specs_from_feed,
            }

        self.logger.info(
            f"✅ UA фід {feed_id}: {len(self.products_ua)} товарів у кеші "
            f"({with_params} з <param> хар-ками)"
        )

        yield scrapy.Request(
            url=f"https://secur.ua/feed/export/{feed_id}?lang=ru",
            callback=self.parse_ru_feed,
            errback=self.errback_feed,
            meta=_playwright_feed_meta({"feed_id": feed_id, "feed_index": feed_index}),
            dont_filter=True,
        )

    def parse_ru_feed(self, response):
        """
        RU фід (XML, Playwright) — об'єднує з UA кешем, формує чергу Playwright-запитів.

        Дедуплікація: ключ (feed_id, product_url) — дозволяє одному URL
        з'явитися в різних фідах як різні товари.
        """
        feed_id = response.meta["feed_id"]
        feed_index = response.meta["feed_index"]

        self.logger.info(
            f"📂 [{feed_index + 1}/{len(self.ALL_FEED_IDS)}] RU фід {feed_id} "
            f"| len={len(response.text)} | preview: {response.text[:200]!r}"
        )

        if _is_banned(response.text):
            req = self._ban_retry_or_next(response, feed_id, feed_index, "RU")
            if req:
                yield req
            return

        xml_text = _extract_xml_from_browser_response(response.text)
        selector = Selector(text=xml_text, type="xml")
        selector.remove_namespaces()

        total = queued = deleted = skipped = 0
        unmapped_categories: set[str] = set()

        for offer in selector.xpath("//offer"):
            product_id = offer.xpath("@id").get()
            category_id = offer.xpath("categoryId/text()").get() or ""

            if self._is_deleted_category(category_id):
                deleted += 1
                continue

            total += 1

            name_ru = offer.xpath("name/text()").get() or ""
            name_ru = re.sub(r'\s*\([A-Z0-9\.]+\)\s*$', '', name_ru).strip()
            description_ru = self._clean_description(offer.xpath("description/text()").get())

            ua_data = self.products_ua.get(product_id, {})

            if (
                (feed_id, category_id) not in self.category_mapping
                and ("", category_id) not in self.category_mapping
            ):
                unmapped_categories.add(category_id)

            feed_item = self._build_feed_item(
                offer=offer,
                feed_id=feed_id,
                name_ru=name_ru,
                name_ua=ua_data.get("name_ua", name_ru),
                description_ru=description_ru,
                description_ua=ua_data.get("description_ua", description_ru),
                category_id=category_id,
            )

            if not feed_item:
                continue

            product_url = feed_item.pop("_product_url", "")

            # URL з RU фіду може містити /ru/ → стрипаємо для UA-версії сторінки
            product_url = product_url.replace("secur.ua/ru/", "secur.ua/")
            feed_item["Продукт_на_сайті"] = product_url

            specs_from_feed: list = ua_data.get("specs_from_feed", [])

            if not product_url:
                feed_item["specifications_list"] = specs_from_feed
                skipped += 1
                yield feed_item
                continue

            # Ключ містить feed_id → однаковий URL у різних фідах НЕ є дублем
            dedup_key = (feed_id, product_url)
            if dedup_key in self.processed_seen:
                skipped += 1
                self.logger.debug(f"⏭️  Дублікат у фіді {feed_id}: {product_url}")
                continue

            self.processed_seen.add(dedup_key)
            queued += 1

            yield scrapy.Request(
                url=product_url,
                callback=self.parse_product_page,
                meta=_playwright_meta({
                    "feed_item":       feed_item,
                    "feed_id":         feed_id,
                    "category_id":     category_id,
                    "specs_from_feed": specs_from_feed,
                }),
                dont_filter=True,
                errback=self.errback_product,
            )

        self.logger.info(
            f"✅ Фід {feed_id}: {total} товарів | {queued} → Playwright | "
            f"{deleted} видалено (уцінка) | {skipped} пропущено"
        )
        if unmapped_categories:
            self.logger.warning(
                f"⚠️ Фід {feed_id} — unmapped categories: {sorted(unmapped_categories)}"
            )

        next_index = feed_index + 1
        if next_index < len(self.ALL_FEED_IDS):
            self.logger.info(
                f"➡️  Перехід до фіду [{next_index + 1}/{len(self.ALL_FEED_IDS)}]: "
                f"{self.ALL_FEED_IDS[next_index]}"
            )
            yield self._ua_request(feed_index=next_index)
        else:
            self.logger.info("🎉 Всі фіди прочитані! Очікую завершення Playwright-черги...")

    # ──────────────────────────────────────────────────────────────────
    # PLAYWRIGHT: зображення + хар-ки (якщо XML не мав <param>)
    # ──────────────────────────────────────────────────────────────────

    def parse_product_page(self, response):
        """
        UA-сторінка товару (Playwright):
          - ЗАВЖДИ: парсить усі великі зображення
          - УМОВНО: парсить хар-ки, якщо XML не мав жодного <param>
        """
        feed_item = response.meta["feed_item"]
        feed_id = response.meta.get("feed_id", "")
        category_id = response.meta.get("category_id", "")
        specs_from_feed: list = response.meta.get("specs_from_feed", [])

        if response.status != 200:
            self.logger.warning(
                f"⚠️ HTTP {response.status} → {response.url} | yield з фід-даними"
            )
            feed_item["specifications_list"] = specs_from_feed
            yield feed_item
            return

        if _is_banned(response.text):
            self.logger.warning(f"🚫 БАН ДЕТЕКТОВАНО → {response.url}")
            feed_item["specifications_list"] = specs_from_feed
            yield feed_item
            return

        # ── 1. ЗОБРАЖЕННЯ (завжди) ─────────────────────────────────────
        page_image_url = self._parse_images(response)
        if page_image_url:
            feed_item["Посилання_зображення"] = page_image_url

        # ── 2. ХАРАКТЕРИСТИКИ (УКРАЇНСЬКОЮ) ───────────────────────────
        if specs_from_feed:
            specs_list = specs_from_feed
            source_label = "XML"
        else:
            specs_list = self._parse_specifications(response)
            source_label = "сторінка"

        specs_list = self.category_enricher.enrich_specs_by_category_id(
            specs_list, category_id, feed_id
        )
        feed_item["specifications_list"] = specs_list

        self.logger.info(
            f"✅ {feed_item.get('Назва_позиції', '')[:50]} | "
            f"Ціна: {feed_item.get('Ціна', '')} UAH | "
            f"Хар-к: {len(specs_list)} ({source_label}) | "
            f"Фото: {len(page_image_url.split(', ')) if page_image_url else 0}"
        )
        yield feed_item

    # ──────────────────────────────────────────────────────────────────
    # ERRBACKS
    # ──────────────────────────────────────────────────────────────────

    def errback_feed(self, failure):
        """
        Errback для реальних мережевих помилок XML-фідів (UA та RU).
        Ретраїть до FEED_MAX_RETRIES разів з паузою FEED_RETRY_DELAY секунд.

        Повертає Deferred — це коректно ТІЛЬКИ з errback-ланцюжка.
        НЕ викликати з parse-генератора (→ "Error processing <Deferred>").
        """
        request = failure.request
        retry_count = request.meta.get("feed_retry_count", 0)
        feed_id = request.meta.get("feed_id", "?")
        feed_index = request.meta.get("feed_index", 0)

        if retry_count < FEED_MAX_RETRIES:
            self.logger.warning(
                f"⚠️ Помилка завантаження фіду {feed_id} "
                f"(спроба {retry_count + 1}/{FEED_MAX_RETRIES}): {failure.value} "
                f"— повтор через {FEED_RETRY_DELAY}с | URL: {request.url}"
            )
            new_req = request.copy()
            new_req.meta["feed_retry_count"] = retry_count + 1
            new_req.dont_filter = True

            d: defer.Deferred = defer.Deferred()
            reactor.callLater(FEED_RETRY_DELAY, d.callback, new_req)
            return d

        self.logger.error(
            f"❌ Фід {feed_id} недоступний після {FEED_MAX_RETRIES} спроб "
            f"— пропускаємо | URL: {request.url}"
        )
        next_index = feed_index + 1
        if next_index < len(self.ALL_FEED_IDS):
            self.logger.info(
                f"➡️  Пропускаємо до фіду [{next_index + 1}/{len(self.ALL_FEED_IDS)}]: "
                f"{self.ALL_FEED_IDS[next_index]}"
            )
            return self._ua_request(feed_index=next_index)

    def errback_product(self, failure):
        """Playwright впав (timeout/network) → yield з фід-даними. Товар не губиться."""
        self.logger.error(f"❌ Playwright помилка: {failure.value}")
        self.logger.error(f"   URL: {failure.request.url}")
        feed_item = failure.request.meta.get("feed_item", {})
        if not feed_item:
            return

        specs_from_feed = failure.request.meta.get("specs_from_feed", [])
        category_id = failure.request.meta.get("category_id", "")
        feed_id = failure.request.meta.get("feed_id", "")

        specs = self.category_enricher.enrich_specs_by_category_id(
            specs_from_feed, category_id, feed_id
        )
        feed_item["specifications_list"] = specs

        self.logger.info(
            f"⚠️  YIELD (fallback, фід-дані): {feed_item.get('Назва_позиції', '')[:50]} "
            f"| Хар-к: {len(specs)} | Фото з фіду: {bool(feed_item.get('Посилання_зображення'))}"
        )
        yield feed_item

    # ──────────────────────────────────────────────────────────────────
    # ITEM BUILDING
    # ──────────────────────────────────────────────────────────────────

    def _build_feed_item(
        self,
        offer,
        feed_id: str,
        name_ru: str,
        name_ua: str,
        description_ru: str,
        description_ua: str,
        category_id: str,
    ) -> dict | None:
        product_id = offer.xpath("@id").get()

        price_raw = self._resolve_price(offer, feed_id)
        if not price_raw:
            return None

        url = offer.xpath("url/text()").get() or ""
        available = offer.xpath("@available").get() == "true"
        vendor_code = offer.xpath("vendorCode/text()").get() or ""
        brand = (
            offer.xpath("brand/text()").get()
            or offer.xpath("vendor/text()").get()
            or ""
        ).strip()

        pictures = offer.xpath("picture/text()").getall()
        feed_image_url = ", ".join(pictures) if pictures else ""

        # available=True  → "" → pipeline підставить DEFAULT_QUANTITY ("10000")
        # available=False → "0"
        quantity = "" if available else "0"

        category_info = (
            self.category_mapping.get((feed_id, category_id))
            or self.category_mapping.get(("", category_id))
            or {}
        )

        return {
            # Службове поле — буде стрипнуто/нормалізовано в parse_ru_feed
            "_product_url": url,

            "supplier_id":  self.supplier_id,
            "output_file":  self.output_filename,
            "price_type":   self.price_type,
            "source":       "feed",

            "Код_товару":           vendor_code or product_id,
            "Ідентифікатор_товару": product_id,

            "Назва_позиції":     name_ru,
            "Назва_позиції_укр": name_ua,
            "Опис":              description_ru,
            "Опис_укр":          description_ua,

            "Тип_товару":     "r",
            "Ціна":           str(price_raw),
            "Валюта":         self.currency,
            "Одиниця_виміру": "шт.",

            "Посилання_зображення": feed_image_url,
            "Наявність":            "В наявності" if available else "Немає в наявності",
            "Кількість":            quantity,

            "Виробник":         brand,
            "Країна_виробник":  "",
            "Продукт_на_сайті": url,

            "Пошукові_запити":     "",
            "Пошукові_запити_укр": "",

            "Назва_групи":              category_info.get("category_name", ""),
            "Назва_групи_укр":          category_info.get("category_name", ""),
            "Номер_групи":              category_info.get("group_number", ""),
            "Ідентифікатор_підрозділу": category_info.get("subdivision_id", ""),
            "Посилання_підрозділу":     category_info.get("subdivision_link", ""),

            "feed_id":      feed_id,
            "category_url": category_info.get("category_url", ""),
            "category_id":  category_id,

            "specifications_list": [],
        }

    def _resolve_price(self, offer, feed_id: str) -> float | None:
        """
        Фід 50 (Ajax):   → завжди <price> (роздрібна)
        Фід 52 (Імпорт): → <dealerPrice>, захист: якщо > price → беремо price
        """
        retail_price = self._to_float(offer.xpath("price/text()").get())

        if feed_id == "50":
            return retail_price

        dealer_price = self._to_float(offer.xpath("dealerPrice/text()").get())

        if not dealer_price:
            return retail_price

        if retail_price is not None and dealer_price > retail_price:
            self.logger.warning(
                f"⚠️ dealerPrice ({dealer_price}) > price ({retail_price}) "
                f"для id={offer.xpath('@id').get()} — використовуємо price"
            )
            return retail_price

        return dealer_price

    # ──────────────────────────────────────────────────────────────────
    # PARSE FEED SPECS — хар-ки з XML <param> (УКРАЇНСЬКОЮ з UA фіду)
    # ──────────────────────────────────────────────────────────────────

    def _parse_feed_specs(self, offer) -> list:
        """
        Парсить <param> теги з UA XML-фіду (УКРАЇНСЬКА мова).

        Приклад XML:
          <param name="Колір">Чорний</param>
          <param name="Вага" unit="кг">0.5</param>
        """
        specs = []
        for param in offer.xpath("param"):
            name = (param.xpath("@name").get() or "").strip()
            unit = (param.xpath("@unit").get() or "").strip()
            value = (param.xpath("text()").get() or "").strip()
            if name and value:
                specs.append({"name": name, "unit": unit, "value": value})
        return specs

    # ──────────────────────────────────────────────────────────────────
    # PARSE IMAGES (ідентично retail.py)
    # ──────────────────────────────────────────────────────────────────

    def _parse_images(self, response) -> str:
        """
        Парсить усі великі зображення зі сторінки (/images/big/).
        Пріоритет: слайдер keen-slider → всі img (без /preview/).
        """
        slider_images = response.css(
            'div.keen-slider__slide img[src*="/images/big/"]::attr(src)'
        ).getall()

        if slider_images:
            seen: set = set()
            unique: list = []
            for src in slider_images:
                if src not in seen:
                    seen.add(src)
                    unique.append(src)
            return ", ".join(response.urljoin(u) for u in unique)

        all_big = response.css('img[src*="/images/big/"]::attr(src)').getall()
        filtered = [u for u in all_big if "/preview/" not in u]
        return ", ".join(response.urljoin(u) for u in filtered)

    # ──────────────────────────────────────────────────────────────────
    # PARSE SPECIFICATIONS зі сторінки (ідентично retail.py)
    # ──────────────────────────────────────────────────────────────────

    def _parse_specifications(self, response) -> list:
        """
        Парсить хар-ки зі сторінки товару (UA URL → УКРАЇНСЬКА мова).
        Потребує Playwright: secur.ua рендерить хар-ки через JS.
        Викликається тільки якщо XML-фід не мав жодного <param>.
        """
        specs_list = []
        items = response.xpath('//div[@class="item"][.//div[@class="subtitle"]]')
        for item in items:
            characteristic = item.xpath('.//div[@class="subtitle"]/text()').get()
            if not characteristic:
                continue
            characteristic = characteristic.strip()
            value_texts = item.xpath('.//div[@class="inner"]//text()').getall()
            value = " ".join(t.strip() for t in value_texts if t.strip())
            if value:
                specs_list.append({
                    "name":  characteristic,
                    "unit":  "",
                    "value": value.replace("\u00a0", " ").strip(),
                })
        return specs_list

    # ──────────────────────────────────────────────────────────────────
    # UTILS
    # ──────────────────────────────────────────────────────────────────

    def closed(self, reason):
        self.logger.info(f"🎉 Паук {self.name} завершено! Причина: {reason}")
        try:
            import winsound
            for _ in range(3):
                winsound.Beep(1000, 300)
            self.logger.info("🔔 Звуковий сигнал відтворено!")
        except Exception:
            pass

    @staticmethod
    def _to_float(value: str | None) -> float | None:
        if not value:
            return None
        try:
            return float(str(value).replace(",", ".").strip())
        except (ValueError, AttributeError):
            return None

    @staticmethod
    def _clean_description(description: str | None) -> str:
        if not description:
            return ""
        description = re.sub(r'<!\[CDATA\[', '', description)
        description = re.sub(r'\]\]>', '', description)
        description = re.sub(r'\s*style="[^"]*"', '', description)
        description = re.sub(r'>\s+<', '><', description)
        if len(description) > 10_000:
            description = description[:10_000] + "...</p>"
        return description.strip()
