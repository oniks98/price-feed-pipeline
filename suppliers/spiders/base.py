"""
Базові класи для всіх пауків-постачальників.
Мінімізує дублювання коду та забезпечує уніфікований підхід.
"""
import scrapy
import re
from pathlib import Path
from typing import Optional, Dict, List


class BaseSupplierSpider(scrapy.Spider):
    """Базовий клас для всіх пауків постачальників"""
    
    # Налаштування за замовчуванням (можна перевизначити в дочірніх класах)
    custom_settings = {
        "CONCURRENT_REQUESTS": 8,
        "CONCURRENT_REQUESTS_PER_DOMAIN": 8,
        "AUTOTHROTTLE_ENABLED": True,
        "AUTOTHROTTLE_START_DELAY": 1,
        "AUTOTHROTTLE_MAX_DELAY": 60,
        "AUTOTHROTTLE_TARGET_CONCURRENCY": 2.0,
        "DOWNLOAD_TIMEOUT": 60,  # дефолт Scrapy = 180 сек, це спричиняє тихі зависання
    }
    
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.processed_products = set()
        self.failed_products = []
    
    def _clean_price(self, price_str: str) -> str:
        """Очищення ціни від зайвих символів та округлення"""
        if not price_str:
            return ""
        
        price_str = price_str.replace(" ", "").replace("грн", "").replace("₴", "")
        price_str = price_str.replace("у.е.", "").replace("$", "").replace("USD", "")
        price_str = price_str.replace(",", ".")
        
        try:
            cleaned = "".join(c for c in price_str if c.isdigit() or c == ".")
            if not cleaned:
                return ""
            
            price_float = float(cleaned)
            
            # Округлення згідно decimal_places
            decimal_places = getattr(self, 'decimal_places', 0)
            
            if decimal_places == 0:
                return str(int(round(price_float)))
            else:
                return f"{price_float:.{decimal_places}f}"
        except ValueError:
            return ""
    
    def _normalize_availability(self, availability: Optional[str]) -> str:
        """Нормалізація статусу наявності"""
        if not availability:
            return "Уточняйте"
        
        availability_lower = availability.lower()
        
        if any(word in availability_lower for word in ["є в наявності", "в наличии", "есть", "заканчивается", "закінчується"]):
            return "В наличии"
        elif any(word in availability_lower for word in ["під замовлення", "под заказ"]):
            return "Под заказ"
        elif any(word in availability_lower for word in ["немає", "нет"]):
            return "Нет в наличии"
        else:
            return "Уточняйте"
    
    def _extract_quantity(self, text: Optional[str]) -> str:
        """Витягує кількість з тексту наявності"""
        if not text:
            return ""
        
        quantity_match = re.search(r'\d+', text)
        if quantity_match:
            return quantity_match.group(0)
        
        return ""
    
    def _sanitize_image_url(self, url: str) -> str:
        """Екранує спеціальні символи в URL зображень для PROM
        
        PROM не приймає URL із запятими - потрібно замінити на %2C
        """
        if not url:
            return ""
        
        # Замінюємо запятую на %2C
        url = url.replace(",", "%2C")
        
        return url
    
    def _load_keywords_mapping(self) -> Dict[str, Dict[str, List[str]]]:
        """Завантажує маппінг ключових слів з CSV за Ідентифікатор_підрозділу
        
        Структура (НОВА ВЕРСІЯ З ProductKeywordsGenerator):
        {
            "301105": {
                "universal_phrases_ru": [...],  # Універсальні фрази категорії
                "universal_phrases_ua": [...],
                "base_keyword_ru": "...",  # Базове ключове слово для характеристик
                "base_keyword_ua": "..."
            }
        }
        """
        import csv
        import os as _os
        mapping = {}
        csv_path = Path(_os.environ.get("PROJECT_ROOT", r"C:\FullStack\PriceFeedPipeline")) / "data" / "viatec" / "viatec_keywords.csv"
        if not csv_path.exists():
            self.logger.warning("viatec_keywords.csv not found")
            return mapping
        try:
            with open(csv_path, encoding="utf-8-sig") as f:
                reader = csv.DictReader(f, delimiter=";")
                for row in reader:
                    subdivision_id = row["Ідентифікатор_підрозділу"].strip()
                    
                    # Обробка universal_phrases - видаляємо лапки та парсимо
                    universal_ru_raw = row.get("universal_phrases_ru", "").strip()
                    universal_ua_raw = row.get("universal_phrases_ua", "").strip()
                    
                    # Видаляємо зовнішні лапки якщо є
                    if universal_ru_raw.startswith('"') and universal_ru_raw.endswith('"'):
                        universal_ru_raw = universal_ru_raw[1:-1]
                    if universal_ua_raw.startswith('"') and universal_ua_raw.endswith('"'):
                        universal_ua_raw = universal_ua_raw[1:-1]
                    
                    mapping[subdivision_id] = {
                        "universal_phrases_ru": [p.strip() for p in universal_ru_raw.split(",") if p.strip()],
                        "universal_phrases_ua": [p.strip() for p in universal_ua_raw.split(",") if p.strip()],
                        "base_keyword_ru": row.get("base_keyword_ru", "").strip(),
                        "base_keyword_ua": row.get("base_keyword_ua", "").strip(),
                    }
            self.logger.info(f"✅ Завантажено {len(mapping)} підрозділів з ключовими словами")
        except Exception as e:
            self.logger.warning(f"⚠️ Помилка завантаження viatec_keywords.csv: {e}")
        return mapping
    
    # СТАРИЙ МЕТОД - видалено, оскільки генерація ключових слів
    # тепер відбувається через ProductKeywordsGenerator у pipeline.py


class BaseRetailSpider(BaseSupplierSpider):
    """Базовий клас для роздрібних пауків"""
    
    price_type = "retail"
    currency = "UAH"  # За замовчуванням UAH для роздрібу
    decimal_places = 0  # За замовчуванням цілі числа для UAH
    
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        # Ініціалізація специфічних для роздрібу властивостей
        if not hasattr(self, 'supplier_id'):
            raise ValueError(f"Spider {self.name} must define 'supplier_id' attribute")
        
        if not hasattr(self, 'output_filename'):
            self.output_filename = f"{self.supplier_id}_retail.csv"


class BaseDealerSpider(BaseSupplierSpider):
    """Базовий клас для дилерських пауків"""
    
    price_type = "dealer"
    currency = "USD"  # За замовчуванням USD для дилерів
    decimal_places = 2  # За замовчуванням 2 знаки для USD
    
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        # Ініціалізація специфічних для дилерів властивостей
        if not hasattr(self, 'supplier_id'):
            raise ValueError(f"Spider {self.name} must define 'supplier_id' attribute")
        
        if not hasattr(self, 'output_filename'):
            self.output_filename = f"{self.supplier_id}_dealer.csv"


class EserverBaseSpider(BaseSupplierSpider):
    """Базовий клас для пауків E-Server (загальна логіка для retail і dealer)"""
    
    allowed_domains = ["e-server.com.ua"]
    
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.category_urls = []
        self.products_from_pagination = []
    
    def _extract_manufacturer(self, product_name: str) -> str:
        """Визначає виробника з назви товару"""
        if not product_name:
            return ""
        
        product_name_lower = product_name.lower()
        
        # ПРІОРИТЕТ 1: Явні згадки брендів
        priority_patterns = {
            "eserver": "EServer",
            "e-server": "EServer",
            "hikvision": "Hikvision",
            "dahua": "Dahua Technology",
            "axis": "Axis",
            "uniview": "UniView",
            "imou": "Imou",
            "ezviz": "Ezviz",
            "unv": "UNV",
            "hiwatch": "HiWatch",
            "ajax": "Ajax",
            "tp-link": "TP-Link",
            "mikrotik": "MikroTik",
            "ubiquiti": "Ubiquiti",
        }
        
        for pattern, name in priority_patterns.items():
            if pattern in product_name_lower:
                return name
        
        return ""
    
    def _extract_model_components(self, title: str, lang: str = "ua") -> List[str]:
        """Витягує компоненти моделі з назви товару для пошукових термінів
        
        Екстрагує:
        - Бренд (виробник)
        - Модель (буквено-числові комбінації)
        - Характеристики (розміри, параметри)
        - Ключові слова
        """
        components = []
        
        if not title:
            return components
        
        # Словники ключових слів залежно від мови
        keywords_dict = {
            "ua": [
                "серверний", "шафа", "стійка", "стелаж", "корпус",
                "серверна", "сервер", "рековий", "юніт", "юнит",
                "настінний", "підлоговий", "напольный", "напольний",
                "телекомунікаційний", "телекомунікаційна", "комунікаційний",
                "двері", "двірка", "дверь", "полиця", "полка",
                "вентилятор", "блок", "живлення", "розетка",
            ],
            "ru": [
                "серверный", "шкаф", "стойка", "стеллаж", "корпус",
                "серверная", "сервер", "рековый", "юнит", "юніт",
                "настенный", "напольный", "напольний", "підлоговий",
                "телекоммуникационный", "телекоммуникационная", "коммуникационный",
                "дверь", "двірка", "двері", "полка", "полиця",
                "вентилятор", "блок", "питания", "розетка",
            ]
        }
        
        keywords = keywords_dict.get(lang, keywords_dict["ua"])
        
        # 1. Додаємо бренди
        brand_patterns = [
            r"\beserver\b", r"\be-server\b", r"\bhikvision\b",
            r"\bdahua\b", r"\baxis\b", r"\buniviev\b"
        ]
        
        title_lower = title.lower()
        for pattern in brand_patterns:
            match = re.search(pattern, title_lower, re.IGNORECASE)
            if match:
                components.append(match.group(0).capitalize())
        
        # 2. Витягуємо модельні номери (буквено-числові комбінації)
        # Наприклад: UA-OF42, DS-7608NI, ABC-123
        model_patterns = [
            r"[A-Z]{2,}-[A-Z0-9-]+",  # UA-OF42, DS-7608NI
            r"[A-Z]+\d+[A-Z]*",  # ABC123, DS7608
            r"\d+U",  # 42U, 18U
        ]
        
        for pattern in model_patterns:
            matches = re.findall(pattern, title, re.IGNORECASE)
            components.extend(matches)
        
        # 3. Витягуємо розміри та числові параметри
        size_patterns = [
            r"\d+U\b",  # 42U
            r"\d+\s*мм\b",  # 600 мм
            r"\d+x\d+(?:x\d+)?",  # 600x600, 600x600x1200
            r"\d+\s*см\b",  # 60 см
        ]
        
        for pattern in size_patterns:
            matches = re.findall(pattern, title, re.IGNORECASE)
            components.extend(matches)
        
        # 4. Витягуємо ключові слова з назви
        title_words = re.findall(r'\b\w+\b', title_lower)
        for word in title_words:
            if word in keywords and word not in [c.lower() for c in components]:
                components.append(word)
        
        # 5. Очищаємо та повертаємо унікальні компоненти
        cleaned_components = []
        seen = set()
        
        for comp in components:
            comp_clean = comp.strip()
            comp_lower = comp_clean.lower()
            
            if comp_clean and comp_lower not in seen and len(comp_clean) >= 2:
                cleaned_components.append(comp_clean)
                seen.add(comp_lower)
        
        return cleaned_components
    
    def closed(self, reason):
        """Викликається при завершенні паука"""
        self.logger.info(f"🎉 Паук {self.name} завершено! Причина: {reason}")
        
        if self.failed_products:
            self.logger.info("=" * 80)
            self.logger.info("📦 СПИСОК ТОВАРІВ З ПОМИЛКАМИ ЗАВАНТАЖЕННЯ")
            self.logger.info("=" * 80)
            for failed in self.failed_products:
                self.logger.error(f"- Товар: {failed['product_name']} | URL: {failed['url']} | Причина: {failed['reason']}")
            self.logger.info("=" * 80)
        else:
            self.logger.info("✅ Товарів з помилками завантаження не знайдено.")
        
        # Звуковий сигнал (опціонально, працює тільки на Windows)
        try:
            import winsound
            for _ in range(3):
                winsound.Beep(1000, 300)
            self.logger.info("🔔 Звуковий сигнал відтворено!")
        except Exception as e:
            self.logger.debug(f"Не вдалося відтворити звук: {e}")


class ViatecBaseSpider(BaseSupplierSpider):
    """Базовий клас для пауків Viatec (загальна логіка для retail і dealer)"""
    
    allowed_domains = ["viatec.ua"]
    
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.category_urls = []
        self.products_from_pagination = []
    
    def _extract_description_with_br(self, response) -> str:
        """
        Витягує опис зі збереженням переносів <br> та обробкою списків <ul>
        """
        description_container = response.css("div.card-header__card-info-text")
        if not description_container:
            self.logger.warning(f"Не знайдено контейнер опису на {response.url}")
            return ""
        
        # Перевірка на наявність <ul>
        ul_list = description_container.css("ul")
        if ul_list:
            list_items = ul_list.css("li")
            
            description_parts = []
            for item in list_items:
                inner_content = item.get()
                inner_content = re.sub(r'</?li[^>]*>', '', inner_content).strip()
                if not inner_content.startswith('●'):
                    description_parts.append(f"● {inner_content}")
                else:
                    description_parts.append(inner_content)
            
            return "<br>".join(description_parts)
        
        # Обробка <p> тегів
        p_tags = description_container.css("p")
        if p_tags:
            # self.logger.info(f"Знайдено <p> теги в описі на {response.url}")
            result_parts = []
            for p in p_tags:
                if p.css("::attr(class)").get() == "card-header__analog-link":
                    continue
                
                p_html = p.get()
                inner_html = re.sub(r'^<p[^>]*>|</p>$', '', p_html).strip()
                
                if inner_html:
                    inner_html = inner_html.replace("<br/>", "<br>").replace("<br />", "<br>")
                    result_parts.append(inner_html)
            
            return "<br>".join(result_parts)
        
        # Fallback: raw text + <br> теги безпосередньо в <div> (без <p>/<ul> wrapper)
        # Приклад: <div>● Текст;<br>● Ще текст;<br></div>
        inner_div = description_container.css("div")
        raw_html = inner_div[0].get() if inner_div else ""
        # Знімаємо зовнішній <div ...> ... </div>
        inner_html = re.sub(r"^<div[^>]*>", "", raw_html, count=1)
        inner_html = re.sub(r"</div>\s*$", "", inner_html).strip()

        if not inner_html:
            self.logger.warning(f"Порожній контейнер опису на {response.url}")
            return ""

        # Нормалізуємо <br> варіанти → єдиний <br>
        inner_html = re.sub(r"<br\s*/?>", "<br>", inner_html, flags=re.IGNORECASE)
        # Знімаємо всі HTML-теги КРІМ <br> (span, a, b, тощо)
        inner_html = re.sub(r"<(?!br\b)[^>]+>", "", inner_html, flags=re.IGNORECASE)
        # Збираємо рядки, пропускаємо порожні
        lines = [line.strip() for line in inner_html.split("<br>") if line.strip()]

        if not lines:
            self.logger.warning(f"Після обробки fallback опис порожній на {response.url}")
            return ""

        self.logger.debug(f"Fallback (raw text+br): {len(lines)} рядків на {response.url}")
        return "<br>".join(lines)
    
    def _extract_specifications(self, response) -> List[Dict[str, str]]:
        """
        Витягує характеристики товару з таблиці (українські назви)
        """
        specs_list = []
        
        # Спроба 1: Активна вкладка
        spec_rows = response.css("li.card-tabs__item.active div.card-tabs__characteristic-content table tr")
        
        # Спроба 2: Будь-яка вкладка з характеристиками
        if not spec_rows:
            spec_rows = response.css("div.card-tabs__characteristic-content table tr")
        
        # Спроба 3: Загальний селектор таблиці
        if not spec_rows:
            spec_rows = response.css("ul.card-tabs__list table tr")
        
        for row in spec_rows[:60]:
            name = row.css("th::text").get()
            value = row.css("td::text").get()
            
            if name and value:
                specs_list.append({
                    "name": name.strip(),
                    "value": value.strip(),
                    "unit": ""
                })
        
        return specs_list
    
    def _convert_to_ru_url(self, url: str) -> str:
        """Конвертує український URL в російський"""
        if "/ru/" not in url:
            url = url.replace("viatec.ua/", "viatec.ua/ru/")
        return url
    
    def closed(self, reason):
        """Викликається при завершенні паука"""
        self.logger.info(f"🎉 Паук {self.name} завершено! Причина: {reason}")
        
        if self.failed_products:
            self.logger.info("=" * 80)
            self.logger.info("📦 СПИСОК ТОВАРІВ З ПОМИЛКАМИ ЗАВАНТАЖЕННЯ")
            self.logger.info("=" * 80)
            for failed in self.failed_products:
                self.logger.error(f"- Товар: {failed['product_name']} | URL: {failed['url']} | Причина: {failed['reason']}")
            self.logger.info("=" * 80)
        else:
            self.logger.info("✅ Товарів з помилками завантаження не знайдено.")
        
        # Звуковий сигнал (опціонально, працює тільки на Windows)
        try:
            import winsound
            for _ in range(3):
                winsound.Beep(1000, 300)
            self.logger.info("🔔 Звуковий сигнал відтворено!")
        except Exception as e:
            self.logger.debug(f"Не вдалося відтворити звук: {e}")
