"""
Сервіс для валідації даних товарів.

Відповідає за перевірку обов'язкових полів, коректності значень
та фільтрацію товарів за критеріями якості.
"""

import re


class ValidationService:
    """Валідація даних товарів перед записом у CSV"""

    @staticmethod
    def is_valid_price(price) -> bool:
        """
        Перевіряє чи ціна є валідною (>0).
        
        Args:
            price: ціна в будь-якому форматі (str, int, float)
        
        Returns:
            True якщо ціна > 0, False інакше
        """
        if not price:
            return False
        
        try:
            price_float = float(str(price).replace(",", ".").strip())
            return price_float > 0
        except (ValueError, AttributeError):
            return False

    @staticmethod
    def sanitize_csv_value(value) -> str:
        """
        Очищає значення для безпечного запису в CSV.
        
        Заміни:
        - ; → , (розділювач CSV)
        - " → ″ (подвійні лапки)
        - \n → <br> (перенос рядка для PROM)
        - \r → видаляється
        
        Args:
            value: будь-яке значення
        
        Returns:
            Очищений рядок
        """
        return (
            str(value)
            .replace(";", ",")
            .replace('"', "″")
            .replace("\n", "<br>")
            .replace("\r", "")
        )

    @staticmethod
    def sanitize_prom_numeric(value: str, decimals: int = 3, decimal_sep: str = ',') -> str:
        """
        Нормалізує числове значення для числових полів Prom.ua.

        Для фізичних колонок (Вага,кг / Ширина,см / Висота,см / Довжина,см)
        ОБОВ'ЯЗКОВО викликати з decimal_sep='.' та decimals=2:
          - Prom.ua приймає ТІЛЬКИ крапку як десятковий розділювач у цих полях
            (кома дає помилку "Допустимі тільки числові значення")
          - decimals=2 гарантує максимум 2 знаки після крапки — тоді крапка
            не може бути сплутана з роздільником тисяч (у роздільника тисяч
            завжди рівно 3 цифри після крапки: 123.569 → Excel ЄС-локаль = 123569)

        Перетворення (decimal_sep='.', decimals=2):
        - "630.805"              → "630.81"
        - "0.6308050000000001"   → "0.63"
        - "2,8"                  → "2.8"   (кома на вході нормалізується)
        - "1500"                 → "1500"  (ціле — без роздільника)
        - ""  / None / сміття   → ""

        Перетворення (decimal_sep=',', decimals=3 — legacy для spec-значень):
        - "630.805"              → "630,805"
        - "0.6308050000000001"   → "0,631"

        Args:
            value:       числовий рядок (крапка або кома як розділювач на вході)
            decimals:    максимальна кількість знаків після коми (за замовч. 3)
            decimal_sep: символ десяткового розділювача у виводі (',' або '.')

        Returns:
            Нормалізований рядок із заданим розділювачем, або "" при помилці
        """
        if not value:
            return ""

        try:
            num = float(str(value).strip().replace(",", "."))
            num = round(num, decimals)

            # 0,002 з decimals=2 → round=0.0 → Prom.ua відхиляє "0" у фізичних полях,
            # але пуста комірка проходить валідацію
            if num == 0:
                return ""

            if num == int(num):
                return str(int(num))

            formatted = f"{num:.{decimals}f}".rstrip("0")
            return formatted.replace(".", decimal_sep)

        except (ValueError, AttributeError):
            return ""

    @staticmethod
    def validate_required_fields(item_dict: dict, required_fields: list) -> tuple[bool, list]:
        """
        Перевіряє наявність обов'язкових полів.
        
        Args:
            item_dict: словник з даними товару
            required_fields: список обов'язкових полів
        
        Returns:
            (is_valid, missing_fields)
        """
        missing = []
        
        for field in required_fields:
            value = item_dict.get(field, "")
            
            # Порожнє значення або тільки пробіли
            if not value or (isinstance(value, str) and not value.strip()):
                missing.append(field)
        
        return len(missing) == 0, missing

    @staticmethod
    def normalize_boolean(value) -> str:
        """
        Нормалізує булеві значення для PROM.
        
        Args:
            value: будь-яке значення (True/False/"так"/"ні"/1/0)
        
        Returns:
            "+" або "-"
        """
        if not value:
            return "-"
        
        value_str = str(value).lower().strip()
        
        # Позитивні значення
        if value_str in ['true', '1', 'yes', 'так', '+', 'є', 'yes', 'y']:
            return "+"
        
        # Негативні значення
        if value_str in ['false', '0', 'no', 'ні', '-', 'немає', 'no', 'n']:
            return "-"
        
        # Якщо незрозуміле значення - вважаємо негативним
        return "-"

    @staticmethod
    def clean_html(text: str) -> str:
        """
        Очищає HTML теги з тексту (опціонально).
        
        Args:
            text: текст з можливими HTML тегами
        
        Returns:
            Текст без HTML тегів
        """
        if not text:
            return ""
        
        # Видаляємо HTML теги
        clean = re.sub(r'<[^>]+>', '', text)
        
        # Видаляємо множинні пробіли
        clean = re.sub(r'\s+', ' ', clean)
        
        return clean.strip()

    @staticmethod
    def validate_url(url: str) -> bool:
        """
        Перевіряє чи URL є валідним.
        
        Args:
            url: URL адреса
        
        Returns:
            True якщо URL валідний
        """
        if not url:
            return False
        
        # Базова перевірка URL
        url_pattern = re.compile(
            r'^https?://'  # http:// or https://
            r'(?:(?:[A-Z0-9](?:[A-Z0-9-]{0,61}[A-Z0-9])?\.)+[A-Z]{2,6}\.?|'  # domain
            r'localhost|'  # localhost
            r'\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3})'  # or IP
            r'(?::\d+)?'  # optional port
            r'(?:/?|[/?]\S+)$', re.IGNORECASE
        )
        
        return bool(url_pattern.match(url))

    @staticmethod
    def normalize_spec_value(value: str) -> str:
        """
        Нормалізує значення характеристики перед записом у CSV.

        Поточні перетворення:
        - Замінює всі варіанти дефісу/тире (en-dash –, em-dash —, minus sign −)
          на стандартний ASCII-мінус (-), щоб числові значення з мінусом
          (напр. "–40") коректно проходили валідацію на сторонніх платформах.
        - Прибирає пробіл між знаком мінус та цифрою: "- 40" → "-40".

        Args:
            value: сире значення характеристики

        Returns:
            Нормалізований рядок
        """
        if not value:
            return value

        # Замінюємо всі варіанти тире/мінусу на ASCII-мінус
        normalized = re.sub(r'[\u2010-\u2015\u2212\u2212\u002D\uFE58\uFE63\uFF0D–—−]', '-', value)

        # Прибираємо пробіл після мінуса перед цифрою: "- 40" → "-40"
        normalized = re.sub(r'-\s+(\d)', r'-\1', normalized)

        return normalized

    @staticmethod
    def truncate_text(text: str, max_length: int, suffix: str = "...") -> str:
        """
        Обрізає текст до максимальної довжини.
        
        Args:
            text: текст для обрізання
            max_length: максимальна довжина
            suffix: суфікс для обрізаного тексту
        
        Returns:
            Обрізаний текст
        """
        if not text or len(text) <= max_length:
            return text
        
        return text[:max_length - len(suffix)] + suffix

    @staticmethod
    def validate_numeric_range(value, min_value=None, max_value=None) -> bool:
        """
        Перевіряє чи значення в допустимому діапазоні.
        
        Args:
            value: числове значення
            min_value: мінімальне значення (опціонально)
            max_value: максимальне значення (опціонально)
        
        Returns:
            True якщо значення в діапазоні
        """
        try:
            num = float(str(value).replace(",", "."))
            
            if min_value is not None and num < min_value:
                return False
            
            if max_value is not None and num > max_value:
                return False
            
            return True
        except (ValueError, AttributeError):
            return False
