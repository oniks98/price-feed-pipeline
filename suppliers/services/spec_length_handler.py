"""
Сервіс для обробки занадто довгих значень характеристик.

Вирішує дві проблеми при імпорті в Prom.ua:
  1. HTML-теги у значеннях (<p style="...">, <br>, тощо) — Prom рахує їх у ліміт 255.
     Рішення: _strip_html() очищає ПЕРЕД будь-якою перевіркою довжини.
  2. Значення >255 символів після очистки — Prom відхиляє імпорт.
     Рішення: smart_trim до 255, залишаємо як характеристику (не переносимо в опис).

ПОРЯДОК ОБРОБКИ (strategy="hybrid", рекомендовано):
  КРОК 1 — очищення HTML
  КРОК 2 — якщо >1024 після очистки → переносимо в опис товару
  КРОК 3 — якщо >255 → smart_trim до 255, зберігаємо як характеристику
"""
import re
from typing import List, Dict, Tuple


class SpecificationLengthHandler:
    """
    Обробляє занадто довгі значення характеристик.
    
    Використовує розумну логіку для збереження інформації:
    - Короткі характеристики залишає без змін
    - Довгі характеристики або обрізає, або переносить в опис
    """
    
    # Константи обмежень
    MAX_SPEC_LENGTH = 1024       # Технічний максимум Prom.ua (для переносу в опис)
    PROM_FIELD_MAX  = 255        # Реальний ліміт поля характеристики в Prom.ua
    DESCRIPTION_THRESHOLD = 500  # Поріг для перенесення в опис
    SAFE_TRIM_LENGTH = 251       # Безпечна довжина для 255-ліміту (255 - 4 для "...")
    SPEC_NAME_MAX   = 255        # Ліміт назви характеристики
    SPEC_UNIT_MAX   = 255        # Ліміт одиниці виміру
    
    def __init__(self, strategy: str = "hybrid"):
        """
        Args:
            strategy: Стратегія обробки:
                - "trim" - просто обрізати до 1024
                - "smart_trim" - обрізати до 1020 + "..."
                - "move_to_description" - переносити довгі в опис
                - "hybrid" - комбінований підхід (РЕКОМЕНДОВАНО)
        """
        self.strategy = strategy
        self.stats = {
            "total_specs": 0,
            "trimmed": 0,
            "moved_to_description": 0,
            "unchanged": 0,
        }
    
    @staticmethod
    def _strip_html(text: str) -> str:
        """
        Повністю видаляє HTML-теги та нормалізує пробіли.

        Проблема: постачальники кладуть у <param> значення вигляду
            <p style="text-align: justify">IPv4, IGMP, ...</p>
        Prom.ua рахує теги у ліміт 255 символів і відхиляє імпорт.
        """
        # Видаляємо HTML-теги
        cleaned = re.sub(r'<[^>]+>', '', text)
        # Замінюємо NBSP та зайві пробіли
        cleaned = cleaned.replace('\u00a0', ' ')
        cleaned = re.sub(r'[ \t]+', ' ', cleaned)
        # Замінюємо переноси рядків на пробіл
        cleaned = cleaned.replace('\n', ' ').replace('\r', '')
        return cleaned.strip()

    def process_specifications(
        self,
        specifications_list: List[Dict],
        current_description: str = ""
    ) -> Tuple[List[Dict], str]:
        """
        Обробляє список характеристик та опис товару.

        ПОРЯДОК ОБРОБКИ КОЖНОЇ ХАРАКТЕРИСТИКИ:
          1. Очищення HTML-тегів (вирішує основну причину довгих значень)
          2. Якщо після очистки >MAX_SPEC_LENGTH (1024) — стратегія (hybrid: в опис)
          3. Якщо >PROM_FIELD_MAX (255) — smart_trim до 255, залишаємо характеристикою
          4. Обрізаємо name/unit до 255 якщо потрібно
        """
        if not specifications_list:
            return specifications_list or [], current_description

        processed_specs = []
        additional_description_parts = []

        for spec in specifications_list:
            self.stats["total_specs"] += 1

            name  = spec.get("name",  "")
            value = spec.get("value", "")
            unit  = spec.get("unit",  "")

            # Обрізаємо назву та одиницю виміру до ліміту Prom.ua
            name = name[:self.SPEC_NAME_MAX] if len(name) > self.SPEC_NAME_MAX else name
            unit = unit[:self.SPEC_UNIT_MAX] if len(unit) > self.SPEC_UNIT_MAX else unit

            if not value:
                processed_specs.append({"name": name, "value": value, "unit": unit})
                self.stats["unchanged"] += 1
                continue

            # ── КРОК 1: очищення HTML ────────────────────────────────────────────
            # Видаляємо теги ДО перевірки довжини — це головна причина помилки
            # "Максимальна довжина поля 255 символів" при імпорті в Prom.ua
            value = self._strip_html(value)

            if not value:
                # Після очистки значення порожнє — пропускаємо
                self.stats["unchanged"] += 1
                continue

            value_length = len(value)

            # ── КРОК 2: стратегія для значень > MAX_SPEC_LENGTH (1024) ──────────
            if self.strategy in ("trim", "smart_trim"):
                processed_value = (
                    self._simple_trim(value)
                    if self.strategy == "trim"
                    else self._smart_trim(value)
                )
                processed_specs.append({"name": name, "value": processed_value, "unit": unit})
                if value_length > self.MAX_SPEC_LENGTH:
                    self.stats["trimmed"] += 1
                else:
                    self.stats["unchanged"] += 1
                continue

            elif self.strategy == "move_to_description":
                if value_length > self.DESCRIPTION_THRESHOLD:
                    additional_description_parts.append(
                        self._format_as_description(name, value, unit)
                    )
                    self.stats["moved_to_description"] += 1
                    continue
                # Коротше DESCRIPTION_THRESHOLD — далі у КРОК 3

            elif self.strategy == "hybrid":
                if value_length > self.MAX_SPEC_LENGTH:
                    # >1024 після очистки HTML — переносимо в опис
                    additional_description_parts.append(
                        self._format_as_description(name, value, unit)
                    )
                    self.stats["moved_to_description"] += 1
                    continue
                # ≤1024 — далі у КРОК 3 (255-ліміт)

            # ── КРОК 3: жорсткий ліміт Prom.ua 255 символів ─────────────────────
            # Залишаємо як характеристику (не переносимо в опис).
            # Значення вже очищене від HTML, тому обрізання мінімальне.
            if len(value) > self.PROM_FIELD_MAX:
                value = self._smart_trim_to(value, self.PROM_FIELD_MAX)
                self.stats["trimmed"] += 1
            else:
                self.stats["unchanged"] += 1

            processed_specs.append({"name": name, "value": value, "unit": unit})
        
        # Формуємо оновлений опис
        updated_description = current_description
        if additional_description_parts:
            # Додаємо секцію "Додаткова інформація" з заголовком
            additional_section = (
                "\n\n<h3>📋 Додаткова інформація</h3>\n\n" + 
                "\n\n".join(additional_description_parts)
            )
            updated_description = (current_description + additional_section).strip()
        
        return processed_specs, updated_description
    
    def _simple_trim(self, value: str) -> str:
        """Просто обрізає до MAX_SPEC_LENGTH"""
        if len(value) <= self.MAX_SPEC_LENGTH:
            return value
        return value[:self.MAX_SPEC_LENGTH]

    def _smart_trim(self, value: str) -> str:
        """Розумне обрізання до MAX_SPEC_LENGTH з "..." в кінці"""
        return self._smart_trim_to(value, self.MAX_SPEC_LENGTH)

    def _smart_trim_to(self, value: str, max_len: int) -> str:
        """
        Розумне обрізання до довільного max_len.
        Використовується і для MAX_SPEC_LENGTH (1024), і для PROM_FIELD_MAX (255).
        """
        if len(value) <= max_len:
            return value

        safe = max_len - 4  # місце для "..."
        trimmed = value[:safe]

        # Знаходимо останній пробіл, щоб не обрізати посеред слова
        last_space = trimmed.rfind(' ')
        if last_space > safe - 60:
            trimmed = trimmed[:last_space]

        return trimmed.rstrip('.,;:!? ') + "..."
    
    def _format_as_description(self, name: str, value: str, unit: str = "") -> str:
        """
        Форматує характеристику для вставки в опис товару.
        
        Args:
            name: Назва характеристики
            value: Значення
            unit: Одиниця виміру (опціонально)
        
        Returns:
            Відформатований HTML для опису
        """
        # Очищаємо HTML теги з value, якщо вони є
        value_clean = re.sub(r'<[^>]+>', '', value)
        value_clean = value_clean.strip()
        
        # Форматуємо як HTML параграф
        unit_text = f" ({unit})" if unit else ""
        
        return f"<p><strong>{name}{unit_text}:</strong><br>{value_clean}</p>"
    
    def get_stats(self) -> Dict[str, int]:
        """Повертає статистику обробки"""
        return self.stats.copy()
    
    def reset_stats(self):
        """Скидає статистику"""
        self.stats = {
            "total_specs": 0,
            "trimmed": 0,
            "moved_to_description": 0,
            "unchanged": 0,
        }
    
    def print_stats(self):
        """Виводить статистику в консоль"""
        total = self.stats["total_specs"]
        if total == 0:
            print("📊 Немає оброблених характеристик")
            return
        
        print("\n" + "="*60)
        print("📊 СТАТИСТИКА ОБРОБКИ ХАРАКТЕРИСТИК")
        print("="*60)
        print(f"Всього характеристик: {total}")
        print(f"✂️  Обрізано: {self.stats['trimmed']} ({self.stats['trimmed']/total*100:.1f}%)")
        print(f"📝 Перенесено в опис: {self.stats['moved_to_description']} ({self.stats['moved_to_description']/total*100:.1f}%)")
        print(f"✅ Без змін: {self.stats['unchanged']} ({self.stats['unchanged']/total*100:.1f}%)")
        print("="*60 + "\n")


# Зручний хелпер для швидкого використання
def process_long_specifications(
    specifications_list: List[Dict],
    description: str = "",
    strategy: str = "hybrid"
) -> Tuple[List[Dict], str]:
    """
    Швидкий хелпер для обробки характеристик.
    
    Args:
        specifications_list: Список характеристик
        description: Опис товару
        strategy: Стратегія ("trim", "smart_trim", "move_to_description", "hybrid")
    
    Returns:
        Tuple[List[Dict], str]: (оброблені характеристики, оновлений опис)
    
    Example:
        specs, desc = process_long_specifications(
            specifications_list=item['specifications_list'],
            description=item['Опис'],
            strategy='hybrid'
        )
    """
    handler = SpecificationLengthHandler(strategy=strategy)
    return handler.process_specifications(specifications_list, description)
