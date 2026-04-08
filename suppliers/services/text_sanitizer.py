"""
Сервіс очищення тексту від слів, заборонених платформою Prom.ua.

Щоб додати нове заборонене слово — просто додай його до BANNED_WORDS.
"""

import re

# Слова, заборонені до розміщення на Prom.ua (у назві, описі, пошукових запитах).
# Перевірка case-insensitive, тому регістр тут не важливий.
BANNED_WORDS: list[str] = [
    "copy",
]

# Прекомпільований патерн для швидкості (будується один раз при імпорті)
_BANNED_PATTERN: re.Pattern = re.compile(
    r'\b(' + '|'.join(re.escape(w) for w in BANNED_WORDS) + r')\b',
    re.IGNORECASE,
)


class TextSanitizer:
    """Очищення текстових полів від заборонених слів"""

    # Поля товару, які підлягають перевірці
    SANITIZED_FIELDS: tuple[str, ...] = (
        "Назва_позиції",
        "Назва_позиції_укр",
        "Опис",
        "Опис_укр",
        "Пошукові_запити",
        "Пошукові_запити_укр",
    )

    @staticmethod
    def sanitize(text: str) -> str:
        """
        Замінює заборонені слова на пробіл і прибирає зайві пробіли.

        Args:
            text: вхідний рядок

        Returns:
            Очищений рядок
        """
        if not text:
            return text
        cleaned = _BANNED_PATTERN.sub(" ", text)
        # Прибираємо подвійні пробіли що могли утворитися
        return re.sub(r' {2,}', ' ', cleaned).strip()

    @classmethod
    def sanitize_item(cls, item: dict) -> dict:
        """
        Очищає всі текстові поля товару in-place.

        Args:
            item: словник полів товару (result з _clean_item)

        Returns:
            Той самий словник з очищеними полями
        """
        for field in cls.SANITIZED_FIELDS:
            if item.get(field):
                item[field] = cls.sanitize(item[field])
        return item
