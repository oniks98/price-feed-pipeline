"""
Сервіс очищення тексту від небажаного вмісту у Prom-фіді.

Два незалежних механізми:
  BANNED_WORDS      — замінює заборонені слова пробілом (case-insensitive).
  _PROMO_ARTIFACTS  — видаляє промо-фрази/артефакти цілком (разом з роздільником).

Щоб додати заборонене слово   — додай до BANNED_WORDS.
Щоб додати промо-артефакт     — додай до _PROMO_ARTIFACTS.
"""

import re

# Слова, заборонені до розміщення на Prom.ua (у назві, описі, пошукових запитах).
# Перевірка case-insensitive, тому регістр тут не важливий.
BANNED_WORDS: list[str] = [
    "copy",
    "підробка",
    "подделка",
]

# Прекомпільований патерн для швидкості (будується один раз при імпорті)
_BANNED_PATTERN: re.Pattern = re.compile(
    r'\b(' + '|'.join(re.escape(w) for w in BANNED_WORDS) + r')\b',
    re.IGNORECASE,
)

# Промо-артефакти Prom-розмітки що видаляються повністю.
# Включає попередній роздільник ([,;\s]*) і хвостові пробіли,
# щоб не залишати пунктуаційних артефактів у тексті.
_PROMO_ARTIFACTS: list[str] = [
    "Є товари з аналогічними характеристиками →",
]

_PROMO_ARTIFACTS_RE: re.Pattern = re.compile(
    r'[,;\s]*(?:' + '|'.join(re.escape(p) for p in _PROMO_ARTIFACTS) + r')\s*',
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
        Видаляє промо-артефакти та замінює заборонені слова на пробіл.

        Порядок:
          1. _PROMO_ARTIFACTS_RE — видалення цілих промо-фраз разом з роздільником.
          2. _BANNED_PATTERN     — заміна заборонених слів на пробіл.
          3. Collapse зайвих пробілів.

        Args:
            text: вхідний рядок

        Returns:
            Очищений рядок
        """
        if not text:
            return text
        cleaned = _PROMO_ARTIFACTS_RE.sub("", text)
        cleaned = _BANNED_PATTERN.sub(" ", cleaned)
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
