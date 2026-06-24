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


# ─── LP (LogicPower) supplier: HTML-артефакти описів ────────────────────────
# Заміна: fragment → крапка (напр., вбудований телефон у тексті).
# Видалення: рекламні посилання та абзаци від виробника.
_LP_REPLACE_RULES: list[tuple[str, str]] = [
    (
        # <strong><a href="tel:+380800309988">+38(080)030-99-88</a></strong>  →  .
        r'<strong><a\s+href=["\u2033]tel:\+380800309988["\u2033]>'
        r'\+38\(080\)030-99-88</a></strong>',
        ".",
    ),
]

_LP_DELETE_PATTERNS: list[str] = [
    # RU: "Узнать больше информации Вы можете прочитав <a ...><strong>данную статью</strong></a>."
    r'Узнать больше информации Вы можете прочитав\s+<a\s+href=["\u2033][^"\u2033]+["\u2033]>'
    r'<strong>данную статью</strong></a>\.',
    # UA: "Детальнішу інформацію Ви можете отримати з <a ...><strong>нашої статті</strong></a>."
    r'Детальнішу інформацію Ви можете отримати з\s+<a\s+href=["\u2033][^"\u2033]+["\u2033]>'
    r'<strong>нашої статті</strong></a>\.',
    # RU: промо-абзац про замовлення на офіційному сайті
    r'<p>На официальном сайте LogicPower можно заказать аккумуляторные батареи для ИБП'
    r' и другие типы аккумуляторов с доставкой по Украине\.\s*Цена\s*[–\-]\s*напрямую от производителя\.</p>',
    # UA: промо-абзац про замовлення на офіційному сайті
    r'<p>На офіційному сайті LogicPower можна замовити акумуляторні батареї для ДБЖ'
    r' та інші типи акумуляторів з доставкою по Україні\.\s*Ціна\s*[–\-]\s*безпосередньо від виробника\.</p>',
]

_LP_REPLACE_RE: list[tuple[re.Pattern, str]] = [
    (re.compile(pattern, re.IGNORECASE), repl)
    for pattern, repl in _LP_REPLACE_RULES
]

_LP_DELETE_RE: re.Pattern = re.compile(
    r'(?:' + '|'.join(_LP_DELETE_PATTERNS) + r')',
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
          1. _LP_DELETE_RE        — видалення LP-специфічних HTML-фрагментів.
          2. _LP_REPLACE_RE       — LP-специфічні заміни (напр., телефон → крапка).
          3. _PROMO_ARTIFACTS_RE  — видалення загальних промо-фраз разом з роздільником.
          4. _BANNED_PATTERN      — заміна заборонених слів на пробіл.
          5. Collapse зайвих пробілів.

        Args:
            text: вхідний рядок

        Returns:
            Очищений рядок
        """
        if not text:
            return text
        cleaned = _LP_DELETE_RE.sub("", text)
        for pattern, repl in _LP_REPLACE_RE:
            cleaned = pattern.sub(repl, cleaned)
        cleaned = _PROMO_ARTIFACTS_RE.sub("", cleaned)
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
