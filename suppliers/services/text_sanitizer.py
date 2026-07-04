"""
Сервіс очищення тексту від небажаного вмісту у Prom-фіді.

Механізми (застосовуються в цьому порядку, див. TextSanitizer.sanitize):
  1. Supplier-специфічні видалення/заміни (LP, Viatec) — лише коли
     переданий відповідний `supplier`.
  2. _IMG_TAG_RE / _LINK_UNWRAP_RE — загальні для ВСІХ постачальників:
     видаляють <img>-теги цілком та розгортають <a>...</a>, лишаючи вміст.
  3. _PROMO_ARTIFACTS      — видаляє промо-фрази/артефакти цілком (разом з роздільником).
  4. Viatec: видалення окремих слів (Viatec/Акція) — в самому кінці.
  5. BANNED_WORDS          — замінює заборонені слова пробілом (case-insensitive).

Щоб додати заборонене слово        — додай до BANNED_WORDS.
Щоб додати промо-артефакт          — додай до _PROMO_ARTIFACTS.
Щоб додати LP-правило              — додай до _LP_DELETE_PATTERNS / _LP_REPLACE_RULES.
Щоб додати Viatec-правило          — додай до _VIATEC_DELETE_PHRASES / _VIATEC_REPLACE_RULES.

ВАЖЛИВО: supplier-специфічні правила застосовуються лише коли викликач
передав `supplier="lp"` / `supplier="viatec"` (див. SupplierConfig.supplier_name).
Без цього параметра LP/Viatec-специфічні кроки просто пропускаються —
це свідома ізоляція постачальників (жодних побічних ефектів між ними).
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


# ─── Загальне для ВСІХ постачальників: посилання та фото у описах ───────────
# 1) <img ...> — видаляється цілком (теги без окремого закриваючого тегу).
# 2) <a ...>текст</a> — тег розгортається, текст/вкладені теги (<strong> тощо)
#    лишаються. Працює для будь-яких лапок в атрибутах (" або типографське ″).
_IMG_TAG_RE: re.Pattern = re.compile(r'<img\b[^>]*>', re.IGNORECASE)

_LINK_UNWRAP_RE: re.Pattern = re.compile(
    r'<a\b[^>]*>(.*?)</a>',
    re.IGNORECASE | re.DOTALL,
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
    (
        # RU: "На официальном сайте LogicPower Вы можете купить в Украине" → "Купите"
        r'На официальном сайте LogicPower Вы можете купить в Украине',
        "Купите",
    ),
    (
        # UA: "На офіційному сайті LogicPower Ви можете купити в Україні" → "Купуйте"
        r'На офіційному сайті LogicPower Ви можете купити в Україні',
        "Купуйте",
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
    # RU: повідомлення про відсутність акції безкоштовної доставки (сонячні панелі)
    r'(?:<p>)?На этот товар не распространяется акция по бесплатной доставке\.\s*'
    r'Доставка солнечных панелей осуществляется за счет покупателя согласно тарифам перевозчика\.(?:</p>)?',
    # UA: повідомлення про відсутність акції безкоштовної доставки (сонячні панелі)
    r'(?:<p>)?На цей товар не розповсюджується акція з безкоштовної доставки\.\s*'
    r'Доставлення сонячних панелей здійснюється за рахунок покупця згідно з тарифами перевізника\.(?:</p>)?',
    # RU: про можливість купівлі в кредит/рассрочку (інвертори)
    r'(?:<p>)?На официальном сайте LogicPower можно купить инвертор для солнечных батарей в Украине'
    r' в кредит или воспользовавшись услугой.*?оплата частями.*?\.(?:</p>)?',
    # UA: про можливість купівлі в кредит/розстрочку (інвертори)
    r'(?:<p>)?На офіційному сайті LogicPower можна купити інвертор для сонячних батарей в Україні'
    r' в кредит або скориставшись послугою.*?оплата частинами.*?\.(?:</p>)?',
    # Літеральний img src-фрагмент від logicfox.info (конкретне зображення в описі)
    r'["\u2033]\s*src=["\u2033]https://logicfox\.info/foto/4148/site/4148_content_2\.jpg["\u2033]',
]

_LP_REPLACE_RE: list[tuple[re.Pattern, str]] = [
    (re.compile(pattern, re.IGNORECASE), repl)
    for pattern, repl in _LP_REPLACE_RULES
]

_LP_DELETE_RE: re.Pattern = re.compile(
    r'(?:' + '|'.join(_LP_DELETE_PATTERNS) + r')',
    re.IGNORECASE,
)


# ─── Viatec supplier: промо-фрази та артефакти описів ───────────────────────
# Видаляються повністю (разом з попереднім роздільником, як і _PROMO_ARTIFACTS).
_VIATEC_DELETE_PHRASES: list[str] = [
    "Є товари з аналогічними характеристиками →",
    "Есть товары с аналогичными характеристиками →",
    "В компанії Viatec є рішення!",
    "У компании Viatec есть решение!",
    "або реєстрації як партнера Viatec",
    "или регистрации в качестве партнера Viatec",
    "или зарегистрируйтесь как партнер для доступа к эксклюзивным предложениям",
    "або зареєструйтеся як партнер для доступу до ексклюзивних пропозицій",
    "или зарегистрируйтесь как партнер на портале Viatec",
    "або зареєструйтесь як партнер на порталі Viatec",
]

_VIATEC_DELETE_RE: re.Pattern = re.compile(
    r'[,;\s]*(?:' + '|'.join(re.escape(p) for p in _VIATEC_DELETE_PHRASES) + r')\s*',
    re.IGNORECASE,
)

# Заміни Viatec (виконуються після видалень, до видалення слів Viatec/Акція).
_VIATEC_REPLACE_RULES: list[tuple[str, str]] = [
    ("до вашого менеджера", "до менеджера"),
    ("Viatec пропонує", "Пропонуєм"),
    ("Viatec предлагает", "Предлагаем"),
]

_VIATEC_REPLACE_RE: list[tuple[re.Pattern, str]] = [
    (re.compile(re.escape(pattern), re.IGNORECASE), repl)
    for pattern, repl in _VIATEC_REPLACE_RULES
]

# Фінальне видалення окремих слів для Viatec — після всіх інших кроків.
# Слова видаляються цілком (не замінюються пробілом, як BANNED_WORDS) —
# це слова без якої сенсової цінності окремо від решти тексту.
_VIATEC_WORD_DELETE: list[str] = [
    "Viatec",
    "Акція",
    "Акция",
    "Акции",
    "Акції",
]

_VIATEC_WORD_DELETE_RE: re.Pattern = re.compile(
    r'\b(?:' + '|'.join(re.escape(w) for w in _VIATEC_WORD_DELETE) + r')\b',
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
    def sanitize(text: str, supplier: str | None = None) -> str:
        """
        Видаляє промо-артефакти, посилання/фото та замінює заборонені слова на пробіл.

        Порядок:
          1. LP-специфічні видалення/заміни      — лише коли supplier == "lp".
          2. Viatec-специфічні видалення/заміни  — лише коли supplier == "viatec".
          3. _IMG_TAG_RE / _LINK_UNWRAP_RE      — видалення <img> та розгортання <a> — для ВСІХ постачальників.
          4. _PROMO_ARTIFACTS_RE                — видалення загальних промо-фраз разом з роздільником.
          5. _VIATEC_WORD_DELETE_RE             — видалення слів Viatec/Акція — лише коли supplier == "viatec", в самому кінці.
          6. _BANNED_PATTERN                    — заміна заборонених слів на пробіл.
          7. Collapse зайвих пробілів.

        Args:
            text: вхідний рядок
            supplier: нормалізоване ім'я постачальника (SupplierConfig.supplier_name,
                напр. "lp", "viatec"). None/інше значення — supplier-специфічні
                кроки пропускаються.

        Returns:
            Очищений рядок
        """
        if not text:
            return text

        cleaned = text

        if supplier == "lp":
            cleaned = _LP_DELETE_RE.sub("", cleaned)
            for pattern, repl in _LP_REPLACE_RE:
                cleaned = pattern.sub(repl, cleaned)
        elif supplier == "viatec":
            cleaned = _VIATEC_DELETE_RE.sub("", cleaned)
            for pattern, repl in _VIATEC_REPLACE_RE:
                cleaned = pattern.sub(repl, cleaned)

        # Загальне для ВСІХ постачальників: видалення фото та розгортання посилань
        cleaned = _IMG_TAG_RE.sub("", cleaned)
        cleaned = _LINK_UNWRAP_RE.sub(r'\1', cleaned)

        cleaned = _PROMO_ARTIFACTS_RE.sub("", cleaned)

        if supplier == "viatec":
            cleaned = _VIATEC_WORD_DELETE_RE.sub("", cleaned)

        cleaned = _BANNED_PATTERN.sub(" ", cleaned)
        # Прибираємо подвійні пробіли що могли утворитися
        return re.sub(r' {2,}', ' ', cleaned).strip()

    @classmethod
    def sanitize_item(cls, item: dict, supplier: str | None = None) -> dict:
        """
        Очищає всі текстові поля товару in-place.

        Args:
            item: словник полів товару (result з _clean_item)
            supplier: нормалізоване ім'я постачальника (напр. config.supplier_name) —
                вмикає LP/Viatec-специфічні правила в sanitize().

        Returns:
            Той самий словник з очищеними полями
        """
        for field in cls.SANITIZED_FIELDS:
            if item.get(field):
                item[field] = cls.sanitize(item[field], supplier=supplier)
        return item
