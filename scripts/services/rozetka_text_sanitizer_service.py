"""
services/rozetka_text_sanitizer_service.py
--------------------------------------------
Санітайзер текстових полів (<name>/<name_ua>, <description>/<description_ua>)
для Rozetka-фіду.

Розв'язує 3 незалежні задачі:

1. Емодзі в описах.
   Rozetka не потребує емодзі — прибираються з <description>/<description_ua>.

2. "Розпродажні" мітки в назвах товару.
   Постачальник додає в <name>/<name_ua> службовий префікс типу:
       "Розпродаж (4160) Акумулятор AGM LPM 6V - 14 Ah 09/23"
       "Розпродаж Акумулятор AGM LPM 12V - 18 Ah"
       "Распродажа (3861) Аккумулятор AGM LPM 12V - 5 Ah (11-2024)"
       "Распродажа Аккумулятор AGM LPM 12V - 18 Ah"
   Префікс (слово + опційний "(ID)" в дужках) видаляється, товарна назва
   лишається чистою: "Акумулятор AGM LPM 6V - 14 Ah 09/23".

3. Причина уцінки в описах.
   Постачальник додає в опис службове речення типу:
       "Причина уцінки, акумулятор 2024 року."
       "Причина уценки, аккумулятор 2024 года."
   Речення видаляється цілком (від "Причина уц[ін|ен]ки," до першої крапки).
   Окремі згадки слова "уцінка"/"уценка" (в будь-якій формі), що лишились
   поза цим реченням, видаляються теж.

Усі функції працюють пооферно (через <offer id="...">...</offer>), а не
одним re.sub по всьому XML — це гарантує, що трансформація ніколи не
зачепить <name> магазину (shop-рівень, поза <offer>) чи інші структурні поля.
Один "поганий" офер (відсутній тег, дивна CDATA) не валить обробку решти —
regex просто не знаходить збігу і тег лишається без змін.

Виклик — з generate_rozetka_feed.py, ДО set_shop_name (щоб не залежати від
того, що перший <name> — це вже назва магазину, хоча наші regex і так
шукають теги лише всередині <offer>, тому порядок відносно set_shop_name
не є критичним):

    from services.rozetka_text_sanitizer_service import (
        strip_sale_labels_from_names,
        strip_emojis_from_descriptions,
        strip_discount_reason_from_descriptions,
    )
    updated_xml = strip_sale_labels_from_names(updated_xml)
    updated_xml = strip_emojis_from_descriptions(updated_xml)
    updated_xml = strip_discount_reason_from_descriptions(updated_xml)

    # або одним викликом (той самий порядок):
    from services.rozetka_text_sanitizer_service import sanitize_rozetka_text
    updated_xml = sanitize_rozetka_text(updated_xml)
"""

from __future__ import annotations

import re
from functools import lru_cache
from typing import Callable, Final

# ---------------------------------------------------------------------------
# Offer / CDATA parsing — спільне для всіх трьох трансформацій
# ---------------------------------------------------------------------------

_OFFER_RE: Final[re.Pattern[str]] = re.compile(
    r'<offer\s+id="([^"]+)"([^>]*)>(.*?)</offer>',
    re.DOTALL,
)
_CDATA_RE: Final[re.Pattern[str]] = re.compile(r"<!\[CDATA\[(.*?)\]\]>", re.DOTALL)
_MULTI_SPACE_RE: Final[re.Pattern[str]] = re.compile(r"[ \t]{2,}")

# Теги, що підлягають кожній трансформації — config, не hardcode в логіці.
_NAME_TAGS: Final[tuple[str, ...]] = ("name", "name_ua")
_DESCRIPTION_TAGS: Final[tuple[str, ...]] = ("description", "description_ua")


@lru_cache(maxsize=None)
def _tag_re(tag: str) -> re.Pattern[str]:
    return re.compile(rf"<{tag}>(.*?)</{tag}>", re.DOTALL)


def _unwrap_cdata(raw: str) -> tuple[str, bool]:
    """Знімає CDATA-обгортку зі значення тегу. Повертає (текст, чи_була_cdata)."""
    stripped = raw.strip()
    m = _CDATA_RE.fullmatch(stripped)
    if m:
        return m.group(1), True
    return raw, False


def _rewrap(inner: str, was_cdata: bool) -> str:
    """Повертає CDATA-обгортку назад, якщо вона була знята _unwrap_cdata."""
    return f"<![CDATA[{inner}]]>" if was_cdata else inner


def _transform_offer_tags(
    xml: str,
    tags: tuple[str, ...],
    func: Callable[[str], str],
) -> tuple[str, int]:
    """
    Застосовує func(текст_тегу) -> новий_текст до кожного з `tags` всередині
    кожного <offer>...</offer>. CDATA-обгортка (якщо була) зберігається.

    Тег відсутній в оферi / порожній — пропускається без помилки (idempotent,
    не падає на одному товарі).

    Returns:
        (оновлений_xml, кількість_тегів, значення яких було змінено)
    """
    changed = 0

    def _on_offer(m: re.Match[str]) -> str:
        nonlocal changed
        offer_id, tail_attrs, body = m.group(1), m.group(2), m.group(3)

        for tag in tags:
            pattern = _tag_re(tag)

            def _on_tag(tm: re.Match[str]) -> str:
                nonlocal changed
                inner, was_cdata = _unwrap_cdata(tm.group(1))
                if not inner:
                    return tm.group(0)
                new_inner = func(inner)
                if new_inner == inner:
                    return tm.group(0)
                changed += 1
                return f"<{tag}>{_rewrap(new_inner, was_cdata)}</{tag}>"

            body = pattern.sub(_on_tag, body)

        return f'<offer id="{offer_id}"{tail_attrs}>{body}</offer>'

    result = _OFFER_RE.sub(_on_offer, xml)
    return result, changed


# ---------------------------------------------------------------------------
# 1. Емодзі
# ---------------------------------------------------------------------------

# Основні Unicode-блоки емодзі + модифікатори (skin tone, ZWJ, variation selector,
# keycap). Діапазони не перетинаються — порядок у класі символів не критичний.
_EMOJI_RE: Final[re.Pattern[str]] = re.compile(
    "["
    "\U0001F1E6-\U0001F1FF"  # regional indicators (прапори)
    "\U0001F300-\U0001F5FF"  # symbols & pictographs
    "\U0001F600-\U0001F64F"  # emoticons
    "\U0001F680-\U0001F6FF"  # transport & map
    "\U0001F700-\U0001F77F"  # alchemical symbols
    "\U0001F780-\U0001F7FF"  # geometric shapes extended
    "\U0001F800-\U0001F8FF"  # supplemental arrows-c
    "\U0001F900-\U0001F9FF"  # supplemental symbols & pictographs
    "\U0001FA00-\U0001FA6F"  # chess symbols
    "\U0001FA70-\U0001FAFF"  # symbols & pictographs extended-a
    "\U00002600-\U000026FF"  # misc symbols (☀☂☎ тощо)
    "\U00002700-\U000027BF"  # dingbats (✅❌❤ тощо)
    "\U00002300-\U000023FF"  # misc technical (⏰⌚⏳ тощо)
    "\U00002B00-\U00002BFF"  # misc symbols and arrows
    "\U0001F3FB-\U0001F3FF"  # skin tone modifiers
    "\uFE0F"                 # variation selector-16
    "\u200D"                 # zero-width joiner (складені емодзі)
    "\u20E3"                 # combining enclosing keycap
    "]+",
    flags=re.UNICODE,
)


def strip_emojis_from_descriptions(xml: str) -> str:
    """
    Видаляє емодзі з <description>/<description_ua>.

    Після видалення схлопує зайві пробіли, що могли лишитись на місці емодзі.
    """

    def _strip(inner: str) -> str:
        cleaned = _EMOJI_RE.sub("", inner)
        return _MULTI_SPACE_RE.sub(" ", cleaned).strip()

    result, changed = _transform_offer_tags(xml, _DESCRIPTION_TAGS, _strip)
    if changed:
        print(f"😀 Rozetka емодзі: видалено з {changed} описів")
    else:
        print("😀 Rozetka емодзі: збігів не знайдено")
    return result


# ---------------------------------------------------------------------------
# 2. "Розпродажна" мітка в назвах
# ---------------------------------------------------------------------------

# "Розпродаж (4160) Акумулятор..." / "Розпродаж Акумулятор..." / RU-варіанти.
# Опційні дужки з ID — не критичні для матчу, слово + необов'язковий "(ID)" +
# розділовий пробіл видаляються одним проходом з початку рядка.
_SALE_LABEL_RE: Final[re.Pattern[str]] = re.compile(
    r"^\s*(?:Розпродаж|Распродажа)\s*(?:\(\s*\d+\s*\))?\s*",
    re.IGNORECASE,
)

# Мітка, яка додається в кінець очищеної назви, щоб візуально позначати sale-товар
# після того, як службовий префікс видалено (артикул в <name> для ідентифікації все одно є, ID з дужок втрачено разом з префіксом).
_SALE_SUFFIX: Final[str] = " s"


def strip_sale_labels_from_names(xml: str) -> str:
    """
    Видаляє службовий префікс "Розпродаж (ID)" / "Распродажа (ID)" (з ID або
    без нього) з початку <name>/<name_ua> і додає до залишку назви маркер " s"
    (від sale), щоб sale-товар було візуально відрізняти вже після видалення префікса
    (артикул в <name> залишається той же, тому товар все одно можна знайти за артикулом).

    Приклад:
        "Розпродаж (4160) Акумулятор AGM LPM 6V - 14 Ah 09/23"
        → "Акумулятор AGM LPM 6V - 14 Ah 09/23 s"

    Ідемпотентність: другий прогін на вже очищеній назві нічого не змінює — префікс вже
    видалено, тому регекс більше не збігається і суфікс вдруге не додається.
    """

    def _strip(inner: str) -> str:
        new_inner, matched = _SALE_LABEL_RE.subn("", inner, count=1)
        new_inner = new_inner.strip()
        if matched and new_inner and not new_inner.endswith(_SALE_SUFFIX):
            new_inner = f"{new_inner}{_SALE_SUFFIX}"
        return new_inner

    result, changed = _transform_offer_tags(xml, _NAME_TAGS, _strip)
    if changed:
        print(f"🏷️  Rozetka sale-мітки: видалено з {changed} назв (Розпродаж/Распродажа)")
    else:
        print("🏷️  Rozetka sale-мітки: збігів не знайдено")
    return result


# ---------------------------------------------------------------------------
# 3. "Причина уцінки" в описах
# ---------------------------------------------------------------------------

# Ціле речення "Причина уцінки, ... ." / "Причина уценки, ... ." — від мітки
# до першої крапки включно. "уц(?:ін|ен)ки" покриває UA- та RU-форму слова.
_DISCOUNT_REASON_SENTENCE_RE: Final[re.Pattern[str]] = re.compile(
    r"Причина\s+уц(?:ін|ен)ки\s*,[^.]*\.\s*",
    re.IGNORECASE,
)

# Будь-яка інша згадка слова "уцінка"/"уценка" (в будь-якій відмінковій формі),
# що лишилась поза реченням вище — видаляється як окреме слово.
_DISCOUNT_WORD_RE: Final[re.Pattern[str]] = re.compile(
    r"\bуц(?:ін|ен)к\w*\b",
    re.IGNORECASE,
)

# Прибирає розділові знаки/пробіли, що лишаються на початку рядка після
# видалення першого речення (напр. залишковий ", " чи ". ").
_LEADING_PUNCT_RE: Final[re.Pattern[str]] = re.compile(r"^[,;.\s]+")


def strip_discount_reason_from_descriptions(xml: str) -> str:
    """
    Видаляє службове речення "Причина уцінки/уценки, ...." (повністю, до першої
    крапки) з <description>/<description_ua>, а також будь-які самостійні
    згадки слова "уцінка"/"уценка", що лишились поза цим реченням.

    Приклад:
        "Причина уцінки, акумулятор 2024 року. Зовнішня універсальна АКБ..."
        → "Зовнішня універсальна АКБ..."
    """

    def _strip(inner: str) -> str:
        cleaned = _DISCOUNT_REASON_SENTENCE_RE.sub("", inner)
        cleaned = _DISCOUNT_WORD_RE.sub("", cleaned)
        cleaned = _MULTI_SPACE_RE.sub(" ", cleaned).strip()
        cleaned = _LEADING_PUNCT_RE.sub("", cleaned).strip()
        return cleaned

    result, changed = _transform_offer_tags(xml, _DESCRIPTION_TAGS, _strip)
    if changed:
        print(f"🏷️  Rozetka причина уцінки: видалено з {changed} описів")
    else:
        print("🏷️  Rozetka причина уцінки: збігів не знайдено")
    return result


# ---------------------------------------------------------------------------
# Public: комбінований виклик
# ---------------------------------------------------------------------------

def sanitize_rozetka_text(xml: str) -> str:
    """
    Застосовує всі три санітайзери в детермінованому порядку:
        1. strip_sale_labels_from_names            — <name>/<name_ua>
        2. strip_emojis_from_descriptions           — <description>/<description_ua>
        3. strip_discount_reason_from_descriptions  — <description>/<description_ua>

    Зручний єдиний entrypoint для generate_rozetka_feed.py, якщо не потрібен
    контроль над кожним кроком окремо.
    """
    xml = strip_sale_labels_from_names(xml)
    xml = strip_emojis_from_descriptions(xml)
    xml = strip_discount_reason_from_descriptions(xml)
    return xml
