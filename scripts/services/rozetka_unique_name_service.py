"""
services/rozetka_unique_name_service.py
-----------------------------------------
Rozetka вимагає, щоб <name> (назва) та <name_ua> (UA-назва) були унікальними
в межах усього фіду. Prom-фід формується з постачальницьких даних, де кілька
товарів (варіанти виконання, кольору, модифікації) можуть мати однакову
назву — Rozetka відхиляє такі оффери з помилкою «Назва ... не унікальне.».

Сервіс не намагається вигадати "правильну" унікальну назву — це вимагало б
знання товарної специфіки. Замість цього для кожного офера, чиє нормалізоване
значення <name> або <name_ua> повторюється у фіді, в кінець значення дописується
" (ID)", де ID — атрибут id тега <offer> (той самий "ID з прайсу продавця",
що видно у звіті валідації Rozetka).

<name> та <name_ua> дедуплікуються НЕЗАЛЕЖНО один від одного — в звіті Rozetka
це різні помилки, що зустрічаються як разом, так і окремо (див. приклад
з реального звіту: частина офферів мала не унікальними обидва теги,
частина — лише один з них).

Використання в generate_rozetka_feed.py:
    from services.rozetka_unique_name_service import deduplicate_offer_names
    updated_xml = deduplicate_offer_names(updated_xml)
"""

from __future__ import annotations

import html
import re
from collections import Counter
from typing import Final

# ---------------------------------------------------------------------------
# Regex — одноразова компіляція
# ---------------------------------------------------------------------------

_OFFER_RE: Final[re.Pattern[str]] = re.compile(
    r'<offer\s+id="([^"]+)"([^>]*)>(.*?)</offer>',
    re.DOTALL,
)
_CDATA_RE: Final[re.Pattern[str]] = re.compile(r"<!\[CDATA\[(.*?)\]\]>", re.DOTALL)
_HTML_TAG_RE: Final[re.Pattern[str]] = re.compile(r"<[^>]+>")

# Теги, чия унікальність вимагається Rozetka — дедуплікуються незалежно один від одного.
# УВАГА: тег назви — саме <name>, а НЕ <n> (у деяких коментарях по проєкту
# помилково фігурує "<n>" — це не відповідає реальній структурі фіду).
_NAME_TAGS: Final[tuple[str, ...]] = ("name", "name_ua")


def _tag_re(tag: str) -> re.Pattern[str]:
    return re.compile(rf"<{tag}>(.*?)</{tag}>", re.DOTALL)


# ---------------------------------------------------------------------------
# Text helpers
# ---------------------------------------------------------------------------

def _clean_text(value: str) -> str:
    """Знімає CDATA-обгортку/HTML-теги/entities, схлопує пробіли — лише для порівняння."""
    value = _CDATA_RE.sub(lambda m: m.group(1), value)
    value = _HTML_TAG_RE.sub("", value)
    value = html.unescape(value)
    value = value.replace("\xa0", " ")
    return re.sub(r"\s+", " ", value).strip()


def _normalize(value: str) -> str:
    """Нормалізований ключ для порівняння дублікатів (не для виводу)."""
    return _clean_text(value).casefold()


def _append_suffix(raw_value: str, offer_id: str) -> str:
    """
    Дописує " (ID)" в кінець значення тегу, зберігаючи CDATA-обгортку, якщо вона є.
    """
    suffix = f" ({offer_id})"
    cdata_match = _CDATA_RE.fullmatch(raw_value.strip())
    if cdata_match:
        return f"<![CDATA[{cdata_match.group(1)}{suffix}]]>"
    return f"{raw_value}{suffix}"


# ---------------------------------------------------------------------------
# Core: dedupe одного тегу по всьому фіду (два незалежні проходи)
# ---------------------------------------------------------------------------

def _dedupe_tag(xml: str, tag: str) -> tuple[str, int, int]:
    """
    Дедуплікує значення одного тегу (<name> або <name_ua>) по всьому фіду.

    Прохід 1 — рахує частоту нормалізованих значень.
    Прохід 2 — усім офферам з дубльованим значенням (включно з першим —
    щоб результат не залежав від порядку офферів у XML) дописує " (ID)".

    Оффери без тега або з порожнім значенням пропускаються — сервіс
    ніколи не падає через один товар.

    Returns:
        (оновлений_xml, кількість_офферів_з_суфіксом, кількість_дубльованих_значень)
    """
    tag_re = _tag_re(tag)

    counts: Counter[str] = Counter()

    def _count_offer(m: re.Match[str]) -> str:
        tag_match = tag_re.search(m.group(3))
        if tag_match:
            normalized = _normalize(tag_match.group(1))
            if normalized:
                counts[normalized] += 1
        return m.group(0)

    _OFFER_RE.sub(_count_offer, xml)

    duplicated_values = {value for value, count in counts.items() if count > 1}
    if not duplicated_values:
        return xml, 0, 0

    appended = 0

    def _fix_offer(m: re.Match[str]) -> str:
        nonlocal appended
        offer_id, tail_attrs, body = m.group(1), m.group(2), m.group(3)

        tag_match = tag_re.search(body)
        if not tag_match:
            return m.group(0)

        normalized = _normalize(tag_match.group(1))
        if normalized not in duplicated_values:
            return m.group(0)

        new_value = _append_suffix(tag_match.group(1), offer_id)
        body = body[: tag_match.start(1)] + new_value + body[tag_match.end(1):]
        appended += 1
        return f'<offer id="{offer_id}"{tail_attrs}>{body}</offer>'

    result = _OFFER_RE.sub(_fix_offer, xml)
    return result, appended, len(duplicated_values)


# ---------------------------------------------------------------------------
# Public
# ---------------------------------------------------------------------------

def deduplicate_offer_names(xml: str) -> str:
    """
    Робить <name> та <name_ua> унікальними в межах фіду — вимога Rozetka.

    Для кожного тега — незалежний прохід: якщо нормалізоване значення
    (без CDATA/HTML/entities, casefold) зустрічається у фіді більше 1 разу,
    до значення КОЖНОГО офера з цим значенням (включно з першим) дописується
    " (ID)", де ID — id офера. Суфікс отримують усі, а не лише "другий і далі",
    щоб результат не залежав від порядку офферів у XML і всі дублікати у фіді
    виглядали єдинообразно.
    """
    for tag in _NAME_TAGS:
        xml, appended, dup_count = _dedupe_tag(xml, tag)
        if appended:
            print(
                f"🔁 Rozetka унікальність <{tag}>: {appended} товарів отримали суфікс (ID) "
                f"| дубльованих значень: {dup_count}"
            )
        else:
            print(f"🔁 Rozetka унікальність <{tag}>: дублікатів не знайдено")

    return xml
