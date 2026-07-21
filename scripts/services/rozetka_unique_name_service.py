"""
services/rozetka_unique_name_service.py
-----------------------------------------
Rozetka вимагає, щоб <name> (назва) та <name_ua> (UA-назва) були унікальними
в межах усього фіду. Prom-фід формується з постачальницьких даних, де кілька
товарів (варіанти виконання, кольору, модифікації) можуть мати однакову
назву — Rozetka відхиляє такі оффери з помилкою «Назва ... не унікальне.».

Дублікат шукається НЕЗАЛЕЖНО для кожного тегу (<name> і <name_ua> — це різні
помилки в звіті Rozetka, зустрічаються як разом, так і окремо). Але якщо для
конкретного офера дублюється хоча б один з двох тегів, суфікс " (ID)"
дописується ОБОМ тегам цього офера (тим, що присутні), а не лише тому, що
формально дублюється. Це навмисно: RU- та UA-назва одного товару — це той
самий офер у двох мовах, і якщо вони розходяться (один із суфіксом, інший —
без), Rozetka додатково скаржиться, що назви товару в різних мовних версіях
не збігаються.

Сервіс не намагається вигадати "правильну" унікальну назву — це вимагало б
знання товарної специфіки. Замість цього ID — атрибут id тега <offer>
(той самий "ID з прайсу продавця", що видно у звіті валідації Rozetka).

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

# Теги, чия унікальність вимагається Rozetka.
_NAME_TAGS: Final[tuple[str, ...]] = ("name", "name_ua")
_TAG_PATTERNS: Final[dict[str, re.Pattern[str]]] = {
    tag: re.compile(rf"<{tag}>(.*?)</{tag}>", re.DOTALL) for tag in _NAME_TAGS
}


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
    """Дописує " (ID)" в кінець значення тегу, зберігаючи CDATA-обгортку, якщо вона є."""
    suffix = f" ({offer_id})"
    cdata_match = _CDATA_RE.fullmatch(raw_value.strip())
    if cdata_match:
        return f"<![CDATA[{cdata_match.group(1)}{suffix}]]>"
    return f"{raw_value}{suffix}"


# ---------------------------------------------------------------------------
# Прохід 1 — знайти дубльовані нормалізовані значення для кожного тегу окремо
# ---------------------------------------------------------------------------

def _find_duplicated_values(xml: str, tag: str) -> set[str]:
    """Повертає множину нормалізованих значень тегу `tag`, що зустрічаються
    у фіді більше одного разу. Оффери без тега або з порожнім значенням
    пропускаються — сервіс ніколи не падає через один товар.
    """
    tag_re = _TAG_PATTERNS[tag]
    counts: Counter[str] = Counter()

    for offer_match in _OFFER_RE.finditer(xml):
        tag_match = tag_re.search(offer_match.group(3))
        if not tag_match:
            continue
        normalized = _normalize(tag_match.group(1))
        if normalized:
            counts[normalized] += 1

    return {value for value, count in counts.items() if count > 1}


# ---------------------------------------------------------------------------
# Прохід 2 — застосувати суфікс до ОБОХ тегів офера, якщо хоч один дублюється
# ---------------------------------------------------------------------------

def _offer_tag_matches(body: str) -> dict[str, re.Match[str]]:
    """Повертає знайдені в тілі офера збіги для кожного з `_NAME_TAGS`, що присутній."""
    matches: dict[str, re.Match[str]] = {}
    for tag, tag_re in _TAG_PATTERNS.items():
        tag_match = tag_re.search(body)
        if tag_match:
            matches[tag] = tag_match
    return matches


def _apply_suffix_to_offer(body: str, offer_id: str, matches: dict[str, re.Match[str]]) -> str:
    """Дописує " (ID)" до значень усіх переданих тегів у тілі офера.

    Правки застосовуються від останньої позиції до першої, щоб зсув
    довжини рядка після однієї заміни не ламав офсети іншої.
    """
    for tag_match in sorted(matches.values(), key=lambda m: m.start(1), reverse=True):
        new_value = _append_suffix(tag_match.group(1), offer_id)
        body = body[: tag_match.start(1)] + new_value + body[tag_match.end(1):]
    return body


# ---------------------------------------------------------------------------
# Public
# ---------------------------------------------------------------------------

def deduplicate_offer_names(xml: str) -> str:
    """
    Робить <name> та <name_ua> унікальними в межах фіду — вимога Rozetka.

    Дублікат нормалізованого значення шукається незалежно для кожного тегу.
    Але якщо для офера дублюється хоча б один з двох тегів, суфікс " (ID)"
    дописується ОБОМ тегам цього офера (тим, що присутні) — щоб RU- та
    UA-назва товару завжди лишались парними і не виглядали як різні
    найменування одного й того ж товару.
    """
    duplicated_by_tag = {tag: _find_duplicated_values(xml, tag) for tag in _NAME_TAGS}

    if not any(duplicated_by_tag.values()):
        for tag in _NAME_TAGS:
            print(f"🔁 Rozetka унікальність <{tag}>: дублікатів не знайдено")
        return xml

    appended_by_tag: Counter[str] = Counter()

    def _fix_offer(m: re.Match[str]) -> str:
        offer_id, tail_attrs, body = m.group(1), m.group(2), m.group(3)

        matches = _offer_tag_matches(body)
        if not matches:
            return m.group(0)

        needs_suffix = any(
            _normalize(tag_match.group(1)) in duplicated_by_tag[tag]
            for tag, tag_match in matches.items()
        )
        if not needs_suffix:
            return m.group(0)

        new_body = _apply_suffix_to_offer(body, offer_id, matches)
        for tag in matches:
            appended_by_tag[tag] += 1

        return f'<offer id="{offer_id}"{tail_attrs}>{new_body}</offer>'

    result = _OFFER_RE.sub(_fix_offer, xml)

    for tag in _NAME_TAGS:
        appended = appended_by_tag[tag]
        dup_count = len(duplicated_by_tag[tag])
        if appended:
            print(
                f"🔁 Rozetka унікальність <{tag}>: {appended} товарів отримали суфікс (ID) "
                f"| дубльованих значень: {dup_count}"
            )
        else:
            print(f"🔁 Rozetka унікальність <{tag}>: дублікатів не знайдено")

    return result
