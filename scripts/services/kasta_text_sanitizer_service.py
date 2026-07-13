"""
services/kasta_text_sanitizer_service.py
------------------------------------------
Санітайзер текстових полів Kasta-фіду.

Kasta-фід (generate_kasta_feed.py, FEED_URL_PROM, languages=uk,ru) містить дві
незалежні мовні версії опису безпосередньо з джерела:
    <description>       — RU-версія
    <description_ua>     — UA-версія

Кожна версія отримує свій власний набір трансформацій. На відміну від Epicenter
(де є лише один <description lang="ua"> і санітайзер можна безпечно застосовувати
до всього XML) та Rozetka (де обидві мовні версії чистяться ОДНАКОВО), тут кожна
трансформація має бути застосована лише в межах ОДНОГО заданого тегу — інакше вона
зачепить сусідню мовну версію.

1. <description> (RU) — заміна українських літер, що можуть потрапити в RU-текст
   (мультимовне джерело іноді змішує UA/RU написання):
       і / І  →  и / И
       ї / Ї  →  и / И
       '      →  ъ / Ъ   (регістр обирається за літерою ПЕРЕД апострофом: якщо
                            вона в верхньому регістрі — 'Ъ', інакше — 'ъ')
   Літери в верхньому регістрі (І/Ї) прямо в завданні не згадані, але додані
   за аналогією до ы/Ы, ъ/Ъ нижче — щоб правило покривало й Caps-написання.

2. <description_ua> (UA) — два незалежні кроки:
   a) sanitize_description_ua_chars — замінює кириличні символи, відсутні в
      українському алфавіті (той самий алгоритм, що
      epicenter_text_sanitizer_service.sanitize_russian_chars, але застосований
      лише в межах <description_ua>, а не по всьому XML):
          ы / Ы  →  и / И
          ъ / Ъ  →  ' (апостроф)
   b) strip_external_links_from_description_ua — видаляє зовнішні посилання
      (та сама логіка, що epicenter_text_sanitizer_service.strip_external_links,
      застосована лише в межах <description_ua>):
          "Детальніше: https://..."  — мітка разом з URL
          будь-який bare URL у тексті — якщо не є структурним HTML-полем
          (href="...", src='...', src=... без лапок — лишаються без змін)

Обидві функції CDATA-safe і працюють пооферно (через <offer id="...">...</offer>),
за зразком rozetka_text_sanitizer_service._transform_offer_tags — один "поганий"
офер (відсутній тег, дивна CDATA) не валить обробку решти фіду.

Виклик — з generate_kasta_feed.py, ПІСЛЯ inject_params_into_descriptions (щоб
санітайзер захопив і вставлений туди HTML-блок особливостей теж):

    from services.kasta_text_sanitizer_service import sanitize_kasta_text
    updated_xml = inject_params_into_descriptions(updated_xml)
    updated_xml = sanitize_kasta_text(updated_xml)
"""

from __future__ import annotations

import re
from functools import lru_cache
from typing import Callable, Final

# ---------------------------------------------------------------------------
# Offer / CDATA parsing — спільне для обох мовних версій
# ---------------------------------------------------------------------------

_OFFER_RE: Final[re.Pattern[str]] = re.compile(
    r'<offer\s+id="([^"]+)"([^>]*)>(.*?)</offer>',
    re.DOTALL,
)
_CDATA_RE: Final[re.Pattern[str]] = re.compile(r"<!\[CDATA\[(.*?)\]\]>", re.DOTALL)
_MULTI_SPACE_RE: Final[re.Pattern[str]] = re.compile(r"[ \t]{2,}")


@lru_cache(maxsize=None)
def _tag_re(tag: str) -> re.Pattern[str]:
    """Точна назва тегу без атрибутів — відповідає формату Kasta-фіду.

    Точний літерал тегу гарантує, що "description" НЕ матчить "description_ua"
    і навпаки.
    """
    return re.compile(rf"<{tag}>(.*?)</{tag}>", re.DOTALL)


def _unwrap_cdata(raw: str) -> tuple[str, bool]:
    """Знімає CDATA-обгортку зі значення тегу. Повертає (текст, чи_була_cdata)."""
    stripped = raw.strip()
    m = _CDATA_RE.fullmatch(stripped)
    if m:
        return m.group(1), True
    return raw, False


def _rewrap_cdata(inner: str, was_cdata: bool) -> str:
    """Повертає CDATA-обгортку назад, якщо вона була знята _unwrap_cdata."""
    return f"<![CDATA[{inner}]]>" if was_cdata else inner


def _transform_tag(xml: str, tag: str, func: Callable[[str], str]) -> tuple[str, int]:
    """
    Застосовує func(текст_тегу) -> новий_текст до ОДНОГО заданого тегу в межах
    кожного <offer>...</offer>. CDATA-обгортка (якщо була) зберігається.

    Тег відсутній в оферi / порожній — пропускається без помилки (safe fallback,
    один "проблемний" офер не блокує обробку решти).

    Args:
        xml:  повний XML-рядок фіду.
        tag:  назва тегу без дужок ("description" або "description_ua").
        func: чиста функція трансформації тексту тегу.

    Returns:
        (оновлений_xml, кількість тегів, значення яких було змінено)
    """
    changed = 0
    pattern = _tag_re(tag)

    def _on_offer(m: re.Match[str]) -> str:
        nonlocal changed
        offer_id, tail_attrs, body = m.group(1), m.group(2), m.group(3)

        def _on_tag(tm: re.Match[str]) -> str:
            nonlocal changed
            inner, was_cdata = _unwrap_cdata(tm.group(1))
            if not inner:
                return tm.group(0)
            new_inner = func(inner)
            if new_inner == inner:
                return tm.group(0)
            changed += 1
            return f"<{tag}>{_rewrap_cdata(new_inner, was_cdata)}</{tag}>"

        new_body = pattern.sub(_on_tag, body)
        if new_body == body:
            return m.group(0)
        return f'<offer id="{offer_id}"{tail_attrs}>{new_body}</offer>'

    result = _OFFER_RE.sub(_on_offer, xml)
    return result, changed


# ---------------------------------------------------------------------------
# 1. <description> (RU) — українські літери, що потрапили в RU-текст
# ---------------------------------------------------------------------------

# Порядок не критичний — символи не перетинаються.
_RU_CHAR_REPLACEMENTS: Final[tuple[tuple[str, str], ...]] = (
    ("і", "и"),
    ("І", "И"),
    ("ї", "и"),
    ("Ї", "И"),
)

_APOSTROPHE_RE: Final[re.Pattern[str]] = re.compile(r"'")


def _apostrophe_to_hard_sign(m: re.Match[str]) -> str:
    """Обирає ъ/Ъ за регістром літери, що стоїть безпосередньо перед апострофом."""
    prev_char = m.string[m.start() - 1] if m.start() > 0 else ""
    return "Ъ" if prev_char.isupper() else "ъ"


def _sanitize_ru_text(text: str) -> str:
    for src, dst in _RU_CHAR_REPLACEMENTS:
        text = text.replace(src, dst)
    return _APOSTROPHE_RE.sub(_apostrophe_to_hard_sign, text)


def sanitize_description_ru_chars(xml: str) -> str:
    """
    Замінює в <description> (RU-версія) українські літери та апостроф:
        і / І  →  и / И
        ї / Ї  →  и / И
        '      →  ъ / Ъ

    Детермінований посимвольний прохід (і/ї) + один regex-прохід для
    контекстно-залежного апострофа. Працює лише в межах <description> —
    <description_ua>, <name>, <param> тощо не зачіпаються.

    Args:
        xml: повний XML-рядок фіду.

    Returns:
        XML з санітайзованим <description>.
    """
    result, changed = _transform_tag(xml, "description", _sanitize_ru_text)
    if changed:
        print(f"🔤 Kasta RU-санітайзер: оновлено {changed} <description>")
    else:
        print("🔤 Kasta RU-санітайзер: збігів не знайдено")
    return result


# ---------------------------------------------------------------------------
# 2a. <description_ua> (UA) — кириличні символи, відсутні в українському алфавіті
# ---------------------------------------------------------------------------

_UA_CHAR_REPLACEMENTS: Final[tuple[tuple[str, str], ...]] = (
    ("ы", "и"),
    ("Ы", "И"),
    ("ъ", "'"),
    ("Ъ", "'"),
)


def _sanitize_ua_text(text: str) -> str:
    for src, dst in _UA_CHAR_REPLACEMENTS:
        text = text.replace(src, dst)
    return text


def sanitize_description_ua_chars(xml: str) -> str:
    """
    Замінює в <description_ua> (UA-версія) кириличні символи, відсутні в
    українському алфавіті:
        ы / Ы  →  и / И
        ъ / Ъ  →  ' (апостроф)

    Детермінований, без регулярок — plain str.replace (як в
    epicenter_text_sanitizer_service.sanitize_russian_chars), але застосований
    лише в межах <description_ua>.

    Args:
        xml: повний XML-рядок фіду.

    Returns:
        XML з санітайзованим <description_ua>.
    """
    result, changed = _transform_tag(xml, "description_ua", _sanitize_ua_text)
    if changed:
        print(f"🔤 Kasta UA-санітайзер: оновлено {changed} <description_ua>")
    else:
        print("🔤 Kasta UA-санітайзер: збігів не знайдено")
    return result


# ---------------------------------------------------------------------------
# 2b. <description_ua> (UA) — зовнішні посилання
# ---------------------------------------------------------------------------

# «Детальніше: URL» — видаляє мітку разом з URL і попереднім роздільником.
_DETAILS_LINK_RE: Final[re.Pattern[str]] = re.compile(
    r'[,;\s]*Детальніше:\s*https?://[^\s<>"\']+',
    re.IGNORECASE,
)

# Будь-який bare URL у текстовому контексті. Negative lookbehind виключає URL
# у структурних HTML-позиціях (href="...", src='...', src=... без лапок), які
# іноді трапляються всередині опису як inline-розмітка.
#
# ВАЖЛИВО: на відміну від lookbehind-класу в epicenter_text_sanitizer_service
# (де серед виключень випадково опинився ще й пробіл " "), тут пробіл НЕ
# виключений навмисно — інакше найпоширеніший випадок ("текст http://url ще
# текст", URL після звичайного пробілу в реченні) взагалі ніколи б не
# видалявся, що суперечить самій меті цього кроку ("будь-який bare URL —
# якщо не структурне поле").
_BARE_URL_RE: Final[re.Pattern[str]] = re.compile(
    r'(?<![>"\'=])https?://[^\s<>\'"]+',
)


def _strip_links_from_text(text: str) -> str:
    text, _ = _DETAILS_LINK_RE.subn("", text)
    text, _ = _BARE_URL_RE.subn("", text)
    return _MULTI_SPACE_RE.sub(" ", text).strip()


def strip_external_links_from_description_ua(xml: str) -> str:
    """
    Видаляє зовнішні посилання з тексту <description_ua>.

    Прохід 1 — «Детальніше: URL»:
        Видаляє мітку разом з URL і попереднім роздільником (кома, крапка з
        комою, пробіл). Приклад: ", Детальніше: https://example.com/..." → ""

    Прохід 2 — bare URLs у тексті:
        Видаляє будь-який https?://... що не є структурним HTML-полем
        (href="...", src='...', src=... без лапок — лишаються без змін).

    Після видалення схлопує зайві пробіли, що могли лишитись на місці видаленого
    URL — інакше два прилеглих пробіли (один до URL, один після) лишалисяб поруч і
    псували б справжню ідемпотентність (другий прогон над вже очищеним XML знаходив
    би ще один пробіл, і вважав би це за зміну). Ще безпечно тут, бо схлопується
    лише вже витягнутий текст тегу, а не весь XML (порівняйте з глобальним
    підходом в epicenter_text_sanitizer_service.strip_external_links, де таке
    схлопування знищило б відступи у всьому файлі).

    Args:
        xml: повний XML-рядок фіду.

    Returns:
        XML з видаленими external URLs з <description_ua>.
    """
    result, changed = _transform_tag(xml, "description_ua", _strip_links_from_text)
    if changed:
        print(f"🔗 Kasta посилання: видалено з {changed} <description_ua>")
    else:
        print("🔗 Kasta посилання: збігів не знайдено")
    return result


# ---------------------------------------------------------------------------
# Public: комбінований виклик
# ---------------------------------------------------------------------------

def sanitize_kasta_text(xml: str) -> str:
    """
    Застосовує всі санітайзери Kasta-фіду в детермінованому порядку:
        1. sanitize_description_ru_chars           — <description>
        2. sanitize_description_ua_chars            — <description_ua>
        3. strip_external_links_from_description_ua — <description_ua>

    Зручний єдиний entrypoint для generate_kasta_feed.py.

    Args:
        xml: повний XML-рядок фіду.

    Returns:
        Санітайзований XML.
    """
    xml = sanitize_description_ru_chars(xml)
    xml = sanitize_description_ua_chars(xml)
    xml = strip_external_links_from_description_ua(xml)
    return xml
