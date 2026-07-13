"""
services/epicenter_text_sanitizer_service.py
--------------------------------------------
Санітайзер тексту для Епіцентр-фіду.

Замінює кириличні символи, відсутні в українському алфавіті:
    ы / Ы  →  и / И
    ъ / Ъ  →  ' (апостроф)

Видаляє HTML-сміття, що потрапляє з промовського фіду (напр. з Google Translate):
    class="Y2IQFc", class="...", тощо

Видаляє зовнішні посилання з текстових полів (описів):
    "Детальніше: https://..."  — мітка разом з URL
    будь-який bare URL у тексті — якщо не є структурним XML-полем

Виклик відбувається над повним XML-рядком фіду в самому кінці пайплайну —
після всіх трансформацій (inject_epicenter_attrs, normalize_name_description_tags,
strip_prom_offer_fields), щоб гарантовано захопити сміття у будь-яких полях:
описах, назвах, значеннях атрибутів, CDATA-секціях.

Безпечно: XML-теги та атрибути (paramcode, lang тощо) не містять слова «class»
та кирилиці — заміна їх не зачіпає.
Структурні URL (<picture>https://...) не видаляються — URL після > не матчиться.

Видаляє емодзі з <description lang="ua">...</description>:
    Пром-фід іноді містить емодзі в описах — видаляються всі Unicode-блоки емодзі.

Видаляє sale-мітки з назв <name lang="ua">...</name>:
    "Розпродаж (4160) Акумулятор..." / "Распродажа Аккумулятор..."
    службовий префікс видаляється, а в кінець очищеної назви додається маркер " s"
    (від sale), щоб sale-товар було візуально відрізняти (артикул в offer id залишається той же).

Видаляє причину уцінки/уценки з <description lang="ua">...</description>:
    "Причина уцінки, акумулятор 2024 року." / "Причина уценки, ..."
    речення видаляється цілком, окремі того видаляються будь-які інші згадки слова
    "уцінка"/"уценка", що лишились поза цим реченням.

emoji/sale-мітка/причина-уцінки функції працюють після normalize_name_description_tags
(коли <name>/<description> вже мають lang="ua" і description вже в CDATA) — тому шукають теги
з атрибутом (<name(?:\\s[^>]*)?>), а не голий <name> як у Rozetka-варіанті цього санітайзера.
Offer-обгортка (як у Rozetka-варіанті) тут не потрібна: до моменту виклику цих функцій shop-рівневий
<name> вже видалено (strip_prom_shop_block викликається першим кроком в main()) — глобальний regex по
всьому XML безпечний і відповідає стилю решти функцій цього файлу.
"""

from __future__ import annotations

import re
from typing import Final

# ---------------------------------------------------------------------------
# Кириличні символи, відсутні в українській мові
# ---------------------------------------------------------------------------

# Порядок не критичний — символи не перетинаються.
_CHAR_REPLACEMENTS: Final[tuple[tuple[str, str], ...]] = (
    ("ы", "и"),
    ("Ы", "И"),
    ("ъ", "'"),
    ("Ъ", "'"),
)

# ---------------------------------------------------------------------------
# Зовнішні посилання в текстових полях
# ---------------------------------------------------------------------------

# «Детальніше: URL» — видаляє разом з попереднім роздільником.
# Приклади що матчаться:
#   ", Детальніше: https://example.com/path?q=1"
#   " Детальніше: https://example.com"
_DETAILS_LINK_RE: Final[re.Pattern[str]] = re.compile(
    r'[,;\s]*Детальніше:\s*https?://[^\s<>"\']+'  ,
    re.IGNORECASE,
)

# Будь-який bare URL у текстовому контексті.
# Negative lookbehind виключає URLs у структурних XML-позиціях:
#   >  — безпосередній вміст XML-тегу: <picture>https://img.ua/...  → KEEP
#   "  — значення XML/HTML-атрибута:  href="https://..."            → KEEP
#   '  — значення XML/HTML-атрибута:  href='https://...'            → KEEP
#   =  — значення без лапок:          src=https://...               → KEEP
#
# ВИПРАВЛЕНО (2026-07-12, виявлено при розробці kasta_text_sanitizer_service.py):
# у класі виключень раніше випадково опинився ще й пробіл " ", через що
# НАЙПОШИРЕНІШИЙ випадок — bare URL просто в реченні з пробілом перед ним
# ("...на сторінці https://...") — ніколи не видалявся. Перевірено емпірично
# на живому data/markets/epicenter_feed.xml: до фіксу strip_external_links не
# видаляв ЖОДНОГО bare URL по всьому фіду (0 збігів), після фіксу — 2
# (обидва — реальні сторонні посилання в описах, жодного структурного поля
# серед них не виявлено).
#
# ЩЕ ОДНЕ ВИПРАВЛЕННЯ (той же день): додано \s? в кінці патерну —
# видалення URL з'їдає ще й ОДИН прилеглий пробіл після URL, інакше залишалисяб
# два пробіли поруч (один до URL, один після) — і це ламалоб справжню
# ідемпотентність (другий прогон знаходив би ще один “пробіл” для схлопування).
# НЕ використовуємо тут глобальний _MULTI_SPACE_RE.sub по всьому XML (як в
# strip_emojis_from_descriptions/strip_discount_reason_from_descriptions нижче) —
# ті дві функції скоповані лише в межах <description>, а strip_external_links
# навмисне глобальна (ловить посилання й у інших полях теж) — глобальне
# схлопування знищилоб відступи (2/4 пробіли) по всьому файлу.
_BARE_URL_RE: Final[re.Pattern[str]] = re.compile(
    r'(?<![>"\'=])https?://[^\s<>"\']+\s?',
)

# ---------------------------------------------------------------------------
# HTML class-атрибути
# ---------------------------------------------------------------------------

# Видаляє: class="Y2IQFc"  class='foo-bar'  тощо.
# \s+ перед class — щоб не залишати зайвих пробілів у відкриваючому тезі.
# [^"']* всередині лапок — безпечний жадібний: зупиняється на закриваючому
# символі того ж типу, що й відкриваючий.
_HTML_CLASS_RE: Final[re.Pattern[str]] = re.compile(
    r'\s+class=(["\'])[^"\']*\1',
    re.IGNORECASE,
)

# ---------------------------------------------------------------------------
# Emoji / sale-мітки / причина уцінки — теги з атрибутом lang="ua" (Epicenter-формат)
# ---------------------------------------------------------------------------

# (?:\s[^>]*)? — матчить теги як з атрибутами (<name lang="ua">), так і без них (<name>).
_NAME_TAG_RE: Final[re.Pattern[str]] = re.compile(
    r'(<name(?:\s[^>]*)?>)(.*?)(</name>)',
    re.DOTALL,
)
_DESCRIPTION_TAG_RE: Final[re.Pattern[str]] = re.compile(
    r'(<description(?:\s[^>]*)?>)(.*?)(</description>)',
    re.DOTALL,
)
_CDATA_RE: Final[re.Pattern[str]] = re.compile(r'<!\[CDATA\[(.*?)\]\]>', re.DOTALL)
_MULTI_SPACE_RE: Final[re.Pattern[str]] = re.compile(r'[ \t]{2,}')

# Основні Unicode-блоки емодзі + модифікатори (skin tone, ZWJ, variation selector, keycap).
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

# "Розпродаж (4160) Акумулятор..." / "Розпродаж Акумулятор..." / RU-варіанти.
_SALE_LABEL_RE: Final[re.Pattern[str]] = re.compile(
    r"^\s*(?:Розпродаж|Распродажа)\s*(?:\(\s*\d+\s*\))?\s*",
    re.IGNORECASE,
)
# Мітка, яка додається в кінець очищеної назви, щоб візуально позначати sale-товар
# після того, як службовий префікс видалено.
_SALE_SUFFIX: Final[str] = " s"

# Ціле речення "Причина уцінки, ... ." / "Причина уценки, ... ." — від мітки до першої крапки включно.
_DISCOUNT_REASON_SENTENCE_RE: Final[re.Pattern[str]] = re.compile(
    r"Причина\s+уц(?:ін|ен)ки\s*,[^.]*\.\s*",
    re.IGNORECASE,
)
# Будь-яка інша згадка слова "уцінка"/"уценка" (в будь-якій відмінковій формі), що лишилась поза реченням вище.
_DISCOUNT_WORD_RE: Final[re.Pattern[str]] = re.compile(
    r"\bуц(?:ін|ен)к\w*\b",
    re.IGNORECASE,
)
# Прибирає розділові знаки/пробіли, що лишаються на початку рядка після видалення першого речення.
_LEADING_PUNCT_RE: Final[re.Pattern[str]] = re.compile(r"^[,;.\s]+")


def _unwrap_cdata(raw: str) -> tuple[str, bool]:
    """Знімає CDATA-обгортку зі значення тегу. Повертає (текст, чи_була_cdata)."""
    stripped = raw.strip()
    m = _CDATA_RE.fullmatch(stripped)
    if m:
        return m.group(1), True
    return raw, False


def _rewrap_cdata(inner: str, was_cdata: bool) -> str:
    """Повертає CDATA-обгортку назад, якщо вона була знята _unwrap_cdata."""
    return f'<![CDATA[{inner}]]>' if was_cdata else inner


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def sanitize_russian_chars(xml: str) -> str:
    """
    Замінює кириличні символи, відсутні в українській мові.

    Детермінований, без регулярок — plain str.replace достатньо
    і є найшвидшим для посимвольних замін на великих рядках.

    Args:
        xml: повний XML-рядок фіду.

    Returns:
        XML з заміненими символами.
    """
    for src, dst in _CHAR_REPLACEMENTS:
        xml = xml.replace(src, dst)
    return xml


def strip_external_links(xml: str) -> str:
    """
    Видаляє зовнішні посилання з текстових полів (описів) фіду.

    Прохід 1 — «Детальніше: URL»:
        Видаляє мітку разом з URL і попереднім роздільником (кома, крапка з комою, пробіл).
        Приклад: ", Детальніше: https://seven-systems.com.ua/..." → ""

    Прохід 2 — bare URLs у тексті:
        Видаляє будь-який https?://... що не є структурним XML-полем.
        Не зачіпає:
            <picture>https://img.ua/...   (URL після  > )
            href="https://..."             (URL після  " )
            src='https://...'             (URL після  ' )

        З'їдає також ОДИН прилеглий пробіл після URL (якщо є), щоб не
        лишати подвійний пробіл на місці видаленого URL. Глобальне схлопування
        пробілів по всьому XML тут НЕ застосовується навмисно — це знищилоб
        відступи у всьому файлі (на відміну від strip_emojis_from_descriptions /
        strip_discount_reason_from_descriptions, які схлопуюють пробіли лише в межах
        вже витягнутого <description>).

    Args:
        xml: повний XML-рядок фіду.

    Returns:
        XML з видаленими external URLs з текстових полів.
    """
    result, n1 = _DETAILS_LINK_RE.subn('', xml)
    result, n2 = _BARE_URL_RE.subn('', result)
    total = n1 + n2
    if total:
        print(
            f"🔗 strip_external_links: видалено {total} посилань "
            f"({n1} 'Детальніше', {n2} bare URL)"
        )
    return result


def strip_html_classes(xml: str) -> str:
    """
    Видаляє HTML class-атрибути з описів.

    Prom-фід іноді містить HTML з Google Translate-розміткою (напр. class="Y2IQFc").
    Епіцентр такі атрибути не потребує; видаляємо глобально по всьому XML.

    Args:
        xml: повний XML-рядок фіду.

    Returns:
        XML з видаленими class-атрибутами.
    """
    cleaned, count = _HTML_CLASS_RE.subn('', xml)
    if count:
        print(f"🧹 strip_html_classes: видалено {count} class-атрибутів")
    return cleaned


def strip_emojis_from_descriptions(xml: str) -> str:
    """
    Видаляє емодзі з <description lang="ua">...</description> (CDATA-safe).

    Після видалення схлопує зайві пробіли, що могли лишитись на місці емодзі.
    CDATA-обгортка (якщо була) зберігається.

    Args:
        xml: повний XML-рядок фіду.

    Returns:
        XML з видаленими емодзі з описів.
    """
    changed = 0

    def _on_description(m: re.Match[str]) -> str:
        nonlocal changed
        open_tag, raw, close_tag = m.group(1), m.group(2), m.group(3)
        inner, was_cdata = _unwrap_cdata(raw)
        if not inner:
            return m.group(0)
        cleaned = _EMOJI_RE.sub('', inner)
        cleaned = _MULTI_SPACE_RE.sub(' ', cleaned).strip()
        if cleaned == inner:
            return m.group(0)
        changed += 1
        return f'{open_tag}{_rewrap_cdata(cleaned, was_cdata)}{close_tag}'

    result = _DESCRIPTION_TAG_RE.sub(_on_description, xml)
    if changed:
        print(f"😀 Epicenter емодзі: видалено з {changed} описів")
    else:
        print("😀 Epicenter емодзі: збігів не знайдено")
    return result


def strip_sale_labels_from_names(xml: str) -> str:
    """
    Видаляє службовий префікс "Розпродаж (ID)" / "Распродажа (ID)" (з ID або
    без нього) з початку <name lang="ua">...</name> і додає до залишку назви маркер
    " s" (від sale), щоб sale-товар було візуально відрізняти вже після видалення
    префікса (товар все одно можна знайти за артикулом — offer id не міняється).

    Приклад:
        "Розпродаж (4160) Акумулятор AGM LPM 6V - 14 Ah 09/23"
        → "Акумулятор AGM LPM 6V - 14 Ah 09/23 s"

    Ідемпотентність: другий прогін на вже очищеній назві нічого не змінює — префікс вже
    видалено, тому регекс більше не збігається і суфікс вдруге не додається.

    Args:
        xml: повний XML-рядок фіду.

    Returns:
        XML з очищеними назвами та доданим маркером sale-товарів.
    """
    changed = 0

    def _on_name(m: re.Match[str]) -> str:
        nonlocal changed
        open_tag, inner, close_tag = m.group(1), m.group(2), m.group(3)
        new_inner, matched = _SALE_LABEL_RE.subn('', inner, count=1)
        new_inner = new_inner.strip()
        if matched and new_inner and not new_inner.endswith(_SALE_SUFFIX):
            new_inner = f'{new_inner}{_SALE_SUFFIX}'
        if new_inner == inner:
            return m.group(0)
        changed += 1
        return f'{open_tag}{new_inner}{close_tag}'

    result = _NAME_TAG_RE.sub(_on_name, xml)
    if changed:
        print(f"🏷️  Epicenter sale-мітки: видалено з {changed} назв (Розпродаж/Распродажа)")
    else:
        print("🏷️  Epicenter sale-мітки: збігів не знайдено")
    return result


def strip_discount_reason_from_descriptions(xml: str) -> str:
    """
    Видаляє службове речення "Причина уцінки/уценки, ...." (повністю, до першої
    крапки) з <description lang="ua">...</description>, а також будь-які самостійні згадки слова
    "уцінка"/"уценка", що лишились поза цим реченням. CDATA-safe.

    Приклад:
        "Причина уцінки, акумулятор 2024 року. Зовнішня універсальна АКБ..."
        → "Зовнішня універсальна АКБ..."

    Args:
        xml: повний XML-рядок фіду.

    Returns:
        XML з видаленою причиною уцінки з описів.
    """
    changed = 0

    def _on_description(m: re.Match[str]) -> str:
        nonlocal changed
        open_tag, raw, close_tag = m.group(1), m.group(2), m.group(3)
        inner, was_cdata = _unwrap_cdata(raw)
        if not inner:
            return m.group(0)
        cleaned = _DISCOUNT_REASON_SENTENCE_RE.sub('', inner)
        cleaned = _DISCOUNT_WORD_RE.sub('', cleaned)
        cleaned = _MULTI_SPACE_RE.sub(' ', cleaned).strip()
        cleaned = _LEADING_PUNCT_RE.sub('', cleaned).strip()
        if cleaned == inner:
            return m.group(0)
        changed += 1
        return f'{open_tag}{_rewrap_cdata(cleaned, was_cdata)}{close_tag}'

    result = _DESCRIPTION_TAG_RE.sub(_on_description, xml)
    if changed:
        print(f"🏷️  Epicenter причина уцінки: видалено з {changed} описів")
    else:
        print("🏷️  Epicenter причина уцінки: збігів не знайдено")
    return result
