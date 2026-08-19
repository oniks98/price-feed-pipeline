"""
services/rozetka_dimensions_service.py
-----------------------------------------
Заповнює Rozetka-обов'язкові параметри розмірів/ваги пакування для кожного
офера в Rozetka-фіді:

    Ширина в упаковці, Глибина в упаковці, Висота в упаковці, Вага в упаковці

Джерело даних — інші характеристики ТОГО Ж САМОГО офера (той самий XML,
завантажений з FEED_URL_PROM): жодного окремого HTTP-запиту не потрібно.
Кожен офер у фіді вже несе "сирі" характеристики товару (розміри, вагу
тощо, під різними назвами залежно від постачальника/категорії) — цей
сервіс читає їх і переносить у форматі, якого очікує Rozetka.

Пріоритет визначення ЛІНІЙНИХ розмірів (перше, що вдалось розпарсити —
перемагає; тіри перевіряються по черзі, без змішування даних різних тірів):

    1. «Розмір упаковки (Ш х В х Г)» / «Размер упаковки (...)»
       — композитне поле, явний порядок осей закодовано в самій назві.
    2. «Габаритні розміри (ДхШхВ)» / «Габаритные размеры (...)»
       — так само явний порядок, але інша типова послідовність літер.
    3. «Розміри» / «Розмір» / «Размір» / «Размер» / «Размеры»
       — композитне, порядок у назві НЕ вказано → застосовується порядок
       Ш-В-Г за замовчуванням (як у Rozetka-нативному пункті 1). Якщо
       3-числове значення не розпарсилось — пробуємо циліндричний запис
       «Ø D × H» / «H × Ø D» (в будь-якому порядку): число з позначкою
       Ø/Ф — завжди діаметр (Ш = Г = діаметр), число без позначки —
       завжди висота.
    4. Індивідуальні «Висота» / «Ширина» / «Довжина» (або «Глибина», якщо
       є — вона пріоритетніша за «Довжина») — БЕРУТЬСЯ ЛИШЕ РАЗОМ, усі
       три одразу; часткові дані з цього тіру не використовуються, щоб
       не змішати вимір із різних, потенційно неузгоджених джерел.

    Порядок осей «Довжина» → глибина підтверджено емпірично: на товарах,
    де присутнє і композитне «Розмір упаковки (Ш х В х Г)», і індивідуальні
    Висота/Довжина/Ширина, третє число композита завжди дорівнює «Довжина».

Вага — окремий, незалежний від розмірів пріоритет:
    1. «Вага брутто» / «Вес брутто» / «Вага упаковки» / «Вес упаковки»
    2. «Вага» / «Вес» — вага самого товару (fallback, якщо брутто немає).

Одиниці конвертуються автоматично в см / кг. Одиниця для кожного значення
береться в такому порядку: атрибут unit="...", інакше суфікс у самій назві
параметра (напр. «Довжина, мм», «Вес (кг)»), інакше — текст самого значення
(напр. «135 мм»); якщо одиницю визначити не вдалось — типове мм / г
(найпоширеніші одиниці цих полів у фіді).

Якщо джерело не знайдено — відповідний target-параметр ПРОСТО НЕ
ДОДАЄТЬСЯ (розміри й вага трактуються незалежно: може бути додано лише
розміри без ваги, лише вага без розмірів, або нічого). Rozetka не банить
товар за відсутність цих параметрів, а от за примусовий «0 (см)»/«0 кг» —
банить (перевірено на практиці), тому відсутність поля свідомо кращий
варіант, ніж неправдиве нульове значення.

Функція ІДЕМПОТЕНТНА: якщо target-параметр вже присутній в офері (напр.
повторний запуск на вже збагаченому файлі) — його значення оновлюється
на місці, а не дублюється новим тегом. Якщо джерела немає — існуючий тег
не чіпається (сервіс лише додає/оновлює, ніколи не видаляє й не обнуляє).

Використання в generate_rozetka_feed.py (будь-де після завантаження XML;
незалежний від ціноутворення/категорій/бренду — читає й пише лише
параметри всередині одного й того ж офера):

    from services.rozetka_dimensions_service import apply_package_dimensions
    updated_xml = apply_package_dimensions(updated_xml)
"""

from __future__ import annotations

import re
from collections import Counter
from dataclasses import dataclass
from decimal import ROUND_HALF_UP, Decimal, InvalidOperation
from typing import Final

# ---------------------------------------------------------------------------
# Target-параметри (те, що ми заповнюємо) — фіксований порядок для
# детермінованого виводу.
# ---------------------------------------------------------------------------

_TARGET_WIDTH: Final[str] = "Ширина в упаковці"
_TARGET_DEPTH: Final[str] = "Глибина в упаковці"
_TARGET_HEIGHT: Final[str] = "Висота в упаковці"
_TARGET_WEIGHT: Final[str] = "Вага в упаковці"
_TARGET_PARAM_NAMES: Final[tuple[str, str, str, str]] = (
    _TARGET_WIDTH,
    _TARGET_DEPTH,
    _TARGET_HEIGHT,
    _TARGET_WEIGHT,
)

# ---------------------------------------------------------------------------
# Джерельні поля — конфігурація пріоритетів (звідси читаємо).
# Ключі множин порівнюються з НОРМАЛІЗОВАНОЮ (casefold, без суфікса одиниці
# й без дужок-порядку) базовою назвою параметра — див. _split_name().
# ---------------------------------------------------------------------------

# Композитні (3-в-1) поля розмірів, перевіряються в цьому порядку;
# перше поле, що знайшлось І успішно розпарсилось — перемагає.
# Кортеж: (мітка тіра для логів, набір базових назв, порядок осей за
# замовчуванням — якщо явний порядок не закодовано в самій назві дужками).
_COMPOSITE_SIZE_TIERS: Final[tuple[tuple[str, frozenset[str], tuple[str, str, str]], ...]] = (
    (
        "розмір упаковки",
        frozenset({"розмір упаковки", "размір упаковки", "размер упаковки"}),
        ("Ш", "В", "Г"),
    ),
    (
        "габаритні розміри",
        frozenset({"габаритні розміри", "габаритные размеры", "габаритні розміри товару"}),
        ("Д", "Ш", "В"),
    ),
    (
        "розміри",
        frozenset({"розміри", "розмір", "размір", "размер", "размеры"}),
        ("Ш", "В", "Г"),
    ),
)

_WIDTH_FIELDS: Final[frozenset[str]] = frozenset({"ширина"})
_HEIGHT_FIELDS: Final[frozenset[str]] = frozenset({"висота", "высота"})
# «Глибина/Глубина» — пряма семантика глибини, пріоритетніша за «Довжина/Длина»
# (довжина емпірично підтверджена як еквівалент глибини для пакування —
# див. докстрінг модуля — але коли є прямий вимір глибини, довіряємо йому більше).
_DEPTH_FIELD_TIERS: Final[tuple[frozenset[str], ...]] = (
    frozenset({"глибина", "глубина"}),
    frozenset({"довжина", "длина"}),
)

_WEIGHT_FIELD_TIERS: Final[tuple[frozenset[str], ...]] = (
    frozenset({"вага брутто", "вес брутто", "вага упаковки", "вес упаковки", "вага товару брутто"}),
    frozenset({"вага", "вес"}),
)

# Літера в назві (у дужках-порядку типу "(Ш х В х Г)") → роль осі.
# «Г» (Глибина) і «Д» (Довжина) обидві мапляться на depth — узгоджено
# з _DEPTH_FIELD_TIERS вище: обидва варіанти семантично відповідають
# "третьому вимірюванню" пакування.
_AXIS_LETTER_TO_ROLE: Final[dict[str, str]] = {
    "Ш": "width",
    "В": "height",
    "Г": "depth",
    "Д": "depth",
}

_LENGTH_UNIT_TO_CM: Final[dict[str, Decimal]] = {
    "мм": Decimal("0.1"),
    "см": Decimal("1"),
    "дм": Decimal("10"),
    "м": Decimal("100"),
}
_WEIGHT_UNIT_TO_KG: Final[dict[str, Decimal]] = {
    "г": Decimal("0.001"),
    "кг": Decimal("1"),
}
# Найпоширеніші одиниці цих полів у фіді — застосовуються, якщо одиницю
# не вдалось визначити з атрибута unit, назви параметра чи тексту значення.
_DEFAULT_LENGTH_UNIT: Final[str] = "мм"
_DEFAULT_WEIGHT_UNIT: Final[str] = "г"

# ---------------------------------------------------------------------------
# Регулярні вирази — компілюються один раз на рівні модуля.
# ---------------------------------------------------------------------------

_OFFER_RE: Final[re.Pattern[str]] = re.compile(
    r'<offer\s+id="(\d+)"([^>]*)>(.*?)</offer>', re.DOTALL
)
# Припущення (як і в інших services/*): name="..." — перший атрибут тегу.
# Так Prom завжди й віддає <param> у цьому фіді (перевірено емпірично).
_PARAM_RE: Final[re.Pattern[str]] = re.compile(
    r'<param\s+name="([^"]+)"([^>]*)>(.*?)</param>', re.DOTALL
)
_UNIT_ATTR_RE: Final[re.Pattern[str]] = re.compile(r'\bunit="([^"]*)"')
_CDATA_RE: Final[re.Pattern[str]] = re.compile(r'<!\[CDATA\[(.*?)\]\]>', re.DOTALL)

# Порядок осей, закодований у назві дужками: "(Ш х В х Г)", "(ДхШхВ)", "(д/ш/в)".
_AXIS_ORDER_HINT_RE: Final[re.Pattern[str]] = re.compile(
    r'[\(\[]\s*([шШвВгГдД])\s*[xх×/]\s*([шШвВгГдД])\s*[xх×/]\s*([шШвВгГдД])\s*[\)\]]'
)
# Суфікс одиниці виміру в самій назві параметра: ", мм", "(кг)", ", мм **" тощо.
_TRAILING_UNIT_HINT_RE: Final[re.Pattern[str]] = re.compile(
    r'[,\s]+[\(\[]?(мм|см|дм|м|кг|г)[\)\]]?\s*\**\s*$', re.IGNORECASE
)

_NUM: Final[str] = r"\d+(?:[.,]\d+)?"
_SEP: Final[str] = r"[xх×*]"
_LENGTH_UNIT_WORD: Final[str] = r"(мм|см|дм|м)"
_WEIGHT_UNIT_WORD: Final[str] = r"(кг|г)"

_TRIPLE_RE: Final[re.Pattern[str]] = re.compile(
    rf"({_NUM})\s*{_SEP}\s*({_NUM})\s*{_SEP}\s*({_NUM})\s*{_LENGTH_UNIT_WORD}?", re.IGNORECASE
)
# Ø/Ф завжди позначає діаметр; число без позначки — завжди висота.
# Порядок у тексті трапляється обидва варіанти: «Ø D × H» і «H × Ø D».
_DIAMETER_FIRST_RE: Final[re.Pattern[str]] = re.compile(
    rf"[ØøΦφФф]\s*({_NUM})\s*{_SEP}\s*({_NUM})\s*{_LENGTH_UNIT_WORD}?", re.IGNORECASE
)
_DIAMETER_SECOND_RE: Final[re.Pattern[str]] = re.compile(
    rf"({_NUM})\s*{_SEP}\s*[ØøΦφФф]\s*({_NUM})\s*{_LENGTH_UNIT_WORD}?", re.IGNORECASE
)
_SINGLE_LENGTH_RE: Final[re.Pattern[str]] = re.compile(
    rf"({_NUM})\s*{_LENGTH_UNIT_WORD}?", re.IGNORECASE
)
_SINGLE_WEIGHT_RE: Final[re.Pattern[str]] = re.compile(
    rf"({_NUM})\s*{_WEIGHT_UNIT_WORD}?", re.IGNORECASE
)


# ---------------------------------------------------------------------------
# Дані
# ---------------------------------------------------------------------------

@dataclass(frozen=True, slots=True)
class _ParamEntry:
    name: str
    unit_attr: str
    value: str


@dataclass(frozen=True, slots=True)
class _LinearResult:
    width_cm: Decimal
    height_cm: Decimal
    depth_cm: Decimal
    source: str  # мітка тіра — лише для підсумкового логування


# ---------------------------------------------------------------------------
# Парсинг тексту: назва параметра, числа, одиниці
# ---------------------------------------------------------------------------

def _strip_cdata(value: str) -> str:
    m = _CDATA_RE.match(value.strip())
    return m.group(1).strip() if m else value.strip()


def _normalize(text: str) -> str:
    return re.sub(r"\s+", " ", text).strip().casefold()


def _split_name(raw_name: str) -> tuple[str, str | None, tuple[str, str, str] | None]:
    """
    Розбирає сиру назву параметра на:
        base            — нормалізована базова назва (без суфікса одиниці
                          й без дужок-порядку осей), готова для порівняння
                          з _COMPOSITE_SIZE_TIERS / _WIDTH_FIELDS / тощо.
        unit_hint       — одиниця виміру, знайдена в самій назві, або None.
        axis_order_hint — кортеж літер (Ш/В/Г/Д), якщо назва містить явний
                          порядок осей у дужках, інакше None.
    """
    name = raw_name.strip()

    axis_hint: tuple[str, str, str] | None = None
    m = _AXIS_ORDER_HINT_RE.search(name)
    if m:
        axis_hint = (m.group(1).upper(), m.group(2).upper(), m.group(3).upper())
        name = name[: m.start()] + name[m.end():]

    unit_hint: str | None = None
    m = _TRAILING_UNIT_HINT_RE.search(name)
    if m:
        unit_hint = m.group(1).casefold()
        name = name[: m.start()]

    base = re.sub(r"[,\*\s]+$", "", name)
    return _normalize(base), unit_hint, axis_hint


def _to_decimal(raw: str) -> Decimal:
    return Decimal(raw.replace(",", "."))


def _length_to_cm(value: Decimal, unit: str | None) -> Decimal:
    factor = _LENGTH_UNIT_TO_CM.get(
        (unit or _DEFAULT_LENGTH_UNIT).casefold(), _LENGTH_UNIT_TO_CM[_DEFAULT_LENGTH_UNIT]
    )
    return value * factor


def _weight_to_kg(value: Decimal, unit: str | None) -> Decimal:
    factor = _WEIGHT_UNIT_TO_KG.get(
        (unit or _DEFAULT_WEIGHT_UNIT).casefold(), _WEIGHT_UNIT_TO_KG[_DEFAULT_WEIGHT_UNIT]
    )
    return value * factor


def _parse_triple_cm(raw_value: str, unit_hint: str | None) -> tuple[Decimal, Decimal, Decimal] | None:
    """Розпарсює «105 x 100 x 195 мм» / «160×110×430» тощо → (a, b, c) у см, у порядку появи в тексті."""
    m = _TRIPLE_RE.search(raw_value)
    if not m:
        return None
    unit = m.group(4) or unit_hint
    return (
        _length_to_cm(_to_decimal(m.group(1)), unit),
        _length_to_cm(_to_decimal(m.group(2)), unit),
        _length_to_cm(_to_decimal(m.group(3)), unit),
    )


def _parse_diameter_as_triple_cm(raw_value: str, unit_hint: str | None) -> tuple[Decimal, Decimal, Decimal] | None:
    """
    Розпізнає циліндричний запис «Ø D × H» / «H × Ø D» (в будь-якому
    порядку) — типово для куполових/циліндричних камер, датчиків тощо.
    Число з позначкою Ø/Ф — завжди діаметр, число без позначки — завжди
    висота. Апроксимує пакування квадратом у плані: Ш = Г = діаметр.
    Повертає (Ш, В, Г) — сумісний порядок з рештою композитних полів.
    """
    m = _DIAMETER_FIRST_RE.search(raw_value)
    if m:
        unit = m.group(3) or unit_hint
        diameter = _length_to_cm(_to_decimal(m.group(1)), unit)
        height = _length_to_cm(_to_decimal(m.group(2)), unit)
        return diameter, height, diameter

    m = _DIAMETER_SECOND_RE.search(raw_value)
    if m:
        unit = m.group(3) or unit_hint
        height = _length_to_cm(_to_decimal(m.group(1)), unit)
        diameter = _length_to_cm(_to_decimal(m.group(2)), unit)
        return diameter, height, diameter

    return None


def _parse_single_length(raw_value: str, unit_hint: str | None) -> Decimal | None:
    m = _SINGLE_LENGTH_RE.search(raw_value)
    if not m:
        return None
    return _length_to_cm(_to_decimal(m.group(1)), m.group(2) or unit_hint)


def _parse_single_weight(raw_value: str, unit_hint: str | None) -> Decimal | None:
    m = _SINGLE_WEIGHT_RE.search(raw_value)
    if not m:
        return None
    return _weight_to_kg(_to_decimal(m.group(1)), m.group(2) or unit_hint)


def _assign_axes(
    values_cm: tuple[Decimal, Decimal, Decimal], order: tuple[str, str, str]
) -> tuple[Decimal, Decimal, Decimal] | None:
    """(значення в порядку тексту, порядок осей за літерами) → (width, height, depth) у см."""
    roles = [_AXIS_LETTER_TO_ROLE.get(letter) for letter in order]
    if None in roles or len(set(roles)) != 3:
        return None  # непізнаний/неоднозначний порядок — безпечно відмовляємось, а не вгадуємо
    by_role = dict(zip(roles, values_cm))
    return by_role["width"], by_role["height"], by_role["depth"]


# ---------------------------------------------------------------------------
# Резолюція розмірів/ваги одного офера
# ---------------------------------------------------------------------------

def _extract_params(body: str) -> list[_ParamEntry]:
    entries: list[_ParamEntry] = []
    for m in _PARAM_RE.finditer(body):
        attrs = m.group(2)
        unit_match = _UNIT_ATTR_RE.search(attrs)
        entries.append(
            _ParamEntry(
                name=m.group(1).strip(),
                unit_attr=unit_match.group(1).strip() if unit_match else "",
                value=_strip_cdata(m.group(3)),
            )
        )
    return entries


def _find_single_length(entries: list[_ParamEntry], names: frozenset[str]) -> Decimal | None:
    for entry in entries:
        base, name_unit_hint, _ = _split_name(entry.name)
        if base in names:
            value = _parse_single_length(entry.value, entry.unit_attr or name_unit_hint)
            if value is not None:
                return value
    return None


def _find_single_length_by_tier(
    entries: list[_ParamEntry], tiers: tuple[frozenset[str], ...]
) -> Decimal | None:
    for names in tiers:
        value = _find_single_length(entries, names)
        if value is not None:
            return value
    return None


def _resolve_linear(entries: list[_ParamEntry]) -> _LinearResult | None:
    for tier_label, names, default_order in _COMPOSITE_SIZE_TIERS:
        for entry in entries:
            base, name_unit_hint, axis_hint = _split_name(entry.name)
            if base not in names:
                continue
            unit_hint = entry.unit_attr or name_unit_hint

            triple = _parse_triple_cm(entry.value, unit_hint)
            if triple is not None:
                axes = _assign_axes(triple, axis_hint or default_order)
                if axes is not None:
                    width, height, depth = axes
                    return _LinearResult(width, height, depth, tier_label)

            if tier_label == "розміри":
                diameter_triple = _parse_diameter_as_triple_cm(entry.value, unit_hint)
                if diameter_triple is not None:
                    width, height, depth = diameter_triple
                    return _LinearResult(width, height, depth, "розміри (Ø)")

    width = _find_single_length(entries, _WIDTH_FIELDS)
    height = _find_single_length(entries, _HEIGHT_FIELDS)
    depth = _find_single_length_by_tier(entries, _DEPTH_FIELD_TIERS)
    if width is not None and height is not None and depth is not None:
        return _LinearResult(width, height, depth, "індивідуальні поля")
    return None


def _resolve_weight(entries: list[_ParamEntry]) -> tuple[Decimal, str] | None:
    for names in _WEIGHT_FIELD_TIERS:
        for entry in entries:
            base, name_unit_hint, _ = _split_name(entry.name)
            if base in names:
                value = _parse_single_weight(entry.value, entry.unit_attr or name_unit_hint)
                if value is not None:
                    label = "вага брутто" if names is _WEIGHT_FIELD_TIERS[0] else "вага"
                    return value, label
    return None


# ---------------------------------------------------------------------------
# Форматування виводу
# ---------------------------------------------------------------------------

def _trim_decimal(value: Decimal) -> str:
    """Фіксований формат без експоненти + обрізка зайвих нулів (Decimal.normalize()
    для круглих чисел на кшталт 100 повертає наукову нотацію "1E+2" — тому вручну)."""
    text = format(value, "f")
    if "." in text:
        text = text.rstrip("0").rstrip(".")
    return text or "0"


def _format_cm(value: Decimal) -> str:
    quantized = value.quantize(Decimal("0.1"), rounding=ROUND_HALF_UP)
    return f"{_trim_decimal(quantized)} (см)"


def _format_kg(value: Decimal) -> str:
    quantized = value.quantize(Decimal("0.001"), rounding=ROUND_HALF_UP)
    return f"{_trim_decimal(quantized)} кг"


# ---------------------------------------------------------------------------
# Застосування до одного офера / усього фіду
# ---------------------------------------------------------------------------

_EXISTING_TARGET_RES: Final[dict[str, re.Pattern[str]]] = {
    name: re.compile(rf'(<param\s+name="{re.escape(name)}"[^>]*>)(.*?)(</param>)', re.DOTALL)
    for name in _TARGET_PARAM_NAMES
}


def _apply_targets(body: str, values: dict[str, str]) -> str:
    """
    Оновлює на місці ті target-параметри, для яких у values є значення;
    відсутні в body — дописує в кінець. Параметри без ключа в values НЕ
    чіпаються взагалі (нічого не додається й не обнуляється) — див.
    докстрінг модуля щодо того, чому «немає поля» кращий за «0».
    """
    to_append: list[str] = []
    for name in _TARGET_PARAM_NAMES:
        if name not in values:
            continue
        value = values[name]
        match = _EXISTING_TARGET_RES[name].search(body)
        if match:
            body = f"{body[:match.start()]}{match.group(1)}{value}{match.group(3)}{body[match.end():]}"
        else:
            to_append.append(f'    <param name="{name}">{value}</param>')
    if to_append:
        body = body.rstrip("\n") + "\n" + "\n".join(to_append) + "\n"
    return body


def apply_package_dimensions(xml: str) -> str:
    """
    Заповнює параметри пакування (див. докстрінг модуля) для кожного офера.

    Args:
        xml: Повний XML-фід (той самий, що вже містить джерельні характеристики
             товару — окремого завантаження не потрібно).

    Returns:
        Оновлений XML з заповненими Ширина/Глибина/Висота/Вага в упаковці
        у кожному <offer> (лише там, де знайдено джерело — див. докстрінг).
    """
    size_sources: Counter[str] = Counter()
    weight_sources: Counter[str] = Counter()
    offers_total = 0
    enriched = 0
    error_offer_ids: list[str] = []

    def _on_offer(m: re.Match[str]) -> str:
        nonlocal offers_total, enriched
        offers_total += 1
        offer_id, tail_attrs, body = m.group(1), m.group(2), m.group(3)

        linear: _LinearResult | None = None
        weight: tuple[Decimal, str] | None = None
        try:
            entries = _extract_params(body)
            linear = _resolve_linear(entries)
            weight = _resolve_weight(entries)
        except (InvalidOperation, ValueError):
            error_offer_ids.append(offer_id)

        if linear is not None:
            size_sources[linear.source] += 1
        if weight is not None:
            weight_sources[weight[1]] += 1
        if linear is not None or weight is not None:
            enriched += 1

        values: dict[str, str] = {}
        if linear is not None:
            values[_TARGET_WIDTH] = _format_cm(linear.width_cm)
            values[_TARGET_DEPTH] = _format_cm(linear.depth_cm)
            values[_TARGET_HEIGHT] = _format_cm(linear.height_cm)
        if weight is not None:
            values[_TARGET_WEIGHT] = _format_kg(weight[0])

        new_body = _apply_targets(body, values) if values else body
        return f'<offer id="{offer_id}"{tail_attrs}>{new_body}</offer>'

    result = _OFFER_RE.sub(_on_offer, xml)

    size_summary = ", ".join(f"{label} ({count})" for label, count in size_sources.most_common()) or "—"
    weight_summary = ", ".join(f"{label} ({count})" for label, count in weight_sources.most_common()) or "—"
    print(
        f"📦 Rozetka розміри пакування: {enriched}/{offers_total} офферів збагачено "
        f"| розміри: {size_summary} | вага: {weight_summary}"
    )
    if error_offer_ids:
        preview = ", ".join(error_offer_ids[:10])
        more = f" (+{len(error_offer_ids) - 10})" if len(error_offer_ids) > 10 else ""
        print(f"⚠️  Rozetka розміри пакування: помилка парсингу в {len(error_offer_ids)} офферах: {preview}{more}")

    return result
