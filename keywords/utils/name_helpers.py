"""
Утиліти для витягування інформації з назв товарів.
"""

import re
from typing import Optional, Dict, List, Set

from keywords.constants import CAMERA_TECHNOLOGIES


# ─────────────────────────────────────────────────────────────────────────────
# Стоп-слова: загальні категорійні слова, що не є моделлю
# ─────────────────────────────────────────────────────────────────────────────
_GENERIC_STOPWORDS: frozenset[str] = frozenset({
    # домофони / камери
    "видеодомофон", "відеодомофон", "домофон",
    "камера", "відеокамера", "видеокамера",
    # комплекти / набори
    "комплект", "набор", "набір",
    # "kit" видалено навмисно: є частиною моделей (KAPPA HD KIT)
    # панелі
    "панель", "вызывная", "виклична",
    # оптика
    "монокуляр", "прицел", "приціл", "тепловизионный",
    # акумулятори
    "аккумулятор", "акумулятор", "батарея",
    # радіо
    "радиостанция", "радіостанція", "radio", "portable",
    # мережа
    "антенна", "антена", "коммутатор", "комутатор",
    "маршрутизатор", "медиаконвертор", "медіаконвертор",
    # кабелі / живлення
    "кабель", "инвертор", "інвертор",
    "источник", "джерело", "блок",
    # корпуси
    "ящик", "шкаф", "шафа",
    "квадрокоптер", "дрон",
    # прийменники / службові
    "для", "з", "із", "с", "and", "the", "for", "with",
})

# Префікси стандартів — не є кодами моделей (зліва від першого дефісу)
_SPEC_CODE_PREFIXES: frozenset[str] = frozenset({
    "rs", "rj", "ik", "wi", "pe", "ac", "dc",
})

# Мінімальна довжина алфавітно-цифрового коду без дефісу
_MIN_ALNUM_LEN: int = 3

# ── Шаблон 1: дефісний код (латиниця; LETTER-START обов'язковий) ─────────────
# Приклади: SQ-04, Wolf-14, S6-EH3P12K02-NV-YD-L, PV18-4024ECO, MultiPlus-II
# NOTE: \b не розрізняє ASCII/кирилицю в Python 3 Unicode — замінено на ASCII-lookahead.
# (?:/\d+)? дозволяє опціональний числовий суфікс: BBG-124/1
_HYPHEN_LATIN_RE = re.compile(
    r"(?<![A-Za-z0-9])[A-Za-z][A-Za-z0-9]{0,15}(?:[-+][A-Za-z0-9]+){1,}(?:/\d+)?(?![A-Za-z0-9])"
)

# ── Шаблон 1b: дефісний код із кирилицею (LETTER або DIGIT-start дозволено) ──
# Приклади: 902С-А, БК-165-1-пенал
_HYPHEN_CYRILLIC_RE = re.compile(
    r"\b[0-9А-ЯҐЄІЇа-яґєії][А-ЯҐЄІЇа-яґєіїA-Za-z0-9]{0,15}"
    r"(?:-[А-ЯҐЄІЇа-яґєіїA-Za-z0-9]+){1,}\b"
)

# ── Шаблон 2: алфавітно-цифровий код без дефісу (Latin, uppercase-start) ─────
# Приклади: TSW200, RUT301, PMNN4808A, S414, X5S, R7a
_ALNUM_MODEL_RE = re.compile(
    r"\b[A-Z][A-Za-z0-9]*\d[A-Za-z0-9]*\b"
)


# ─────────────────────────────────────────────────────────────────────────────
# Публічні функції
# ─────────────────────────────────────────────────────────────────────────────

# Мінімальна довжина ключа для пошуку підрядком (без word boundary).
# Короткі ключі (менше або рівно) перевіряються тільки як окреме слово.
_BRAND_WORD_BOUNDARY_MAX_LEN: int = 3


def extract_brand(text: str, manufacturers: Dict[str, str]) -> Optional[str]:
    """Витягування бренду з назви товару.

    Ключі перебіраються від довших до коротших (dict вже відсортовано в load_manufacturers).
    Короткі ключі (≤ _BRAND_WORD_BOUNDARY_MAX_LEN) перевіряються як окреме слово,
    щоб "ua" не знаходилось всередині "uniview".
    """
    text_lower = text.lower()
    for keyword, manufacturer in manufacturers.items():
        if len(keyword) <= _BRAND_WORD_BOUNDARY_MAX_LEN:
            if re.search(r'(?<![A-Za-z0-9])' + re.escape(keyword) + r'(?![A-Za-z0-9])', text_lower):
                return manufacturer
        else:
            if keyword in text_lower:
                return manufacturer
    return None


def extract_model(text: str, brand: Optional[str] = None) -> Optional[str]:
    """
    Витягування моделі/SKU з назви товару.

    Пріоритети:
      1. Накопичувачі (HDD / SD) — окрема логіка.
      2. Ajax — спеціальна евристика.
      2.5. Кабельний стандарт із дужками: J-Y(ST)-Y.
      3. Дефісні SKU (латиниця, потім кирилиця).
      4. Алфавітно-цифровий SKU без дефісу.
      5. Числові комбінації (MGA 108-550, MultiPlus-II 48/15000/200-100).
      6. Послідовність UPPERCASE-слів (KAPPA HD KIT).
      6.5. Кирилічне позначення кабелю + бренд → бренд як ідентифікатор.
      7. Консервативний fallback.

    NOTE: пошук ведеться в оригінальному тексті (включно зі скобками),
    щоб не втратити моделі типу CRS305-1G-4S, що записані як "(CRS305-1G-4S+OUT)".
    """
    if _is_storage_device(text):
        return _extract_storage_model(text)

    # Видаляємо бренд для пошуку, але зберігаємо скобки — там може бути модель
    search_text = _remove_brand(text, brand)

    # ── 2. Ajax-специфічне витягування ────────────────────────────────────────
    ajax_match = re.search(
        r"\bAjax\s+([A-Z][A-Za-z0-9]+(?:\s+[A-Z][A-Za-z0-9]+)?)",
        search_text,
        re.IGNORECASE,
    )
    if ajax_match:
        return ajax_match.group(1)

    # ── 2.5. Кабельний стандарт із внутрішніми дужками: J-Y(ST)-Y ────────────
    cable_std_match = re.search(
        r"(?<![A-Za-z0-9])[A-Za-z][A-Za-z0-9]*(?:-[A-Za-z0-9]+)*\([A-Za-z]+\)(?:-[A-Za-z0-9]+)+",
        search_text,
        re.IGNORECASE,
    )
    if cable_std_match:
        return cable_std_match.group(0)

    # Коротка абревіатура-модель типу EAP (перед спекою DUAL-1000ВА) — обчислюємо заздалегідь
    early_acr = _early_acronym_candidate(search_text)

    # ── 3a. Дефісний SKU — латиниця (letter-start); шукаємо в ОРИГІНАЛІ зі скобками ──
    lat_candidates = _find_hyphen_latin_skus(search_text)
    best = _pick_best_sku(lat_candidates)
    if best:
        # Якщо знайдений дефісний токен — спека (DUAL-1000ВА), а є коротка модель (EAP)
        if early_acr and _looks_like_power_or_spec(best):
            return early_acr
        return best

    # ── 3b. Дефісний SKU — кирилиця (наприклад 902С-А, БК-165-1) ─────────────
    cyr_candidates = _find_hyphen_cyrillic_skus(search_text)
    best = _pick_best_sku(cyr_candidates)
    if best:
        return best

    # ── 4. Алфавітно-цифровий SKU без дефісу ─────────────────────────────────
    # Пошук у тексті без скобок щоб не ловити розміри "(250x165x70)"
    clean_text = _strip_parentheses(search_text)
    alnum_candidates = _find_alnum_skus(clean_text, brand=brand)
    best = _pick_best_sku(alnum_candidates)
    if best:
        # Коротке слово (≤ 2 символи, напр. V2) доповнюємо попереднім словом
        return _try_combine_with_prev_word(clean_text, best, brand=brand) or best

    # ── 5. Числові комбінації ──────────────────────────────────────────────────
    combined = _try_extract_numeric_combo(clean_text)
    if combined:
        return combined

    # ── 6. Послідовність UPPERCASE-слів ───────────────────────────────────────
    upper_seq = _extract_uppercase_sequence(clean_text)
    if upper_seq:
        return upper_seq

    # ── 6.5. Кирилічне позначення кабелю без цифр + є бренд → бренд як ідентифікатор ──
    # Напр.: "OK-Net КПВ-ВП" — немає окремого SKU, оптимальний пошуковий запит — сам бренд
    if brand and _has_cyrillic_cable_designation(clean_text):
        return brand

    # ── 7. Fallback ────────────────────────────────────────────────────────────
    return _fallback_model_from_words(clean_text)


def extract_technology(text: str) -> Optional[str]:
    """Витягування технології камери з назви."""
    text_lower = text.lower()
    for tech in CAMERA_TECHNOLOGIES:
        if tech in text_lower:
            return tech.upper()
    return None


def check_wifi(name: str, specs: list) -> bool:
    """Перевірка наявності WiFi у назві або характеристиках."""
    if re.search(r"wi[- ]?fi", name, re.I):
        return True
    for spec in specs:
        spec_text = f"{spec.get('name', '')} {spec.get('value', '')}"
        if re.search(r"wi[- ]?fi", spec_text, re.I):
            return True
    return False


# ─────────────────────────────────────────────────────────────────────────────
# Приватні хелпери — підготовка тексту
# ─────────────────────────────────────────────────────────────────────────────

def _remove_brand(text: str, brand: Optional[str]) -> str:
    if not brand:
        return text
    return re.sub(
        r"\s*" + re.escape(brand) + r"\s*",
        " ",
        text,
        flags=re.IGNORECASE,
    ).strip()


def _strip_parentheses(text: str) -> str:
    """Прибираємо дужки з розмірами/кольором, але НЕ з кодами моделей."""
    return re.sub(r"\([^)]*\)", " ", text).replace(",", " ").strip()


def _normalize_token(token: str) -> str:
    return (
        token.strip()
        .strip("()[]{}.,;:+")
        # прибираємо типографські лапки та спецсимволи
        .replace("″", "").replace('"', "")
        .replace("\u201c", "").replace("\u201d", "")
        .replace("•", "")  # Вт•ч → Вт ч, щоб unit-фільтр спрацював
    )


def _token_key(token: str) -> str:
    """Нормалізований ключ для порівняння (без цифр і спецсимволів)."""
    return re.sub(r"[^A-Za-zА-ЯҐЄІЇа-яґєії]", "", token).lower()


def _duplicate_keys(tokens: List[str]) -> Set[str]:
    """Ключі токенів, що зустрічаються більше одного разу (бренд у різних регістрах)."""
    counts: Dict[str, int] = {}
    for t in tokens:
        k = _token_key(t)
        if not k:
            continue
        counts[k] = counts.get(k, 0) + 1
    return {k for k, c in counts.items() if c > 1}


def _is_unit_like(token: str) -> bool:
    """True для технічних одиниць: 24В, 4000Вт, IP65, 100м, 128GB тощо."""
    t = _normalize_token(token)
    if not t:
        return True

    # IP65 / IK08 — рейтинги захисту
    if re.fullmatch(r"(IP|IK)\d{2,3}", t, flags=re.I):
        return True

    # TX1550/RX1310nm — довжини хвиль оптоволоконного обладнання
    if re.fullmatch(r"(TX|RX)\d{3,}(/(TX|RX)\d{3,})?nm?", t, flags=re.I):
        return True

    # 20KM / 100M — дальність передачі
    if re.fullmatch(r"\d+(KM|КМ)", t, flags=re.I):
        return True

    # 1000BASE-T / 10/100BASE-FX — стандарти мережі
    if re.search(r"\d+/?\d*BASE-", t, flags=re.I):
        return True

    # IEEE 802.3at/af/bt — стандарти PoE, не є моделлю
    if re.fullmatch(r"802\.3\w+", t, flags=re.I):
        return True

    # числові значення з одиницями (24В, 4000Вт, 60A, 100м, 128GB, 100Mbps …)
    if re.fullmatch(
        r"\d+(?:[.,]\d+)?"
        r"(вт|w|ч|в|v|a|ah|mah|м|m|мм|mm|см|cm|гб|gb|тб|tb|мп|mp|к|k|mbps|gbps|kbps)?",
        t,
        flags=re.I,
    ):
        return True

    return False


# ─────────────────────────────────────────────────────────────────────────────
# Приватні хелпери — пошук SKU
# ─────────────────────────────────────────────────────────────────────────────

def _early_acronym_candidate(text: str) -> Optional[str]:
    """
    Коротка абревіатура 2-5 літер, що стоїть перед токеном з цифрами — кандидат на модель.
    Напр.: "Westech EAP DUAL-1000ВА" → "EAP".
    Повертає None, якщо такого паттерну немає.
    """
    tokens = [_normalize_token(t) for t in text.split()]
    if len(tokens) < 3:
        return None

    _SKIP = re.compile(
        r"^(UHF|VHF|LTE|GSM|WIFI|WI-FI|POE|POE\+|UTP|FTP|DMR|NKP|GPS|GNSS)$",
        re.I,
    )

    for i in range(1, min(len(tokens) - 1, 8)):
        acr = tokens[i]
        nxt = tokens[i + 1] if i + 1 < len(tokens) else ""
        if not acr or not nxt:
            continue
        if not re.fullmatch(r"[A-Z]{2,5}", acr):
            continue
        if _SKIP.match(acr):
            continue
        if re.search(r"\d", nxt):
            return acr.upper()
    return None


def _looks_like_power_or_spec(token: str) -> bool:
    """
    Повертає True, якщо токен виглядає як спека потужності/напруги, а не модель.
    Напр.: DUAL-1000ВА, DUAL-1000VA/700W, TRIPLE-3000W, DUAL-1000.
    """
    t = token.upper()
    # ВА/КВА/ВТ або VA/KVA/W після цифр
    if re.search(r"\d{2,}\s*(?:ВА|КВА|ВТ|VA|KVA|W)\b", t):
        return True
    # слеш-комбінація: обидва боки >= 2 цифр (48/15000, 800/500), не чепає BBG-124/1
    if re.search(r"\d{2,}/\d{2,}", t):
        return True
    # DUAL-NNNN / TRIPLE-NNNN — інверторні позначення потужності
    if re.fullmatch(r"(?:DUAL|TRIPLE|SINGLE|TWIN)-\d{3,}", t):
        return True
    return False


def _find_hyphen_latin_skus(text: str) -> List[str]:
    """
    Дефісні SKU на латиниці: LETTER-START обов'язковий.
    Це запобігає помилковому захопленню "100BASE-T", "10/100BASE-FX" тощо.
    Приклади: SQ-04, S6-EH3P12K02-NV-YD-L, PV18-4024ECO, Wolf-14, MultiPlus-II
    """
    matches = _HYPHEN_LATIN_RE.findall(text)
    out: List[str] = []
    for m in matches:
        if _is_unit_like(m):
            continue
        prefix = m.split("-")[0].lower()
        if prefix in _SPEC_CODE_PREFIXES:
            continue
        out.append(m)
    return list(dict.fromkeys(out))


def _find_hyphen_cyrillic_skus(text: str) -> List[str]:
    """
    Дефісні SKU, що містять кирилицю: 902С-А, БК-165-1-пенал.
    Дозволяємо digit-start лише тут (кирилічні SKU часто починаються з цифри).
    Обов'язково: хоч одна кирилична літера + хоч одна цифра.
    """
    matches = _HYPHEN_CYRILLIC_RE.findall(text)
    out: List[str] = []
    for m in matches:
        if _is_unit_like(m):
            continue
        if not re.search(r"[А-ЯҐЄІЇа-яґєії]", m):
            continue
        if not re.search(r"\d", m):
            continue
        out.append(m.upper())
    return list(dict.fromkeys(out))


def _find_alnum_skus(text: str, brand: Optional[str] = None) -> List[str]:
    """
    Алфавітно-цифрові SKU без дефісів: S414, PMNN4808A, R7a, X5S.
    Фільтруємо дублікати (часто бренд повторюється у різних регістрах).
    """
    tokens = [_normalize_token(t) for t in text.split()]
    dups = _duplicate_keys(tokens)

    out: List[str] = []
    for tok in tokens:
        if not tok or _is_unit_like(tok):
            continue
        if _token_key(tok) in dups:
            continue
        if not re.search(r"\d", tok):
            continue
        if not re.search(r"[A-Za-z]", tok):
            continue
        # виключаємо відомі технологічні стандарти
        if re.fullmatch(r"(UHF|VHF|LTE|GSM|WIFI|WI-FI|POE|POE\+)", tok, flags=re.I):
            continue
        # виключаємо spec-code префікси (RJ-45, RS-232 тощо) з alnum-шляху
        tok_prefix = tok.split("-")[0].lower() if "-" in tok else ""
        if tok_prefix in _SPEC_CODE_PREFIXES:
            continue
        out.append(tok.upper())
    return list(dict.fromkeys(out))


def _pick_best_sku(candidates: List[str]) -> Optional[str]:
    """Обираємо найбільш специфічний SKU: довший, з більшою кількістю сегментів."""
    if not candidates:
        return None

    def score(s: str) -> tuple:
        return (
            min(len(s), 40),
            s.count("-") + s.count("/"),
            sum(ch.isdigit() for ch in s),
        )

    return max(candidates, key=score)


def _try_combine_with_prev_word(
    text: str,
    sku: str,
    brand: Optional[str] = None,
) -> Optional[str]:
    """
    Для ДУЖЕ коротких SKU (≤ 2 символи, напр. V2) додаємо попереднє слово.
    Порогове значення навмисно мале: "Rattler V2" → ОК, "Radio R7A" → НЕ ОК.
    """
    if len(sku) > 2:
        return None

    tokens = [_normalize_token(t) for t in text.split()]
    for i, t in enumerate(tokens):
        if t.upper() == sku and i > 0:
            prev = tokens[i - 1]
            if not prev or _is_unit_like(prev):
                return None
            if re.search(r"\d", prev):
                return None
            if not re.search(r"[A-Za-z]", prev):
                return None
            if prev.lower() in _GENERIC_STOPWORDS:
                return None
            if brand and prev.lower() == brand.lower():
                return None
            return f"{prev.upper()} {sku}"
    return None


def _try_extract_numeric_combo(text: str) -> Optional[str]:
    """
    Числові моделі з попереднім маркером:
    - "MultiPlus-II 48/15000/200-100" → з дефісного SKU + slash-combo
    - "MGA 108-550"                   → абревіатура + числовий діапазон
    """
    tokens = [_normalize_token(t) for t in text.split()]
    dups = _duplicate_keys(tokens)

    for i, tok in enumerate(tokens):
        if not tok:
            continue

        # slash-combo: 48/15000/200-100
        if re.fullmatch(r"\d{2,}(?:/\d{2,})+(?:-\d{2,})?", tok):
            if i > 0:
                prev = tokens[i - 1]
                if prev and not _is_unit_like(prev) and re.search(r"[A-Za-z]", prev):
                    return f"{prev.upper()} {tok}"
            return tok

        # числовий діапазон: 108-550 з попередньою абревіатурою (MGA)
        if re.fullmatch(r"\d{2,}-\d{2,}", tok) and i > 0:
            prev = tokens[i - 1]
            if (
                prev
                and re.fullmatch(r"[A-Za-z]{2,6}", prev)
                and prev.lower() not in _GENERIC_STOPWORDS
                and _token_key(prev) not in dups
            ):
                return f"{prev.upper()} {tok}"

    return None


def _extract_uppercase_sequence(text: str) -> Optional[str]:
    """
    Послідовність ≥ 2 UPPERCASE-слів як модель: "KAPPA HD KIT".
    Ігноруємо стоп-слова та дублікати.
    """
    tokens = [_normalize_token(t) for t in text.split()]
    dups = _duplicate_keys(tokens)

    best: List[str] = []
    cur: List[str] = []

    def flush() -> None:
        nonlocal best, cur
        if len(cur) >= 2 and cur[0] not in {"HD", "IP", "UHF", "VHF"}:
            if len(cur) > len(best):
                best[:] = cur[:]
        cur.clear()

    for tok in tokens:
        if (
            tok
            and tok.isalpha()
            and tok.isupper()
            and tok.lower() not in _GENERIC_STOPWORDS
            and _token_key(tok) not in dups
        ):
            cur.append(tok)
        else:
            flush()
    flush()

    return " ".join(best[:3]) if best else None


def _has_cyrillic_cable_designation(text: str) -> bool:
    """
    True якщо текст містить кирилічне позначення кабелю без цифр: КПВ-ВП, КППЭ-ВП.
    Такі кейси не мають окремого SKU — бренд є оптимальним ідентифікатором.
    """
    for m in re.findall(r"\b[А-яЁё]{2,}-[А-яЁё]+\b", text):
        if not re.search(r"\d", m):
            return True
    return False


def _fallback_model_from_words(text: str) -> Optional[str]:
    """
    Консервативний fallback:
    1) токен з буквами + цифрами (але не бренд-дублікат).
    2) перша пара слів з великої літери.
    Стоп-слова категорій виключаємо, щоб не повертати "Видеодомофон Slinex".
    """
    words = [_normalize_token(w) for w in text.split()]
    dups = _duplicate_keys(words)

    # пріоритет: токени з цифрами
    for w in words:
        if not w or _is_unit_like(w):
            continue
        if _token_key(w) in dups:
            continue
        if re.search(r"\d", w) and re.search(r"[A-Za-zА-ЯҐЄІЇа-яґєії]", w):
            return w.upper()

    # запасний варіант: слова з великої літери (але не стоп-слова)
    candidates: List[str] = []
    for w in words:
        if not w or len(w) < 3:
            continue
        if w.lower() in _GENERIC_STOPWORDS:
            continue
        if _token_key(w) in dups:
            continue
        if w[0].isupper() or w.isupper():
            candidates.append(w)

    if len(candidates) >= 2:
        return f"{candidates[0]} {candidates[1]}"
    if candidates:
        return candidates[0]
    return None


# ─────────────────────────────────────────────────────────────────────────────
# Приватні хелпери — накопичувачі (HDD / SD)
# ─────────────────────────────────────────────────────────────────────────────

def _is_storage_device(text: str) -> bool:
    storage_keywords = [
        "жесткий диск", "жорсткий диск",
        "карта памяти", "карта пам\u2019яті",
        "microsd", "micro sd",
    ]
    text_lower = text.lower()
    return any(kw in text_lower for kw in storage_keywords)


def _extract_storage_model(text: str) -> Optional[str]:
    """
    Витягування моделі для HDD, SSD, SD карт.
    "Жесткий диск Seagate SkyHawk ST1000VX013 1Тб" → "SkyHawk ST1000VX013"
    """
    tech_patterns = [
        r"^(Жесткий диск|Жорсткий диск)\s+(внутренний|внутрішній|зовнішній|external)?\s*",
        r"^(Карта памяти|Карта пам\u2019яті)\s+(MicroSD|microSD|SD|sd)?\s*",
    ]
    cleaned = text
    for pattern in tech_patterns:
        cleaned = re.sub(pattern, "", cleaned, flags=re.IGNORECASE).strip()

    # видаляємо ємність у кінці
    cleaned = re.sub(
        r"\s+\d+[\s]*(Тб|ТБ|TB|Гб|ГБ|GB|МБ|MB).*$",
        "",
        cleaned,
        flags=re.IGNORECASE,
    ).strip()

    # видаляємо відомі бренди з початку
    for b in ("Seagate", "Western Digital", "WD", "Ezviz", "Imou",
               "Kingston", "Samsung", "Toshiba", "Transcend"):
        if cleaned.lower().startswith(b.lower()):
            cleaned = cleaned[len(b):].strip()
            break

    return cleaned if cleaned and len(cleaned) > 2 else None
