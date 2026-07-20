"""
services/rozetka_stop_brand_service.py
---------------------------------------
Фільтрує Rozetka-фід за публічним Google Sheets списком стоп-брендів.

Таблиця: «Стоп-категорії та стоп-бренди маркетплейс» → лист gid=672087803.
Сервіс знаходить стовпець «Назва бренду / ТМ» за заголовком (без прив'язки
до позиції) і зчитує всі непорожні клітинки під ним.

Використання в generate_rozetka_feed.py:
    from services.rozetka_stop_brand_service import filter_stop_brand_offers
    updated_xml = filter_stop_brand_offers(updated_xml)
"""

from __future__ import annotations

import csv
import html
import json
import re
import time
from collections import Counter
from datetime import datetime, timezone
from io import StringIO
from pathlib import Path
from typing import Final, Iterable

import requests
from requests import RequestException

# ---------------------------------------------------------------------------
# Конфігурація Google Sheets
# Опублікована таблиця використовує шлях e/ — URL відрізняється від звичайних таблиць.
# ---------------------------------------------------------------------------

_PUBLISHED_ID: Final[str] = (
    "2PACX-1vQqHOjuMG8fd9FMF6__c9kEE6IoVvYEOKmysmJpMDVuNj-XdsAkmQp1AR34pQ0Dqg"
)
_SHEET_GID: Final[str] = "672087803"

# Кілька варіантів URL для однієї опублікованої таблиці — перевіряються по черзі як резервні.
# Опубліковані таблиці надають лише ендпоінт pub?output=csv (без /export, без gviz за замовчуванням).
STOP_BRANDS_CSV_URL: Final[str] = (
    f"https://docs.google.com/spreadsheets/d/e/{_PUBLISHED_ID}"
    f"/pub?gid={_SHEET_GID}&single=true&output=csv"
)
_STOP_BRANDS_CSV_URLS: Final[tuple[str, ...]] = (
    STOP_BRANDS_CSV_URL,
    # Альтернативний формат pub (деякі старіші версії GSheets відповідають на такий)
    (
        f"https://docs.google.com/spreadsheets/d/e/{_PUBLISHED_ID}"
        f"/pub?gid={_SHEET_GID}&output=csv"
    ),
)

_REQUEST_TIMEOUT: Final[int] = 30
_MAX_ATTEMPTS: Final[int] = 3
_RETRY_BACKOFF_SECONDS: Final[float] = 2.0

# Локальний резервний варіант, якщо всі джерела недоступні (експорт Google
# pub/csv відомий своєю нестабільністю незалежно від повторних спроб —
# див. докстрінг _download_csv).
_STOP_BRANDS_CACHE_PATH: Final[Path] = (
    Path(__file__).resolve().parents[2] / "data" / "markets" / "rozetka_stop_brands_cache.json"
)

# ---------------------------------------------------------------------------
# Варіанти заголовків, що ідентифікують стовпець "назва бренду" в таблиці.
# Порівнюються після casefold і згортання варіантів -, _, пробілів.
# ---------------------------------------------------------------------------

_BRAND_COL_HEADERS: Final[frozenset[str]] = frozenset({
    "назва бренду",
    "назва бренду / тм",
    "назва бренду/тм",
    "назва тм",
    "бренд",
    "бренди",
    "торгова марка",
    "тм",
    "brand",
    "brands",
    "stop brand",
    "stop brands",
})

# Назви <param> у Prom, що відповідають полю бренду всередині <offer>.
_PROM_BRAND_PARAM_NAMES: Final[frozenset[str]] = frozenset({
    "brand",
    "бренд",
    "виробник",
    "производитель",
    "торгова марка",
    "торговая марка",
    "марка",
})

# ---------------------------------------------------------------------------
# Скомпільовані регулярні вирази — на рівні модуля, компілюються один раз.
# ---------------------------------------------------------------------------

_OFFER_RE: Final[re.Pattern[str]] = re.compile(
    r'[ \t]*<offer\s+id="([^"]+)"([^>]*)>(.*?)</offer>[ \t]*\n?',
    re.DOTALL,
)
_VENDOR_RE: Final[re.Pattern[str]] = re.compile(
    r"<vendor>(.*?)</vendor>",
    re.DOTALL,
)
_PROM_PARAM_RE: Final[re.Pattern[str]] = re.compile(
    r'<param\b[^>]*\bname="([^"]+)"[^>]*>(.*?)</param>',
    re.DOTALL,
)
_CDATA_RE: Final[re.Pattern[str]] = re.compile(
    r'<!\[CDATA\[(.*?)\]\]>',
    re.DOTALL,
)
_HTML_TAG_RE: Final[re.Pattern[str]] = re.compile(r"<[^>]+>")

# Розбиває складені значення бренду на кшталт "Samsung / LG", "Bosch, Siemens", "A + B".
_BRAND_SPLIT_RE: Final[re.Pattern[str]] = re.compile(
    r"\s*(?:[,;/|]|\s+\+\s+)\s*"
)


# ---------------------------------------------------------------------------
# Допоміжні функції для роботи з текстом
# ---------------------------------------------------------------------------

def _clean_text(value: str) -> str:
    """Прибирає обгортки CDATA, HTML-теги, HTML-сутності та згортає пробіли."""
    value = _CDATA_RE.sub(lambda m: m.group(1), value)
    value = _HTML_TAG_RE.sub("", value)
    value = html.unescape(value)
    value = value.replace("\xa0", " ")
    return re.sub(r"\s+", " ", value).strip()


def _normalize_brand(value: str) -> str:
    """Casefold + очищення — використовується як ключ словника для зіставлення брендів."""
    return _clean_text(value).casefold()


def _header_key(value: str) -> str:
    """Нормалізує клітинку заголовка для порівняння з _BRAND_COL_HEADERS."""
    value = _normalize_brand(value)
    value = value.replace("_", " ").replace("-", " ")
    return re.sub(r"\s+", " ", value).strip()


def _looks_like_html(text: str) -> bool:
    head = text.lstrip()[:200].casefold()
    return head.startswith("<!doctype html") or head.startswith("<html")


# ---------------------------------------------------------------------------
# Завантаження та парсинг CSV
# ---------------------------------------------------------------------------

def _download_csv(url: str) -> str:
    """Завантажує CSV, повторюючи спроби при тимчасових мережевих помилках.

    Ендпоінт опублікованої таблиці Google інколи обриває з'єднання
    посеред відповіді або відповідає повільно під навантаженням
    ("Response ended prematurely", таймаути читання) — це не проблеми
    URL чи конфігурації, тож варто повторити спробу перед переходом
    до наступного варіанта URL.
    """
    last_exc: RequestException | None = None
    for attempt in range(1, _MAX_ATTEMPTS + 1):
        try:
            response = requests.get(url, timeout=_REQUEST_TIMEOUT)
            response.raise_for_status()
            csv_text = response.content.decode("utf-8-sig")
            if _looks_like_html(csv_text):
                raise RuntimeError("Google Sheets повернув HTML замість CSV")
            return csv_text
        except RequestException as exc:
            last_exc = exc
            if attempt < _MAX_ATTEMPTS:
                wait = _RETRY_BACKOFF_SECONDS * attempt
                print(
                    f"⚠️  Спроба {attempt}/{_MAX_ATTEMPTS} завантажити {url} "
                    f"невдала ({exc}), повтор через {wait:.0f}с"
                )
                time.sleep(wait)
    assert last_exc is not None
    raise last_exc


def _rows_from_csv(csv_text: str) -> list[list[str]]:
    rows: list[list[str]] = []
    for row in csv.reader(StringIO(csv_text)):
        cleaned = [_clean_text(cell) for cell in row]
        if any(cleaned):
            rows.append(cleaned)
    return rows


# ---------------------------------------------------------------------------
# Локальний резервний кеш
# Зберігає останній успішно завантажений список, щоб запуск не блокувався,
# коли всі живі джерела одночасно недоступні (чому таке трапляється навіть
# з повторними спробами — див. докстрінг _download_csv).
# ---------------------------------------------------------------------------

def _save_stop_brands_cache(brands: frozenset[str]) -> None:
    """Запис за принципом best-effort — помилка кешу ніколи не має зупиняти запуск."""
    payload = {
        "cached_at": datetime.now(timezone.utc).isoformat(),
        "brands": sorted(brands),
    }
    try:
        _STOP_BRANDS_CACHE_PATH.parent.mkdir(parents=True, exist_ok=True)
        _STOP_BRANDS_CACHE_PATH.write_text(
            json.dumps(payload, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
    except OSError as exc:
        print(f"⚠️  Rozetka стоп-бренди: не вдалося зберегти кеш ({exc})")


def _load_cached_stop_brands() -> tuple[frozenset[str], str] | None:
    """Читає останній кешований список. За будь-якої проблеми повертає None, ніколи не кидає виняток."""
    try:
        payload = json.loads(_STOP_BRANDS_CACHE_PATH.read_text(encoding="utf-8"))
        brands = frozenset(payload["brands"])
        cached_at = str(payload.get("cached_at", "?"))
    except (OSError, json.JSONDecodeError, KeyError, TypeError):
        return None
    return (brands, cached_at) if brands else None


# ---------------------------------------------------------------------------
# Визначення стовпця та вилучення брендів
# ---------------------------------------------------------------------------

def _find_brand_column(rows: list[list[str]]) -> tuple[int, int] | None:
    """
    Повертає (header_row_idx, col_idx) стовпця "Назва бренду / ТМ".

    Шукає в перших 10 рядках, щоб урахувати можливі рядки заголовка/підзаголовка
    над самим заголовком таблиці. Повертає None, якщо стовпець не знайдено.
    """
    for row_idx, row in enumerate(rows[:10]):
        for col_idx, cell in enumerate(row):
            if _header_key(cell) in _BRAND_COL_HEADERS:
                return row_idx, col_idx
    return None


def _iter_brand_values(
    rows: list[list[str]],
    header_row_idx: int,
    col_idx: int,
) -> Iterable[str]:
    """
    Повертає (yield) непорожні рядки брендів зі col_idx, починаючи після header_row_idx.

    Рядки над заголовком (рядки з назвою/підзаголовком) свідомо пропускаються,
    щоб їх ніколи не сплутати з назвами брендів.
    """
    for row in rows[header_row_idx + 1 :]:
        if col_idx >= len(row):
            continue
        value = row[col_idx]
        if value:
            yield value


# ---------------------------------------------------------------------------
# Публічна функція: завантаження стоп-брендів
# ---------------------------------------------------------------------------

def load_stop_brands(url: str | None = None) -> frozenset[str]:
    """
    Завантажує назви стоп-брендів із публічного CSV-експорту Google Таблиці.

    Якщо всі живі URL недоступні, повертає останній кешований на диску список
    (див. _STOP_BRANDS_CACHE_PATH) — експорт Google pub/csv відомий своєю
    нестабільністю незалежно від повторних спроб (див. докстрінг _download_csv).

    Args:
        url: Перевизначення URL для тестів; у продакшені використовується
             _STOP_BRANDS_CSV_URLS. Передача url вимикає дисковий кеш (читання й запис).

    Returns:
        Frozenset із сирих (до нормалізації) рядків назв брендів.

    Raises:
        RuntimeError: якщо всі URL недоступні, стовпець бренду не знайдено
            і немає придатного кешу.
    """
    use_cache = url is None  # тестові перевизначення ніколи не торкаються дискового кешу
    errors: list[str] = []
    for csv_url in ((url,) if url else _STOP_BRANDS_CSV_URLS):
        try:
            csv_text = _download_csv(csv_url)
            rows = _rows_from_csv(csv_text)
        except (RequestException, RuntimeError, UnicodeDecodeError) as exc:
            errors.append(f"{csv_url}: {exc}")
            continue

        col_result = _find_brand_column(rows)
        if col_result is None:
            errors.append(
                f"{csv_url}: стовпець «Назва бренду / ТМ» не знайдено "
                f"(перевірте заголовки: {[r[:5] for r in rows[:3]]})"
            )
            continue

        header_row_idx, col_idx = col_result
        brands = frozenset(_iter_brand_values(rows, header_row_idx, col_idx))
        if brands:
            print(f"🚫 Rozetka стоп-бренди: завантажено {len(brands)}")
            if use_cache:
                _save_stop_brands_cache(brands)
            return brands

        errors.append(f"{csv_url}: список порожній або нечитабельний")

    if use_cache:
        cached = _load_cached_stop_brands()
        if cached is not None:
            cached_brands, cached_at = cached
            print(
                f"⚠️  Rozetka стоп-бренди: усі джерела недоступні, "
                f"використано кеш від {cached_at} ({len(cached_brands)} брендів)"
            )
            return cached_brands

    raise RuntimeError(
        "Не вдалося завантажити стоп-бренди Rozetka (кеш також недоступний):\n"
        + "\n".join(errors)
    )


# ---------------------------------------------------------------------------
# Допоміжні функції для зіставлення брендів
# ---------------------------------------------------------------------------

def _stop_brand_index(stop_brands: Iterable[str]) -> dict[str, str]:
    """
    Будує словник пошуку {normalized_brand: display_brand}.

    Використовує setdefault, щоб зберігати першу зустрінуту форму
    відображення при дублікатах.
    """
    index: dict[str, str] = {}
    for brand in stop_brands:
        norm = _normalize_brand(brand)
        if norm:
            index.setdefault(norm, _clean_text(brand))
    return index


def _brand_candidates(body: str) -> list[str]:
    """
    Витягує кандидатів у назви бренду з тіла одного <offer>:
      1. значення тегу <vendor> (основне джерело у фідах Rozetka/Prom)
      2. значення <param name="Бренд|brand|..."> (вторинне / резервне)

    Повертає список непорожніх очищених рядків (зберігає порядок: спочатку vendor).
    """
    candidates: list[str] = []

    vendor_match = _VENDOR_RE.search(body)
    if vendor_match:
        candidates.append(_clean_text(vendor_match.group(1)))

    for param_match in _PROM_PARAM_RE.finditer(body):
        param_name = _normalize_brand(param_match.group(1))
        if param_name in _PROM_BRAND_PARAM_NAMES:
            candidates.append(_clean_text(param_match.group(2)))

    return [c for c in candidates if c]


def _matched_stop_brand(
    candidates: Iterable[str],
    stop_index: dict[str, str],
) -> str | None:
    """
    Повертає відображувану назву першого стоп-бренду, знайденого серед
    кандидатів, або None, якщо збігів немає.

    Кожен кандидат перевіряється як цілком, так і розбитим за роздільниками
    (/, ,, ;, |, +) — для обробки складених рядків бренду на кшталт
    "Bosch / Siemens".
    """
    for candidate in candidates:
        for token in (candidate, *_BRAND_SPLIT_RE.split(candidate)):
            norm = _normalize_brand(token)
            if norm in stop_index:
                return stop_index[norm]
    return None


# ---------------------------------------------------------------------------
# Публічна функція: фільтрація XML-фіда
# ---------------------------------------------------------------------------

def filter_stop_brand_offers(
    xml: str,
    stop_brands: Iterable[str] | None = None,
) -> str:
    """
    Видаляє елементи <offer>, чий vendor/бренд збігається зі списком стоп-брендів.

    Args:
        xml:         Повний рядок XML-фіда.
        stop_brands: Перевизначення списку брендів для тестів; у продакшені
                     завантажується з Google Таблиці.

    Returns:
        XML з видаленими відповідними offer (порожній рядок замість кожного видаленого).

    Raises:
        RuntimeError: якщо список стоп-брендів порожній після нормалізації.
    """
    raw_stop_brands = (
        load_stop_brands() if stop_brands is None else frozenset(stop_brands)
    )
    stop_index = _stop_brand_index(raw_stop_brands)
    if not stop_index:
        raise RuntimeError("Список стоп-брендів Rozetka порожній після нормалізації")

    removed_by_brand: Counter[str] = Counter()

    def _on_offer(m: re.Match[str]) -> str:
        body = m.group(3)
        matched_brand = _matched_stop_brand(_brand_candidates(body), stop_index)
        if matched_brand is None:
            return m.group(0)
        removed_by_brand[matched_brand] += 1
        return ""

    filtered_xml = _OFFER_RE.sub(_on_offer, xml)

    removed_total = sum(removed_by_brand.values())
    if removed_total:
        brands_summary = ", ".join(
            f"{brand} ({count})" for brand, count in removed_by_brand.most_common()
        )
        print(
            f"🚫 Rozetka стоп-бренди: видалено {removed_total} товарів "
            f"| {brands_summary}"
        )
    else:
        print("🚫 Rozetka стоп-бренди: збігів у фіді немає")

    return filtered_xml
