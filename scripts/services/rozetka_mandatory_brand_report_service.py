"""
Звіт про підстановки Anker у категоріях Rozetka, де виробник обов'язковий.

Джерело категорій — публічна Google Таблиця Rozetka. Сервіс не змінює фід:
він лише приймає події про підстановки виробника, а після categoryId-маппінгу
записує в logs/ категорії з таблиці, у яких такі події лишилися у фіді.

Використання в generate_rozetka_feed.py:

    reporter = MandatoryBrandReplacementReporter()
    updated_xml = fill_missing_vendor(
        updated_xml, on_vendor_filled=reporter.record_default_vendor
    )
    updated_xml = remap_rozetka_vendors(
        updated_xml, on_replacement=reporter.record_brand_remapping
    )
    updated_xml, _ = replace_category_ids(updated_xml)
    reporter.write_report(updated_xml)
"""

from __future__ import annotations

import csv
import html
import json
import logging
import os
import re
import time
from collections import Counter
from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime, timezone
from io import StringIO
from pathlib import Path
from tempfile import NamedTemporaryFile
from typing import Final

import requests
from requests import RequestException

logger = logging.getLogger(__name__)

# Ці значення можна перевизначити в CI без зміни коду, якщо Rozetka перенесе
# список на іншу таблицю або лист.
_DEFAULT_SPREADSHEET_ID: Final[str] = "1ELm2-ay5KvXQJsf-jiF-Hi99-TVh-aK6YQgMUDxSa5E"
_DEFAULT_SHEET_GID: Final[str] = "1302008865"
_DEFAULT_CSV_URL: Final[str] = (
    f"https://docs.google.com/spreadsheets/d/{_DEFAULT_SPREADSHEET_ID}/export"
    f"?format=csv&gid={_DEFAULT_SHEET_GID}"
)
MANDATORY_BRAND_CATEGORIES_CSV_URL: Final[str] = os.getenv(
    "ROZETKA_MANDATORY_BRAND_CATEGORIES_CSV_URL", _DEFAULT_CSV_URL
)

_ROOT: Final[Path] = Path(__file__).resolve().parents[2]
DEFAULT_LOG_PATH: Final[Path] = _ROOT / "logs" / "rozetka_mandatory_brand_replacements.log"
_CACHE_PATH: Final[Path] = (
    _ROOT / "data" / "markets" / "rozetka_mandatory_brand_categories_cache.json"
)

_REQUEST_TIMEOUT_SECONDS: Final[int] = 30
_MAX_DOWNLOAD_ATTEMPTS: Final[int] = 3
_RETRY_BACKOFF_SECONDS: Final[float] = 2.0
_ANKER_KEY: Final[str] = "anker"

_CATEGORY_COLUMN_HEADERS: Final[frozenset[str]] = frozenset({
    "категорія",
    "назва категорії",
    "category",
    "category name",
})
_CATEGORY_ID_COLUMN_HEADERS: Final[frozenset[str]] = frozenset({
    "id категорії",
    "ід категорії",
    "category id",
    "id category",
})

_OFFER_RE: Final[re.Pattern[str]] = re.compile(
    r'<offer\s+id="([^"]+)"[^>]*>(.*?)</offer>', re.DOTALL
)
_CATEGORY_ID_RE: Final[re.Pattern[str]] = re.compile(r"<categoryId>(\d+)</categoryId>")
_ARTICLE_RE: Final[re.Pattern[str]] = re.compile(r"<article>(.*?)</article>", re.DOTALL)
_NAME_UA_RE: Final[re.Pattern[str]] = re.compile(r"<name_ua>(.*?)</name_ua>", re.DOTALL)
_CDATA_RE: Final[re.Pattern[str]] = re.compile(r"<!\[CDATA\[(.*?)\]\]>", re.DOTALL)
_WHITESPACE_RE: Final[re.Pattern[str]] = re.compile(r"\s+")
_INTEGER_RE: Final[re.Pattern[str]] = re.compile(r"\b(\d+)\b")


class MandatoryBrandCategoriesError(RuntimeError):
    """Google-список обов'язкових категорій неможливо прочитати."""


@dataclass(frozen=True)
class ReplacementReportStats:
    """Підсумок останнього записаного звіту."""

    tracked_replacements: int
    matched_replacements: int
    category_count: int


def _clean_text(value: str) -> str:
    """Прибирає HTML та повторні пробіли з клітинки Google Sheets."""
    value = re.sub(r"<[^>]+>", "", value)
    return _WHITESPACE_RE.sub(" ", html.unescape(value).replace("\xa0", " ")).strip()


def _unwrap_cdata(value: str) -> str:
    """Знімає обгортку ``<![CDATA[...]]>`` офер-тега Prom XML, якщо вона є."""
    match = _CDATA_RE.fullmatch(value.strip())
    return match.group(1) if match else value


def _extract_tag_text(body: str, pattern: re.Pattern[str]) -> str:
    """Витягує та очищує текст одного офер-тега; відсутній тег → "" (без падіння)."""
    match = pattern.search(body)
    if match is None:
        return ""
    return _clean_text(_unwrap_cdata(match.group(1)))


def _header_key(value: str) -> str:
    """Нормалізує назву колонки для нечутливого до регістру зіставлення."""
    return _clean_text(value).casefold().replace("_", " ").replace("-", " ")


def _looks_like_html(value: str) -> bool:
    """Google віддає HTML при відсутньому доступі замість CSV."""
    return value.lstrip()[:200].casefold().startswith(("<!doctype html", "<html"))


def _download_csv(url: str) -> str:
    """Завантажує CSV Google Sheets з обмеженими повторними спробами."""
    last_error: Exception | None = None
    for attempt in range(1, _MAX_DOWNLOAD_ATTEMPTS + 1):
        try:
            response = requests.get(
                url,
                headers={"User-Agent": "PriceFeedPipeline/1.0"},
                timeout=_REQUEST_TIMEOUT_SECONDS,
            )
            response.raise_for_status()
            content = response.content.decode("utf-8-sig")
            if _looks_like_html(content):
                raise MandatoryBrandCategoriesError(
                    "Google Sheets повернув HTML замість CSV"
                )
            return content
        except (RequestException, UnicodeDecodeError, MandatoryBrandCategoriesError) as exc:
            last_error = exc
            if attempt < _MAX_DOWNLOAD_ATTEMPTS:
                time.sleep(_RETRY_BACKOFF_SECONDS * attempt)

    assert last_error is not None
    raise MandatoryBrandCategoriesError(
        f"Не вдалося завантажити список обов'язкових категорій: {last_error}"
    ) from last_error


def _parse_rows(csv_text: str) -> list[list[str]]:
    """Читає непорожні CSV-рядки та нормалізує їхні клітинки."""
    rows: list[list[str]] = []
    for row in csv.reader(StringIO(csv_text)):
        cleaned = [_clean_text(cell) for cell in row]
        if any(cleaned):
            rows.append(cleaned)
    return rows


def _find_columns(rows: list[list[str]]) -> tuple[int, int, int]:
    """Повертає ``(header_row, category_col, category_id_col)``."""
    for row_index, row in enumerate(rows[:10]):
        category_column: int | None = None
        category_id_column: int | None = None
        for column_index, value in enumerate(row):
            key = _header_key(value)
            if key in _CATEGORY_COLUMN_HEADERS:
                category_column = column_index
            elif key in _CATEGORY_ID_COLUMN_HEADERS:
                category_id_column = column_index
        if category_column is not None and category_id_column is not None:
            return row_index, category_column, category_id_column
    raise MandatoryBrandCategoriesError(
        "У Google Sheets не знайдено колонки «Категорія» та «id категорії»"
    )


def _parse_category_id(raw_value: str) -> int | None:
    """Підтримує значення таблиці у форматі ``id=80086`` або просто ``80086``."""
    match = _INTEGER_RE.search(raw_value)
    return int(match.group(1)) if match else None


def _categories_from_csv(csv_text: str) -> dict[int, str]:
    """Повертає індекс ``{rozetka_category_id: назва з Google Sheets}``."""
    rows = _parse_rows(csv_text)
    if not rows:
        raise MandatoryBrandCategoriesError("Google Sheets порожній")

    header_row, category_column, category_id_column = _find_columns(rows)
    categories: dict[int, str] = {}
    skipped_rows = 0

    for row in rows[header_row + 1:]:
        category = row[category_column] if len(row) > category_column else ""
        raw_category_id = row[category_id_column] if len(row) > category_id_column else ""
        category_id = _parse_category_id(raw_category_id)
        if not category or category_id is None:
            skipped_rows += 1
            continue

        existing = categories.setdefault(category_id, category)
        if existing != category:
            logger.warning(
                "Google Sheets: дубль id категорії=%d має різні назви: %r і %r; "
                "залишено першу",
                category_id,
                existing,
                category,
            )

    if not categories:
        raise MandatoryBrandCategoriesError(
            "У Google Sheets немає коректних рядків обов'язкових категорій"
        )
    if skipped_rows:
        logger.warning(
            "Google Sheets: пропущено %d порожніх або некоректних рядків категорій",
            skipped_rows,
        )
    return categories


def _save_categories_cache(categories: dict[int, str], source_url: str) -> None:
    """Зберігає останню валідну версію, не впливаючи на основний фід."""
    payload = {
        "cached_at": datetime.now(timezone.utc).isoformat(),
        "source_url": source_url,
        "categories": [
            {"id": category_id, "name": name}
            for category_id, name in sorted(categories.items())
        ],
    }
    try:
        _CACHE_PATH.parent.mkdir(parents=True, exist_ok=True)
        _CACHE_PATH.write_text(
            json.dumps(payload, ensure_ascii=False, indent=2) + "\n",
            encoding="utf-8",
        )
    except OSError as exc:
        logger.warning("Не вдалося зберегти кеш категорій Rozetka: %s", exc)


def _load_categories_cache() -> tuple[dict[int, str], str] | None:
    """Повертає валідний кеш або ``None``; пошкоджений кеш ігнорується."""
    try:
        payload = json.loads(_CACHE_PATH.read_text(encoding="utf-8"))
        raw_categories = payload["categories"]
        categories = {
            int(item["id"]): _clean_text(str(item["name"]))
            for item in raw_categories
            if _clean_text(str(item["name"]))
        }
        cached_at = str(payload["cached_at"])
    except (OSError, KeyError, TypeError, ValueError, json.JSONDecodeError):
        return None
    return (categories, cached_at) if categories else None


def load_mandatory_brand_categories(url: str | None = None) -> dict[int, str]:
    """
    Завантажує перелік обов'язкових категорій з Google Sheets.

    Коли стандартне джерело тимчасово недоступне, використовується останній
    валідний кеш. Переданий ``url`` призначений для тестів/ручного override і
    не читає та не змінює production-кеш.
    """
    source_url = MANDATORY_BRAND_CATEGORIES_CSV_URL if url is None else url
    try:
        categories = _categories_from_csv(_download_csv(source_url))
    except MandatoryBrandCategoriesError as exc:
        if url is not None:
            raise
        cached = _load_categories_cache()
        if cached is None:
            raise
        categories, cached_at = cached
        print(
            "⚠️  Rozetka обов'язкові бренди: Google Sheets недоступний, "
            f"використано кеш від {cached_at} ({len(categories)} категорій)"
        )
        return categories

    if url is None:
        _save_categories_cache(categories, source_url)
    print(f"📋 Rozetka обов'язкові бренди: завантажено {len(categories)} категорій")
    return categories


class MandatoryBrandReplacementReporter:
    """
    Накопичує події підстановки Anker і записує детермінований зведений лог.

    Відстеження за ``offer_id`` виключає повторний підрахунок одного товару,
    якщо виробник дублюється у ``vendor`` і ``Компанія-виробник``.
    """

    def __init__(
        self,
        log_path: Path = DEFAULT_LOG_PATH,
        categories_loader: Callable[[], dict[int, str]] = load_mandatory_brand_categories,
    ) -> None:
        self._log_path = log_path
        self._categories_loader = categories_loader
        self._replacement_by_offer: dict[str, str] = {}

    def record_default_vendor(self, offer_id: str, reason: str) -> None:
        """Фіксує випадок, коли ``fill_missing_vendor`` вставив Anker."""
        self._replacement_by_offer.setdefault(
            offer_id,
            f"{reason} виробник → Anker",
        )

    def record_brand_remapping(
        self,
        offer_id: str,
        original_vendor: str,
        replacement_vendor: str,
    ) -> None:
        """Фіксує лише маппінги, чиїм результатом є Anker."""
        if replacement_vendor.strip().casefold() != _ANKER_KEY:
            return
        original = _clean_text(original_vendor)
        if original:
            self._replacement_by_offer.setdefault(offer_id, f"{original} → Anker")

    def write_report(self, xml: str) -> ReplacementReportStats:
        """
        Зіставляє зафіксовані offer_id з фінальними categoryId та пише лог.

        Категорія береться з already-mapped XML, а її назва — виключно з
        Google Sheets. Неможливість прочитати таблицю створює діагностичний
        лог, але не зупиняє генерацію фіду.
        """
        tracked = len(self._replacement_by_offer)
        if not tracked:
            stats = ReplacementReportStats(0, 0, 0)
            self._write_log(
                [
                    "Rozetka: підстановок Anker не було.",
                    "Категорії з Google Sheets не завантажувалися.",
                ]
            )
            print(f"🧾 Rozetka Anker / обов'язкові категорії: підстановок немає | лог: {self._log_path}")
            return stats

        try:
            mandatory_categories = self._categories_loader()
        except Exception as exc:
            logger.warning("Не вдалося сформувати звіт обов'язкових категорій: %s", exc)
            self._write_log(
                [
                    "Rozetka: звіт не зіставлено з обов'язковими категоріями.",
                    f"Причина: {exc}",
                    f"Зафіксовано підстановок Anker: {tracked}",
                ]
            )
            print(
                "⚠️  Rozetka Anker / обов'язкові категорії: "
                f"не вдалося завантажити Google Sheets | лог: {self._log_path}"
            )
            return ReplacementReportStats(tracked, 0, 0)

        replacement_counts: Counter[tuple[int, str, str]] = Counter()
        product_refs: dict[tuple[int, str, str], list[tuple[str, str, str]]] = {}
        for offer_match in _OFFER_RE.finditer(xml):
            offer_id, body = offer_match.groups()
            reason = self._replacement_by_offer.get(offer_id)
            if reason is None:
                continue
            category_match = _CATEGORY_ID_RE.search(body)
            if category_match is None:
                continue
            category_id = int(category_match.group(1))
            category_name = mandatory_categories.get(category_id)
            if category_name is not None:
                key = (category_id, category_name, reason)
                article = _extract_tag_text(body, _ARTICLE_RE)
                name_ua = _extract_tag_text(body, _NAME_UA_RE)
                replacement_counts[key] += 1
                # article — це «Код товару» у Prom XML, name_ua — назва товару
                # укр. мовою. offer_id лишається резервним ідентифікатором для
                # оферів без article.
                product_refs.setdefault(key, []).append((article, offer_id, name_ua))

        matched = sum(replacement_counts.values())
        category_ids = {category_id for category_id, _, _ in replacement_counts}
        stats = ReplacementReportStats(tracked, matched, len(category_ids))
        self._write_log(self._format_report(stats, replacement_counts, product_refs))

        print(
            "🧾 Rozetka Anker / обов'язкові категорії: "
            f"{matched} товарів у {len(category_ids)} категоріях "
            f"(усього підстановок Anker: {tracked}) | лог: {self._log_path}"
        )
        return stats

    def _format_report(
        self,
        stats: ReplacementReportStats,
        replacement_counts: Counter[tuple[int, str, str]],
        product_refs: dict[tuple[int, str, str], list[tuple[str, str, str]]],
    ) -> list[str]:
        lines = [
            "Rozetka: підстановки Anker у категоріях з обов'язковим виробником",
            f"Створено (UTC): {datetime.now(timezone.utc).isoformat()}",
            f"Джерело категорій: {MANDATORY_BRAND_CATEGORIES_CSV_URL}",
            f"Усього зафіксованих підстановок Anker: {stats.tracked_replacements}",
            f"Підстановок у mandatory-категоріях: {stats.matched_replacements}",
            f"Категорій із підстановками: {stats.category_count}",
            "",
        ]
        if not replacement_counts:
            lines.append("Збігів із категоріями з Google Sheets немає.")
            return lines

        by_category: dict[tuple[int, str], Counter[str]] = {}
        for (category_id, category_name, reason), count in replacement_counts.items():
            by_category.setdefault((category_id, category_name), Counter())[reason] += count

        for (category_id, category_name), reasons in sorted(
            by_category.items(),
            key=lambda item: (item[0][1].casefold(), item[0][0]),
        ):
            total = sum(reasons.values())
            lines.append(f"{category_name} (id={category_id}) — {total} товарів")
            for reason, count in sorted(reasons.items(), key=lambda item: item[0].casefold()):
                lines.append(f"  {reason}: {count}")
                for article, offer_id, name_ua in product_refs[(category_id, category_name, reason)]:
                    name_part = name_ua if name_ua else "назва відсутня"
                    if article:
                        lines.append(
                            f"    Код товару: {article} (offer_id={offer_id}) — {name_part}"
                        )
                    else:
                        lines.append(
                            f"    Код товару відсутній (offer_id={offer_id}) — {name_part}"
                        )
        return lines

    def _write_log(self, lines: list[str]) -> None:
        """Записує лог атомарно, щоб паралельний reader не побачив напівфайл."""
        temporary_path: Path | None = None
        try:
            self._log_path.parent.mkdir(parents=True, exist_ok=True)
            with NamedTemporaryFile(
                mode="w",
                encoding="utf-8",
                dir=self._log_path.parent,
                prefix=f".{self._log_path.name}.",
                suffix=".tmp",
                delete=False,
            ) as temporary_file:
                temporary_file.write("\n".join(lines) + "\n")
                temporary_path = Path(temporary_file.name)
            temporary_path.replace(self._log_path)
        except OSError as exc:
            logger.warning("Не вдалося записати звіт Rozetka: %s", exc)
            if temporary_path is not None:
                try:
                    temporary_path.unlink(missing_ok=True)
                except OSError:
                    pass
