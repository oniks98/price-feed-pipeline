"""
rozetka_stop_categories.py
---------------------------
Знаходить у Rozetka-фіді (data/markets/rozetka_feed.xml) усі товари, що
належать до "стоп-категорій" (категорії, які потребують ручного review /
виключення), і вивантажує їх у плоский CSV-звіт для аудиту.

Це extract-only звітний скрипт: сам rozetka_feed.xml НЕ змінюється.

Pipeline (ETL, тільки читання):
    extract (стрім-парсинг XML) -> validate -> normalize -> export CSV

Джерело: data/markets/rozetka_feed.xml           (Rozetka YML, десятки МБ)
Результат: data/markets/rozetka_stop_categories.csv  (";" delimiter, utf-8-sig)

Стоп-категорії — config, а не хардкод. Дефолтний список нижче
(DEFAULT_STOP_CATEGORY_IDS) можна розширювати без зміни логіки; для
одноразового прогону — прапорець --category-id (можна кілька разів).

Ідемпотентність: CSV повністю перезаписується при кожному запуску
(детермінований вивід, без інкрементального стану) — безпечно запускати
повторно чи паралельно з різними --output.

Запуск:
    python scripts/rozetka_stop_categories.py
    python scripts/rozetka_stop_categories.py --category-id 1554082 --category-id 80108
    python scripts/rozetka_stop_categories.py --source path\\to\\feed.xml --output path\\to\\out.csv
    python scripts/rozetka_stop_categories.py -v          # детальний debug-лог
"""

from __future__ import annotations

import argparse
import csv
import logging
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Final, Iterable, Iterator

from lxml import etree

# ---------------------------------------------------------------------------
# Config — єдине джерело правди, нижче по коду констант не хардкодимо
# ---------------------------------------------------------------------------

ROOT = Path(__file__).resolve().parents[1]

SOURCE_PATH: Final[Path] = ROOT / "data" / "markets" / "rozetka_feed.xml"
OUTPUT_PATH: Final[Path] = ROOT / "data" / "markets" / "rozetka_stop_categories.csv"

# Дефолтні стоп-категорії. Додавання нової категорії "в перспективі" —
# просто новий рядок тут (або одноразово через --category-id), без правок
# логіки нижче.
DEFAULT_STOP_CATEGORY_IDS: Final[tuple[str, ...]] = (
    "1554082",
    "80108",
    "4674585",
)

CSV_DELIMITER: Final[str] = ";"
CSV_ENCODING: Final[str] = "utf-8-sig"

# Схема виводу — єдине джерело правди для порядку колонок і CSV-заголовка,
# і для порядку значень у рядку (ніякого хардкоду позицій нижче).
FIELDNAMES: Final[tuple[str, ...]] = (
    "categoryId",
    "offer id",
    "article",
    "name_ua",
    "price",
    "stock_quantity",
)

OFFER_TAG: Final[str] = "offer"

log = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Модель
# ---------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class StopCategoryRow:
    category_id: str
    offer_id: str
    article: str
    name_ua: str
    price: str
    stock_quantity: str

    def as_csv_row(self) -> dict[str, str]:
        return {
            "categoryId": self.category_id,
            "offer id": self.offer_id,
            "article": self.article,
            "name_ua": self.name_ua,
            "price": self.price,
            "stock_quantity": self.stock_quantity,
        }


# ---------------------------------------------------------------------------
# Extract — стрім-парсинг, у пам'яті одночасно тільки один <offer>
# ---------------------------------------------------------------------------


def _child_text(element: etree._Element, tag: str) -> str:
    """Безпечний доступ до тексту дочірнього тегу: '' якщо тег відсутній/порожній."""
    child = element.find(tag)
    if child is None or child.text is None:
        return ""
    return child.text.strip()


def iter_offers(source_path: Path) -> Iterator[etree._Element]:
    """
    Стрім-парсить <offer> елементи з Rozetka YML фіду, не завантажуючи
    весь документ у пам'ять. Кожен елемент і його "мертві" попередники
    очищуються одразу після обробки — пам'ять лишається пласкою незалежно
    від розміру фіду (streaming, no full data in memory).
    """
    context = etree.iterparse(
        str(source_path),
        events=("end",),
        tag=OFFER_TAG,
        recover=True,  # не падати через поодинокі дрібні XML-дефекти в фіді
    )
    for _, element in context:
        yield element
        element.clear()
        while element.getprevious() is not None:
            del element.getparent()[0]
    del context


# ---------------------------------------------------------------------------
# Validate / normalize — окремий крок pipeline, ізольований від extract
# ---------------------------------------------------------------------------


def normalize_offer(
    element: etree._Element,
    stop_category_ids: frozenset[str],
) -> StopCategoryRow | None:
    """
    Валідує й нормалізує один <offer>.

    Повертає None ("safe drop"), якщо categoryId не входить у стоп-список,
    або якщо відсутнє критичне поле (id офера) — один пошкоджений offer
    ніколи не зупиняє весь прогін (reliability: no fail on single item).
    """
    category_id = _child_text(element, "categoryId")
    if category_id not in stop_category_ids:
        return None

    offer_id = str(element.get("id") or "").strip()
    if not offer_id:
        log.warning("dropped: offer without id (categoryId=%s)", category_id)
        return None

    return StopCategoryRow(
        category_id=category_id,
        offer_id=offer_id,
        article=_child_text(element, "article"),
        name_ua=_child_text(element, "name_ua"),
        price=_child_text(element, "price"),
        stock_quantity=_child_text(element, "stock_quantity"),
    )


# ---------------------------------------------------------------------------
# Export
# ---------------------------------------------------------------------------


def export_csv(rows: Iterable[StopCategoryRow], output_path: Path) -> int:
    """Пише рядки в CSV детерміновано (фіксований порядок колонок). Повертає к-сть рядків."""
    output_path.parent.mkdir(parents=True, exist_ok=True)
    written = 0
    with output_path.open("w", newline="", encoding=CSV_ENCODING) as fh:
        writer = csv.DictWriter(
            fh,
            fieldnames=list(FIELDNAMES),
            delimiter=CSV_DELIMITER,
            lineterminator="\n",
        )
        writer.writeheader()
        for row in rows:
            writer.writerow(row.as_csv_row())
            written += 1
    return written


# ---------------------------------------------------------------------------
# Оркестрація pipeline
# ---------------------------------------------------------------------------


def run(source_path: Path, output_path: Path, stop_category_ids: frozenset[str]) -> int:
    if not source_path.exists():
        raise FileNotFoundError(f"Rozetka feed не знайдено: {source_path}")
    if not stop_category_ids:
        raise ValueError("Список стоп-категорій порожній")

    log.info("джерело: %s", source_path)
    log.info(
        "стоп-категорії (%d): %s",
        len(stop_category_ids),
        ", ".join(sorted(stop_category_ids)),
    )

    matched_per_category: dict[str, int] = {}

    def _rows() -> Iterator[StopCategoryRow]:
        for element in iter_offers(source_path):
            row = normalize_offer(element, stop_category_ids)
            if row is None:
                continue
            matched_per_category[row.category_id] = matched_per_category.get(row.category_id, 0) + 1
            yield row

    written = export_csv(_rows(), output_path)

    for category_id, count in sorted(matched_per_category.items(), key=lambda kv: -kv[1]):
        log.info("categoryId=%s: %d товар(ів)", category_id, count)

    missing = stop_category_ids - matched_per_category.keys()
    if missing:
        log.warning("немає жодного товару для categoryId: %s", ", ".join(sorted(missing)))

    log.info("готово: %d рядків -> %s", written, output_path)
    return written


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Вивантажує товари Rozetka-фіду зі стоп-категорій у CSV",
    )
    parser.add_argument(
        "--source", type=Path, default=SOURCE_PATH, help=f"Шлях до фіду (default: {SOURCE_PATH})"
    )
    parser.add_argument(
        "--output", type=Path, default=OUTPUT_PATH, help=f"Шлях до CSV (default: {OUTPUT_PATH})"
    )
    parser.add_argument(
        "--category-id",
        dest="category_ids",
        action="append",
        default=None,
        metavar="ID",
        help="ID стоп-категорії; можна вказати кілька разів. "
        "Якщо не вказано — використовується DEFAULT_STOP_CATEGORY_IDS.",
    )
    parser.add_argument("-v", "--verbose", action="store_true", help="Debug-логування")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> None:
    args = parse_args(argv)
    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s | %(levelname)-8s | %(message)s",
        datefmt="%H:%M:%S",
    )

    stop_category_ids = frozenset(args.category_ids) if args.category_ids else frozenset(DEFAULT_STOP_CATEGORY_IDS)

    try:
        run(args.source, args.output, stop_category_ids)
    except (FileNotFoundError, ValueError) as exc:
        log.error(str(exc))
        sys.exit(1)


if __name__ == "__main__":
    main()
