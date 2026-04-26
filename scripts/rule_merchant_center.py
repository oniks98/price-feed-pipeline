"""
rule_merchant_center.py
-----------------------
Крок 1 пайплайну: генерує CSV-правила для generate_merchant_feed.py.

Парсить google_merchant_center.xml (Prom.ua експорт) та оновлює:
  data/markets/rule_merchant_center.csv — нові категорії → theme + schedule=day

Збагачення XML-фіду — виключно в generate_merchant_feed.py.

CSV-схема (rule_merchant_center.csv):
  keyword   — точний рядок g:product_type з фіду
  theme     — тема для custom_label_0
  schedule  — day/night для custom_label_3 (вручну змінювати у CSV)
  notes     — довільні нотатки

Мітки в фіді:
  custom_label_0 = theme    (з цього CSV, по product_type)
  custom_label_1 = brand    (g:brand as-is — точна назва бренду для таргетингу)
  custom_label_2 = price_tier
  custom_label_3 = schedule (з цього CSV, по product_type)

Гарантії:
  - Існуючі рядки CSV не переміщуються і не перезаписуються.
  - Нові keywords дописуються в кінець файлу (append).
  - Якщо файл відсутній — створюється з нуля.
  - --reclassify оновлює тільки theme == 'other' в існуючих рядках
    (schedule НЕ чіпає).

Запуск:
    python scripts/rule_merchant_center.py
        Стандартний режим: парсить XML, дописує нові категорії.

    python scripts/rule_merchant_center.py --dry-run
        Виводить у лог що БУДЕ додано — БЕЗ запису в файли.

    python scripts/rule_merchant_center.py --reclassify
        Перекласифікує theme == 'other' у існуючих рядках.
        schedule залишається незмінним.
"""

from __future__ import annotations

import argparse
import csv
import logging
import xml.etree.ElementTree as ET
from pathlib import Path
from typing import Final

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------

BASE_DIR:    Final[Path] = Path(__file__).parent.parent
MARKETS_DIR: Final[Path] = BASE_DIR / "data" / "markets"
FEED_URL:    Final[str]  = (
    "https://oniks.org.ua/google_merchant_center.xml"
    "?hash_tag=ae28973743ce141e994ceb22bf044021"
    "&product_ids=&label_ids=&export_lang=uk"
    "&group_ids=2222437%2C2222561%2C2234751%2C4320349%2C4325742%2C4325743"
    "%2C10015559%2C22818554%2C45720479%2C127351905%2C139094517%2C152104228"
    "%2C152208563%2C152208591%2C152208632"
    "&nested_group_ids=2222437%2C2222561%2C2234751%2C4320349%2C4325742%2C4325743"
    "%2C10015559%2C22818554%2C45720479%2C127351905%2C139094517%2C152104228"
    "%2C152208563%2C152208591%2C152208632"
)
RULES_CSV: Final[Path] = MARKETS_DIR / "rule_merchant_center.csv"

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

ENCODING:         Final[str] = "utf-8-sig"
NS_G:             Final[str] = "http://base.google.com/ns/1.0"
NS:               Final[dict[str, str]] = {"g": NS_G}
FALLBACK_THEME:   Final[str] = "other"
DEFAULT_SCHEDULE: Final[str] = "day"   # нові рядки → day; вручну міняти на night у CSV

# ---------------------------------------------------------------------------
# CSV field names
# ---------------------------------------------------------------------------

RULES_FIELDNAMES: Final[list[str]] = [
    "keyword", "theme", "schedule", "google_cat_id", "google_cat_hint", "notes",
]

# ---------------------------------------------------------------------------
# Theme rules (ordered: first match wins — specific sub-themes BEFORE base themes)
# ---------------------------------------------------------------------------

THEME_RULES: Final[list[tuple[str, str]]] = [

    # ── Military ─────────────────────────────────────────────────────────────
    ("воєнторг",                                        "military"),

    # ── Drone ────────────────────────────────────────────────────────────────
    ("квадрокоптер",                                    "drone"),

    # ── Alarm / Fire / Flood ─────────────────────────────────────────────────
    ("охоронні системи та сигналізації",                "alarm_systems"),
    ("приймально-контрольні прилади",                   "alarm_systems"),
    ("пожежн",                                          "alarm"),
    ("охоронні системи",                                "alarm"),
    ("тривожні кнопки",                                 "access"),
    ("системи охорони та оповіщення",                   "alarm"),
    ("охоронн",                                         "alarm"),
    ("датчики руху",                                    "alarm"),
    ("від потопу",                                      "alarm"),

    # ── Access control ────────────────────────────────────────────────────────
    ("системи сквд",                                    "access_systems"),
    ("контролери сккд",                                 "access_controllers"),
    ("аксесуари для домофонного",                       "access"),
    ("домофон",                                         "access_intercom"),
    ("зчитувач",                                        "access_readers"),
    ("ідентифікатор",                                   "access_identifiers"),
    ("замки та клямки",                                 "access_locks"),
    ("доводчик",                                        "access_closers"),
    ("сккд",                                            "access"),
    ("сквд",                                            "access"),
    ("кодонабірн",                                      "access"),
    ("турнікет",                                        "access"),
    ("комплектуючі для замків",                         "access"),

    # ── Smart home ────────────────────────────────────────────────────────────
    ("розумного будинку",                               "smarthome"),
    ("якості повітря",                                  "smarthome"),
    ("зволожувачі",                                     "smarthome"),
    ("кліматична техніка",                              "smarthome"),
    ("інфрачервон",                                     "smarthome"),
    ("зоотовари",                                       "smarthome"),
    ("товари для домашніх тварин",                      "smarthome"),
    ("годівниці",                                       "smarthome"),
    ("побутове очищення води",                          "smarthome"),
    ("фільтри комплексного очищення",                   "smarthome"),
    ("побутове водопостачання",                         "smarthome"),
    ("для особистого користування",                     "smarthome"),

    # ── TV / Satellite ────────────────────────────────────────────────────────
    ("кабелі для електроніки",                          "cable"),
    ("телевізійні антени",                              "tv_antennas"),
    ("медіаплеєр",                                      "tv_players"),
    ("ресивер",                                         "tv_receivers"),
    ("tv та відеотехніка",                              "tv"),
    ("кабельне телебачення",                            "tv"),
    ("супутников",                                      "tv"),
    ("телевізійн",                                      "tv"),
    ("телевізор",                                       "tv"),
    ("запчастини для телевізорів",                      "tv"),

    # ── Video surveillance ────────────────────────────────────────────────────
    ("камери відеоспостереження",                       "video_cameras"),
    ("стаціонарні відеореєстратор",                     "video_recorders"),
    ("відеокамер",                                      "video_cameras"),
    ("відеоняні",                                       "video_cameras"),
    ("радіоняні",                                       "video_cameras"),
    ("фотопастк",                                       "video_cameras"),
    ("камери для полювання",                            "video_cameras"),
    ("відеоспостереження",                              "video"),
    ("відеонагляд",                                     "video"),
    ("відеореєстратор",                                 "video"),
    ("комутатори сигналу",                              "video"),

    # ── Network / Telecom ─────────────────────────────────────────────────────
    ("роутер",                                          "network_routers"),
    ("комутатор",                                       "network_switches"),
    ("телекомунікації та зв'язок",                      "network"),
    ("бездротовий зв'язок",                             "network"),
    ("мережеве обладнання",                             "network"),
    ("патч-корд",                                       "network"),
    ("патч-панел",                                      "network"),
    ("sfp",                                             "network"),
    ("gbic",                                            "network"),
    ("wi-fi",                                           "network"),
    ("оптоволокон",                                     "network"),
    ("приймачі і передавачі сигналу",                   "network"),
    ("стаціонарні телефони",                            "network"),
    ("серверне обладнання",                             "network"),
    ("мережеві накопичувач",                            "network"),
    ("кабельні тестери",                                "network"),
    ("інструмент для закладення кабелю",                "network"),

    # ── Power ─────────────────────────────────────────────────────────────────
    ("джерела безперебійного",                          "power_ups"),
    ("промислові та побутові джерела живлення",         "power_supplies"),
    ("акумулятори загального призначення",              "power_battery"),
    ("зарядні станції",                                 "power_stations"),
    ("повербанки",                                      "power_banks"),
    ("повербанк",                                       "power_banks"),
    ("електрогенератор",                                "power_generators"),
    ("генератори та електростанції",                    "power_generators"),
    ("стабілізатор",                                    "power_stabilizers"),
    ("інвертор",                                        "power_inverters"),
    ("батарейк",                                        "power"),
    ("джерела живлення",                                "power"),
    ("дбж",                                             "power_ups"),
    ("реле напруги",                                    "energy"),
    ("акумулятор",                                      "power"),
    ("зарядні",                                         "power"),
    ("сонячні панелі",                                  "power"),
    ("сонячні контролери",                              "power"),
    ("альтернативні джерела енергії",                   "power"),
    ("щитове обладнання",                               "energy"),
    ("автоматичні вимикачі",                            "energy"),

    # ── Cable / Wiring ────────────────────────────────────────────────────────
    ("монтажні шафи",                                   "cable_box"),
    ("кабель для систем зв'язку",                       "cable_systems"),
    ("монтажне обладнання",                             "cable"),
    ("кабеленесуч",                                     "cable"),
    ("електроізолятор",                                 "cable"),
    ("силові кабелі",                                   "cable"),

    # ── Energy / Electrical infrastructure ───────────────────────────────────
    ("вуличне освітлення",                              "energy"),
    ("led освітлення",                                  "energy"),
    ("настінні вимикачі",                               "energy"),
    ("розетки електричні",                              "energy"),
    ("силові вилки та розетки",                         "energy"),
    ("електричні вилки",                                "energy"),
    ("електричні подовжувачі",                          "energy"),

    # ── IT / Computing ────────────────────────────────────────────────────────
    ("автомобільні відеосистеми",                       "video"),
    ("комп'ютерна техніка",                             "it"),
    ("жорсткі диски",                                   "it"),
    ("карти пам'яті",                                   "it"),
    ("комп'ютерні аксесуари",                           "it"),
    ("планшет",                                         "it"),
    ("монітор",                                         "it"),
    ("клавіатур",                                       "it"),
    ("носії інформації",                                "it"),

    # ── Audio ─────────────────────────────────────────────────────────────────
    ("мікрофон",                                        "components"),
    ("аудіотехніка",                                    "audio"),

    # ── Tool ─────────────────────────────────────────────────────────────────
    ("садові пилосмокти",                               "tool"),
    ("інструменти для обробки грунту",                  "tool"),
    ("інструменти для обрізки",                         "tool"),
    ("електроінструмент",                               "tool"),
    ("ручний інструмент",                               "tool"),
    ("мультитул",                                       "tool"),
    ("паяльник",                                        "tool"),
    ("багатофункціональні інструменти",                 "tool"),
    ("апарати високого тиску",                          "tool"),
    ("драбин",                                          "tool"),
    ("сходи",                                           "tool"),
    ("риштування",                                      "tool"),

    # ── Components / Measurement ──────────────────────────────────────────────
    ("термометри",                                      "components"),
    ("пірометри",                                       "components"),
    ("тепловізори",                                     "components"),
    ("прилади вимірювання",                             "components"),
    ("мультиметр",                                      "components"),
    ("металошукач",                                     "components"),
    ("металодетектор",                                  "components"),
    ("запчастини для техніки",                          "components"),
    ("комплектуючі для відеотехніки",                   "components"),
    ("кухонні ваги",                                    "components"),
    ("герметик",                                        "components"),
    ("монтажна піна",                                   "components"),
    ("силікон",                                         "components"),
    ("кронштейн",                                       "components"),
    ("засоби для очищення побутової техніки",           "components"),
    ("очищення побутової техніки",                      "components"),

    # ── Safety / PPE ──────────────────────────────────────────────────────────
    ("спецодяг",                                        "safety"),
    ("захисний",                                        "safety"),
    ("проблискові маячки",                              "alarm"),
    ("спецсигнали",                                     "alarm"),
    ("сирени",                                          "alarm"),
    ("дорожні огорожі",                                 "safety"),
    ("шлагбаум",                                        "safety"),
    ("ворота, огорожі",                                 "safety"),
    ("аксесуари для воріт",                             "safety"),

    # ── Outdoor / Camping ────────────────────────────────────────────────────
    ("туризм",                                          "outdoor"),
    ("туристичн",                                       "outdoor"),
    ("садов",                                           "outdoor"),
    ("садовий інвентар",                                "outdoor"),
    ("ліхтар",                                          "outdoor"),
    ("ножі для полювання",                              "outdoor"),
    ("каремат",                                         "outdoor"),
    ("тренажер",                                        "outdoor"),
    ("спортивні стінки",                                "outdoor"),
    ("вила",                                            "tool"),
    ("граблі",                                          "tool"),
    ("садові ножиці",                                   "tool"),
    ("пилосмокти, повітродувки",                        "tool"),
]

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------

log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Classifier
# ---------------------------------------------------------------------------

def classify_theme(keyword: str) -> str:
    """
    Класифікує product_type → theme.
    1. Матч по листовому сегменту (після останнього '>') — найбільш специфічний.
    2. Fallback: матч по повному breadcrumb.
    """
    leaf = keyword.rsplit(">", 1)[-1].strip().lower()
    for pattern, theme in THEME_RULES:
        if pattern.lower() in leaf:
            return theme
    kw_lower = keyword.lower()
    for pattern, theme in THEME_RULES:
        if pattern.lower() in kw_lower:
            return theme
    return FALLBACK_THEME


# ---------------------------------------------------------------------------
# XML parsing
# ---------------------------------------------------------------------------

def _text(el: ET.Element, tag: str) -> str:
    child = el.find(f"g:{tag}", NS)
    if child is None:
        child = el.find(tag)
    if child is None:
        return ""
    return (child.text or "").strip()


def _fetch_xml(url: str) -> ET.Element:
    from urllib.request import Request, urlopen
    log.info("Fetching feed: %s", url.split("?")[0])
    req = Request(url, headers={"User-Agent": "RuleMerchantCenter/1.0"})
    with urlopen(req, timeout=60) as resp:
        return ET.fromstring(resp.read())


def parse_xml(url: str) -> list[dict[str, str]]:
    root  = _fetch_xml(url)
    items: list[dict[str, str]] = []
    for item_el in root.iter("item"):
        product_type = _text(item_el, "product_type")
        if product_type:
            items.append({"product_type": product_type})
    log.info("Parsed %d items from feed", len(items))
    return items


# ---------------------------------------------------------------------------
# rule_merchant_center.csv
# ---------------------------------------------------------------------------

def load_rules(path: Path) -> tuple[list[dict[str, str]], set[str]]:
    if not path.exists():
        return [], set()
    with path.open(encoding=ENCODING, newline="") as f:
        rows = list(csv.DictReader(f, delimiter=";"))
    return rows, {row["keyword"] for row in rows}


def update_rules_csv(
    path: Path,
    items: list[dict[str, str]],
    *,
    dry_run: bool = False,
    reclassify: bool = False,
) -> None:
    """
    Дописує нові product_type рядки в кінець файлу.
    --reclassify: оновлює тільки theme == 'other'. schedule НЕ чіпає.
    """
    existing_rows, known = load_rules(path)

    reclassified = 0
    if reclassify:
        for row in existing_rows:
            if row.get("theme", FALLBACK_THEME) == FALLBACK_THEME:
                theme = classify_theme(row["keyword"])
                if theme != FALLBACK_THEME:
                    row["theme"] = theme
                    log.info("RULE RECLASSIFY  theme=%-20s  kw=%s", theme, row["keyword"][:70])
                    reclassified += 1

    new_keywords = sorted(
        {item["product_type"] for item in items if item["product_type"]} - known
    )

    new_rows: list[dict[str, str]] = []
    for kw in new_keywords:
        theme = classify_theme(kw)
        new_rows.append({
            "keyword":         kw,
            "theme":           theme,
            "schedule":        DEFAULT_SCHEDULE,
            "google_cat_id":   "",
            "google_cat_hint": "",
            "notes":           "auto",
        })
        log.info("RULE NEW  theme=%-20s  kw=%s", theme, kw[:70])

    log.info(
        "rules CSV — new: %d | reclassified: %d | existing: %d",
        len(new_rows), reclassified, len(existing_rows),
    )

    if dry_run:
        return

    path.parent.mkdir(parents=True, exist_ok=True)

    if reclassify or not path.exists():
        with path.open("w", encoding=ENCODING, newline="") as f:
            writer = csv.DictWriter(f, fieldnames=RULES_FIELDNAMES, delimiter=";")
            writer.writeheader()
            writer.writerows(existing_rows)
            writer.writerows(new_rows)
    else:
        if not new_rows:
            log.info("rules CSV — nothing to do")
            return
        file_exists = path.exists() and path.stat().st_size > 0
        if not file_exists:
            with path.open("w", encoding=ENCODING, newline="") as f:
                writer = csv.DictWriter(f, fieldnames=RULES_FIELDNAMES, delimiter=";")
                writer.writeheader()
                writer.writerows(new_rows)
        else:
            with path.open("a", encoding=ENCODING, newline="") as f:
                writer = csv.DictWriter(f, fieldnames=RULES_FIELDNAMES, delimiter=";")
                writer.writerows(new_rows)

    log.info("rules CSV updated: %s (+%d rows, %d reclassified)", path, len(new_rows), reclassified)


# ---------------------------------------------------------------------------
# Pipeline
# ---------------------------------------------------------------------------

def run(*, dry_run: bool = False, reclassify: bool = False) -> None:
    items = parse_xml(FEED_URL)
    update_rules_csv(RULES_CSV, items, dry_run=dry_run, reclassify=reclassify)


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description=(
            "Крок 1: парсить google_merchant_center.xml і генерує CSV-правила "
            "для generate_merchant_feed.py."
        )
    )
    p.add_argument(
        "--dry-run", action="store_true",
        help="Показати що буде додано — БЕЗ запису в файли.",
    )
    p.add_argument(
        "--reclassify", action="store_true",
        help=(
            "Перекласифікує theme == 'other' у існуючих рядках. "
            "schedule залишається незмінним."
        ),
    )
    return p.parse_args()


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(levelname)-8s %(message)s")
    args = _parse_args()
    run(dry_run=args.dry_run, reclassify=args.reclassify)


if __name__ == "__main__":
    main()
