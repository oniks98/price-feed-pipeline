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
  - --reclassify-all оновлює theme у всіх існуючих рядках за поточними правилами
    (schedule НЕ чіпає).

Запуск:
    python scripts/rule_merchant_center.py
        Стандартний режим: парсить XML, дописує нові категорії.

    python scripts/rule_merchant_center.py --dry-run
        Виводить у лог що БУДЕ додано — БЕЗ запису в файли.

    python scripts/rule_merchant_center.py --reclassify
        Перекласифікує theme == 'other' у існуючих рядках.
        schedule залишається незмінним.

    python scripts/rule_merchant_center.py --reclassify-all
        Перекласифікує всі існуючі рядки за поточною semantic theme-мапою.
        schedule залишається незмінним.
"""

from __future__ import annotations

import argparse
import csv
import logging
import xml.etree.ElementTree as ET
from pathlib import Path
from typing import Final

from constants_feed_url import FEED_URL_MERCHANT

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------

BASE_DIR:    Final[Path] = Path(__file__).parent.parent
MARKETS_DIR: Final[Path] = BASE_DIR / "data" / "markets"
RULES_CSV:   Final[Path] = MARKETS_DIR / "rule_merchant_center.csv"

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
    ("тактичні тепловізори",                             "military"),
    ("прилади нічного бачення",                          "military"),
    ("приціли і мушки",                                  "military"),
    ("тактичний захист",                                 "military"),
    ("плитоноски",                                       "military"),
    ("розвантажувальні жилети",                          "military"),
    ("аксесуари та комплектуючі для касок",              "military"),
    ("переговорні пристрої",                             "military"),
    ("воєнторг",                                        "military"),

    # ── Drone ────────────────────────────────────────────────────────────────
    ("квадрокоптер",                                    "military_drone"),

    # ── Alarm / Fire / Flood ─────────────────────────────────────────────────
    ("охоронні системи та сигналізації",                "alarm_systems"),
    ("приймально-контрольні прилади",                   "alarm_systems"),
    ("модулі контролю та управління",                   "alarm_systems"),
    ("гучномовці",                                      "alarm_audio"),
    ("акустичні системи",                               "alarm_audio"),
    ("підсилювачі звуку",                               "alarm_audio"),
    ("охоронні сповіщувачі",                            "alarm_sensors"),
    ("датчики для розумного будинку",                    "alarm_sensors"),
    ("датчики якості повітря",                           "alarm_sensors"),
    ("датчики вологості і температури",                  "alarm_sensors"),
    ("датчики руху",                                    "alarm_sensors"),
    ("пожежн",                                          "alarm"),
    ("охоронні системи",                                "alarm"),
    ("тривожні кнопки",                                 "access"),
    ("системи охорони та оповіщення",                   "alarm"),
    ("охоронн",                                         "alarm"),
    ("від потопу",                                      "alarm"),

    # ── Access control ────────────────────────────────────────────────────────
    ("системи сквд",                                    "access_systems"),
    ("контролери сккд",                                 "access_controllers"),
    ("аксесуари для домофонного",                       "access_components"),
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
    ("шлагбаум",                                        "access"),
    ("ворота, огорожі",                                 "access"),
    ("аксесуари для воріт",                             "access"),
    ("механізми і автоматика для вікон і дверей",       "access"),

    # ── Smart home / IoT sensors ──────────────────────────────────────────────
    ("дверні дзвінки",                                   "home"),
    ("розумного будинку",                               "smarthome"),
    ("якості повітря",                                  "smarthome"),

    # ── Industrial specifics that would otherwise match generic appliances ───
    ("промислові пилососи",                             "tool_industrial"),

    # ── Appliances / home utility ─────────────────────────────────────────────
    ("запчастини для пилососів",                         "appliance"),
    ("запчастини для зволожувачів",                      "appliance"),
    ("очищувачів повітря",                               "appliance"),
    ("зволожувачі та очищувачі",                         "appliance"),
    ("засоби для очищення побутової техніки",            "appliance"),
    ("очищення побутової техніки",                       "appliance"),
    ("побутове очищення води",                           "appliance"),
    ("фільтри комплексного очищення",                    "appliance"),
    ("побутове водопостачання",                          "appliance"),
    ("кліматична техніка",                               "appliance"),
    ("інфрачервон",                                      "appliance"),
    ("холодильники",                                     "appliance"),
    ("пилососи",                                         "appliance"),
    ("пароочисники",                                     "appliance"),
    ("кухонні ваги",                                     "appliance"),

    # ── Pet supplies ──────────────────────────────────────────────────────────
    ("зоотовари",                                        "pet"),
    ("товари для домашніх тварин",                       "pet"),
    ("годівниці",                                        "pet"),
    ("поїлки",                                           "pet"),
    ("миски для домашніх тварин",                        "pet"),
    ("сумки і контейнери для перенесення",               "pet"),
    ("засоби для гігієни тварин",                        "pet"),

    # ── TV / Satellite ────────────────────────────────────────────────────────
    ("кабелі для електроніки",                          "cable_components"),
    ("телевізійні антени",                              "tv_antennas"),
    ("супутникові антени",                              "tv_antennas"),
    ("медіаплеєр",                                      "tv_players"),
    ("ресивер",                                         "tv_receivers"),
    ("wi-fi адаптери",                                  "tv_network"),
    ("антенні підсилювачі",                              "tv_network"),
    ("ду пульти",                                       "tv_components"),
    ("кріплення та ліфти",                              "tv_components"),
    ("дайсеки",                                         "tv_components"),
    ("запчастини для супутникового обладнання",         "tv_components"),
    ("супутникові конвертери",                          "tv_components"),
    ("tv та відеотехніка",                              "tv"),
    ("кабельне телебачення",                            "tv"),
    ("супутников",                                      "tv"),
    ("телевізійн",                                      "tv"),
    ("телевізор",                                       "tv"),
    ("запчастини для телевізорів",                      "tv"),

    # ── Video surveillance ────────────────────────────────────────────────────
    ("відеокамери, екшн-камери",                        "video_action"),
    ("автомобільні відеореєстратори",                   "video_auto"),
    ("радіоняні, відеоняні",                            "video_baby"),
    ("фотопастки, камери для полювання",                "video_trap"),
    ("мікрофони",                                       "video_components"),
    ("камери відеоспостереження",                       "video_cameras"),
    ("стаціонарні відеореєстратор",                     "video_recorders"),
    ("відеокамер",                                      "video_cameras"),
    ("відеоняні",                                       "video_cameras"),
    ("радіоняні",                                       "video_cameras"),
    ("фотопастк",                                       "video_cameras"),
    ("камери для полювання",                            "video_cameras"),
    ("кожухи і кронштейни",                             "video_components"),
    ("пульти для систем відеоспостереження",            "video_components"),
    ("об'єктиви для камер відеоспостереження",          "video_components"),
    ("комплектуючі для систем відеоспостереження",      "video_components"),
    ("комутатори сигналу",                              "video_components"),
    ("приймачі і передавачі сигналу",                   "video_components"),
    ("відеореєстратор",                                 "video"),
    ("відеоспостереження",                              "video"),
    ("відеонагляд",                                     "video"),

    # ── Network / Telecom ─────────────────────────────────────────────────────
    ("роутер",                                          "network_routers"),
    ("комутатор",                                       "network_switches"),
    ("кабельні тестери",                                "tool_network"),
    ("інструмент для закладення кабелю",                "tool_network"),
    ("телекомунікації та зв'язок",                      "network"),
    ("бездротовий зв'язок",                             "network"),
    ("мережеве обладнання",                             "network"),
    ("патч-корд",                                       "network"),
    ("патч-панел",                                      "network"),
    ("sfp",                                             "network"),
    ("gbic",                                            "network"),
    ("wi-fi",                                           "network"),
    ("оптоволокон",                                     "network"),
    ("стаціонарні телефони",                            "network"),
    ("серверне обладнання",                             "network"),
    ("мережеві накопичувач",                            "network"),
    ("обладнання для живлення по ethernet",             "network"),

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
    ("кабель для систем зв'язку",                       "cable"),
    ("шнури, перехідники",                              "cable_components"),
    ("роз'єми та конектори",                            "cable_components"),
    ("силові кабелі",                                   "cable_components"),
    ("електроізоляційні стрічки",                       "cable_components"),
    ("кабеленесучі системи",                            "cable_components"),
    ("монтажне обладнання",                             "cable"),
    ("кабеленесуч",                                     "cable"),
    ("електроізолятор",                                 "cable"),

    # ── Energy / Electrical infrastructure ───────────────────────────────────
    ("настінні вимикачі",                               "energy"),
    ("розетки електричні",                              "energy"),
    ("силові вилки та розетки",                         "energy"),
    ("електричні вилки",                                "energy"),
    ("електричні подовжувачі",                          "energy"),

    # ── Lighting ──────────────────────────────────────────────────────────────
    ("led освітлення",                                  "lighting"),
    ("led підсвітка",                                   "lighting"),
    ("вуличне освітлення",                              "lighting"),
    ("стельові світильники",                            "lighting"),
    ("лампочки",                                        "lighting"),
    ("настільні лампи",                                 "lighting"),
    ("нічники",                                         "lighting"),
    ("точкові світильники",                             "lighting"),
    ("трек-системи",                                    "lighting"),

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
    ("мікрофон",                                        "video_components"),
    ("аудіотехніка",                                    "alarm_audio"),

    # ── Measuring / detectors ─────────────────────────────────────────────────
    ("датчики вологості і температури",                  "alarm_sensors"),
    ("термометри",                                      "tool_measuring"),
    ("пірометри",                                       "tool_measuring"),
    ("тепловізори",                                     "tool_measuring"),
    ("прилади вимірювання",                             "tool_measuring"),
    ("контрольно-вимірювальні прилади",                 "tool_measuring"),
    ("мультиметр",                                      "tool_measuring"),
    ("металошукач",                                     "tool_measuring"),
    ("металодетектор",                                  "tool_measuring"),
    ("детектори прихованої проводки",                   "tool_measuring"),
    ("будівельні рівні",                                "tool_measuring"),
    ("вимірювальні рулетки",                            "tool_measuring"),
    ("нівелірні рейки",                                 "tool_measuring"),
    ("лазерні далекоміри",                              "tool_measuring"),
    ("штативи для вимірювального",                      "tool_measuring"),

    # ── Industrial equipment ──────────────────────────────────────────────────
    ("точильні верстати",                               "tool_industrial"),
    ("металообробні верстати",                          "tool_industrial"),
    ("станини",                                         "tool_industrial"),
    ("верстаки",                                        "tool_industrial"),
    ("робочі столи",                                    "tool_industrial"),
    ("поршневі компресори",                             "tool_industrial"),
    ("промислові пилососи",                             "tool_industrial"),

    # ── Power tools ───────────────────────────────────────────────────────────
    ("будівельні фени",                                 "tool_power"),
    ("електроінструмент",                               "tool_power"),
    ("багатофункціональні інструменти",                 "tool_power"),
    ("дрилі",                                           "tool_power"),
    ("шуруповерти",                                     "tool_power"),
    ("електричні будівельні міксери",                   "tool_power"),
    ("електричні гайковерти",                           "tool_power"),
    ("електроножівки",                                  "tool_power"),
    ("ручні дискові пили",                              "tool_power"),
    ("електролобзики",                                  "tool_power"),
    ("клейові електричні пістолети",                    "tool_power"),
    ("паяльник",                                        "tool_power"),
    ("перфоратори",                                     "tool_power"),
    ("фрезерні машини",                                 "tool_power"),
    ("шліфувальні машини",                              "tool_power"),
    ("штроборізи",                                      "tool_power"),
    ("пневматичні відбійні молотки",                    "tool_power"),
    ("гідравлічні ланцюгові пили",                      "tool_power"),

    # ── Tool accessories ──────────────────────────────────────────────────────
    ("запчастини для інструменту",                       "tool_accessories"),
    ("ящики, сумки для інструментів",                    "tool_accessories"),
    ("викруткові біти",                                  "tool_accessories"),
    ("відрізні",                                         "tool_accessories"),
    ("зачисні",                                          "tool_accessories"),
    ("шліфувальні, пильні круги",                        "tool_accessories"),
    ("зубила",                                           "tool_accessories"),
    ("долото для перфоратора",                           "tool_accessories"),
    ("ножувальні полотна",                               "tool_accessories"),
    ("оливи, мастила для інструменту",                   "tool_accessories"),
    ("пилки для лобзиків",                               "tool_accessories"),
    ("свердла",                                          "tool_accessories"),
    ("бури",                                             "tool_accessories"),

    # ── Hand tools ────────────────────────────────────────────────────────────
    ("ручні пили",                                       "tool_hand"),
    ("ножівки",                                          "tool_hand"),
    ("ручні ножиці по металу",                           "tool_hand"),
    ("набори інструментів",                              "tool_hand"),
    ("молотки",                                          "tool_hand"),
    ("кувалди",                                          "tool_hand"),
    ("киянки",                                           "tool_hand"),
    ("будівельні ножі",                                  "tool_hand"),
    ("будівельні олівці",                                "tool_hand"),
    ("викрутки",                                         "tool_hand"),
    ("бокорізи",                                         "tool_hand"),
    ("кусачки",                                          "tool_hand"),
    ("затискні кліщі",                                   "tool_hand"),
    ("пасатижі",                                         "tool_hand"),
    ("плоскогубці",                                      "tool_hand"),
    ("тонкогубці",                                       "tool_hand"),
    ("трубні кліщі",                                     "tool_hand"),
    ("набори ключів",                                    "tool_hand"),
    ("розвідні ключі",                                   "tool_hand"),
    ("торцеві головки",                                  "tool_hand"),
    ("шестигранні ключі",                                "tool_hand"),
    ("монтажні пінцети",                                 "tool_hand"),
    ("мультитул",                                        "tool_hand"),
    ("знімачі ізоляції",                                 "tool_hand"),
    ("ручні обтискні інструменти",                       "tool_hand"),
    ("сокири",                                           "tool_hand"),

    # ── Hardware / building consumables ───────────────────────────────────────
    ("кронштейн",                                        "home"),
    ("метричне кріплення",                               "home"),
    ("гайки",                                            "home"),
    ("герметик",                                         "hardware"),
    ("монтажна піна",                                    "hardware"),
    ("силікон",                                          "hardware"),

    # ── Safety / PPE ──────────────────────────────────────────────────────────
    ("робочі рукавички",                                 "safety"),
    ("господарські рукавички",                           "safety"),
    ("захисний спецодяг",                                "safety"),
    ("робочі комбінезони",                               "safety"),
    ("напівкомбінезони",                                 "safety"),
    ("аксесуари для безпеки дітей",                      "safety"),
    ("будівельні каски",                                 "safety"),
    ("засоби захисту очей",                              "safety"),
    ("засоби захисту органів дихання",                   "safety"),
    ("засоби безпеки праці",                             "safety"),
    ("засоби індивідуального захисту",                   "safety"),
    ("спецодяг",                                        "safety"),
    ("проблискові маячки",                              "alarm"),
    ("спецсигнали",                                     "alarm"),
    ("сирени",                                          "alarm"),
    ("дорожні огорожі",                                 "safety"),

    # ── Outdoor / Camping ────────────────────────────────────────────────────
    ("дощовики",                                        "military_outdoor"),
    ("туристичні плити",                                "outdoor"),
    ("пальники",                                         "outdoor"),
    ("каремати",                                         "outdoor"),
    ("сидушки",                                          "outdoor"),
    ("туристичні фляги",                                 "outdoor"),
    ("ручні та налобні ліхтарі",                         "outdoor"),
    ("ножі для полювання",                               "outdoor"),
    ("туризм",                                          "outdoor"),
    ("туристичн",                                       "outdoor"),
    ("ліхтар",                                          "outdoor"),
    ("каремат",                                         "outdoor"),

    # ── Garden tools ──────────────────────────────────────────────────────────
    ("садові пилосмокти",                               "tool_garden"),
    ("пилосмокти, повітродувки",                        "tool_garden"),
    ("газонокосарки",                                   "tool_garden"),
    ("мотокоси",                                         "tool_garden"),
    ("тримери",                                          "tool_garden"),
    ("інструменти для обробки грунту",                  "tool_garden"),
    ("інструменти для обрізки",                         "tool_garden"),
    ("вила",                                             "tool_garden"),
    ("граблі",                                           "tool_garden"),
    ("лопати",                                           "tool_garden"),
    ("мотики",                                           "tool_garden"),
    ("сапи",                                             "tool_garden"),
    ("сапки",                                            "tool_garden"),
    ("набори садових інструментів",                     "tool_garden"),
    ("посадочні совки",                                  "tool_garden"),
    ("ручні культиватори",                               "tool_garden"),
    ("кущорізи",                                         "tool_garden"),
    ("пили для обрізки гілок",                           "tool_garden"),
    ("садові ножиці",                                    "tool_garden"),
    ("секатори",                                         "tool_garden"),
    ("садовий інвентар",                                 "tool_garden"),
    ("апарати високого тиску",                           "tool_garden"),

    # ── Ladders / scaffolding ─────────────────────────────────────────────────
    ("будівельні та садові драбини",                     "tool_ladders"),
    ("комплектуючі для сходів",                          "tool_ladders"),
    ("будівельних риштувань",                            "tool_ladders"),
    ("драбин",                                           "tool_ladders"),
    ("сходи",                                            "tool_ladders"),
    ("риштування",                                       "tool_ladders"),

    # ── Auto tools / vehicle safety ───────────────────────────────────────────
    ("автомобільні насоси",                              "tool_auto"),
    ("компресори та манометри",                          "tool_auto"),
    ("набори і аксесуари для автомобіліста",             "tool_auto"),

    # ── Kitchenware ───────────────────────────────────────────────────────────
    ("кухонні ножі",                                     "home"),
    ("точила для ножів",                                 "home"),

    # ── Optics ────────────────────────────────────────────────────────────────
    ("комплектуючі для оптичних приладів",               "military_optics"),
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
    reclassify_all: bool = False,
) -> None:
    """
    Дописує нові product_type рядки в кінець файлу.
    --reclassify: оновлює тільки theme == 'other'. schedule НЕ чіпає.
    --reclassify-all: оновлює всі theme за поточними правилами. schedule НЕ чіпає.
    """
    existing_rows, known = load_rules(path)

    reclassified = 0
    if reclassify or reclassify_all:
        for row in existing_rows:
            current_theme = row.get("theme", FALLBACK_THEME) or FALLBACK_THEME
            if not reclassify_all and current_theme != FALLBACK_THEME:
                continue

            theme = classify_theme(row["keyword"])
            if theme == FALLBACK_THEME or theme == current_theme:
                continue

            row["theme"] = theme
            log.info(
                "RULE RECLASSIFY  theme=%-20s  old=%-20s  kw=%s",
                theme, current_theme, row["keyword"][:70],
            )
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

    if reclassify or reclassify_all or not path.exists():
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

def run(
    *,
    dry_run: bool = False,
    reclassify: bool = False,
    reclassify_all: bool = False,
) -> None:
    items = parse_xml(FEED_URL_MERCHANT)
    update_rules_csv(
        RULES_CSV,
        items,
        dry_run=dry_run,
        reclassify=reclassify,
        reclassify_all=reclassify_all,
    )


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
    p.add_argument(
        "--reclassify-all", action="store_true",
        help=(
            "Перекласифікує theme у всіх існуючих рядках за поточними правилами. "
            "schedule залишається незмінним."
        ),
    )
    return p.parse_args()


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(levelname)-8s %(message)s")
    args = _parse_args()
    run(
        dry_run=args.dry_run,
        reclassify=args.reclassify,
        reclassify_all=args.reclassify_all,
    )


if __name__ == "__main__":
    main()
