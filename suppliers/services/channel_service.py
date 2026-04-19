"""
Сервіс для роботи з мультиканальними категоріями.
Підтримує site, prom, rozetka та інші канали продажу.
"""
import csv
from pathlib import Path
from typing import List, Dict, Optional
from decimal import Decimal

from suppliers.services.dealer_price_service import DEFAULT_COEF_RETAIL, DEFAULT_COEF_DEALER


class ChannelConfig:
    """Конфігурація одного каналу продажу"""

    def __init__(
        self,
        channel: str,
        prefix: str,
        coefficient: Decimal,
        coefficient_feed: Decimal,
        coef_retail: Decimal,
        coef_dealer: Decimal,
        group_number: str,
        group_name: str,
        subdivision_id: str,
        subdivision_link: str,
        personal_notes: str,
        label: str,
        feed: str = "",  # порожній = підходить для всіх фідів
    ):
        self.channel = channel
        self.prefix = prefix
        self.coefficient = coefficient
        self.coefficient_feed = coefficient_feed
        # Нові коефіцієнти для viatec dealer pricing
        self.coef_retail = coef_retail
        self.coef_dealer = coef_dealer
        self.group_number = group_number
        self.group_name = group_name
        self.subdivision_id = subdivision_id
        self.subdivision_link = subdivision_link
        self.personal_notes = personal_notes
        self.label = label
        self.feed = feed

    def __repr__(self):
        return (
            f"ChannelConfig(channel={self.channel}, "
            f"coef_retail={self.coef_retail}, coef_dealer={self.coef_dealer}, "
            f"coef={self.coefficient}, coef_feed={self.coefficient_feed}, "
            f"prefix={self.prefix})"
        )


class ChannelService:
    """Сервіс для роботи з мультиканальними категоріями."""

    def __init__(self, category_file: Path, logger=None, decimal_places: int = 0):
        """
        Args:
            category_file: Шлях до CSV з категоріями та каналами.
            logger: Scrapy logger.
            decimal_places: Кількість знаків після коми для цін (0 = цілі).
        """
        self.logger = logger
        self.decimal_places = decimal_places

        # Основний індекс: Линк категории поставщика → [ChannelConfig, ...]
        self.category_channels: Dict[str, List[ChannelConfig]] = {}

        # Додатковий індекс для фідів: category id → [ChannelConfig, ...]
        # Використовується коли URL категорії порожній (є тільки в фіді, не на сайті)
        self.category_id_channels: Dict[str, List[ChannelConfig]] = {}

        self.is_multi_channel = False

        if category_file and category_file.exists():
            self._load_channels(category_file)

    # ------------------------------------------------------------------
    # LOADING
    # ------------------------------------------------------------------

    def _load_channels(self, filepath: Path) -> None:
        """Завантажує канали з CSV, індексує за URL та за category id."""
        try:
            with open(filepath, encoding="utf-8-sig") as f:
                reader = csv.DictReader(f, delimiter=";")

                if "channel" not in (reader.fieldnames or []):
                    if self.logger:
                        self.logger.info(
                            f"📋 Файл {filepath.name} не має колонки 'channel' — звичайний режим"
                        )
                    return

                self.is_multi_channel = True

                for row in reader:
                    category_url = row.get("Линк категории поставщика", "").strip().strip('"')
                    channel = row.get("channel", "").strip()
                    category_id = row.get("category id", "").strip()

                    if not channel:
                        continue

                    channel_config = self._build_channel_config(row)

                    # Індекс за URL (для retail-пауків)
                    if category_url:
                        self.category_channels.setdefault(category_url, []).append(channel_config)

                    # Індекс за category id (для feed-пауків, в т.ч. категорій без URL)
                    if category_id:
                        self.category_id_channels.setdefault(category_id, []).append(channel_config)

            if self.logger:
                total_by_url = sum(len(v) for v in self.category_channels.values())
                total_by_id = sum(len(v) for v in self.category_id_channels.values())
                self.logger.info(
                    f"✅ Завантажено {len(self.category_channels)} URL-категорій "
                    f"з {total_by_url} каналами; "
                    f"{len(self.category_id_channels)} id-категорій з {total_by_id} каналами "
                    f"(multi-channel mode)"
                )

        except Exception as e:
            if self.logger:
                self.logger.error(f"❌ Помилка завантаження каналів: {e}")

    def _build_channel_config(self, row: dict) -> "ChannelConfig":
        """Будує ChannelConfig з рядка CSV."""
        # Зворотня сумісність: legacy coefficient/coefficient_feed
        coefficient = self._parse_decimal(row.get("coefficient", "1.0"), row)
        coefficient_feed = self._parse_decimal(row.get("coefficient_feed", "1.0"), row)

        # Нові коефіцієнти для dealer-пайплайну
        coef_retail = self._parse_decimal(
            row.get("coef_retail", ""), row, fallback=DEFAULT_COEF_RETAIL
        )
        coef_dealer = self._parse_decimal(
            row.get("coef_dealer", ""), row, fallback=DEFAULT_COEF_DEALER
        )

        return ChannelConfig(
            channel=row.get("channel", "").strip(),
            prefix=row.get("prefix", "").strip(),
            coefficient=coefficient,
            coefficient_feed=coefficient_feed,
            coef_retail=coef_retail,
            coef_dealer=coef_dealer,
            group_number=row.get("Номер_групи", "").strip(),
            group_name=row.get("Назва_групи", "").strip(),
            subdivision_id=row.get("Ідентифікатор_підрозділу", "").strip(),
            subdivision_link=row.get("Посилання_підрозділу", "").strip(),
            personal_notes=row.get("Особисті_нотатки", "").strip(),
            label=row.get("Ярлик", "").strip(),
            feed=row.get("feed", "").strip(),
        )

    def _parse_decimal(
        self,
        raw: str,
        row: dict,
        fallback: Decimal | None = None,
    ) -> Decimal:
        """Безпечний парсинг Decimal з CSV рядка."""
        if fallback is None:
            fallback = Decimal("1.0")
        clean = raw.strip().strip('"').replace(",", ".")
        try:
            return Decimal(clean) if clean else fallback
        except Exception:
            if self.logger:
                self.logger.warning(f"⚠️ Некоректний коефіцієнт {raw!r} у рядку: {row}")
            return fallback

    # ------------------------------------------------------------------
    # LOOKUPS
    # ------------------------------------------------------------------

    def get_channels(self, category_url: str) -> List[ChannelConfig]:
        """Повертає канали за URL категорії (для retail-пауків)."""
        return self.category_channels.get(category_url, [])

    def get_channels_by_id(self, category_id: str, feed_id: str = "") -> List[ChannelConfig]:
        """Повертає канали за category id, з фільтрацією по feed_id.

        Якщо feed_id порожній — повертаються всі канали (legacy).
        Якщо feed_id задано — повертаються канали де channel.feed == feed_id
        або channel.feed == "" (універсальні, не прив'язані до фіду).
        """
        all_channels = self.category_id_channels.get(str(category_id).strip(), [])
        if not feed_id:
            return all_channels
        return [c for c in all_channels if not c.feed or c.feed == feed_id]

    def resolve_channels(self, category_url: str, category_id: str = "", feed_id: str = "") -> List[ChannelConfig]:
        """
        Повертає канали: спочатку шукає за URL, потім за category id.

        feed_id фільтрує канали для категорій в декількох фідах
        (напр. 25, 13, 621 у фідах 50 і 52): повертаються тільки
        канали поточного фіду, а не обох.
        """
        channels = self.get_channels(category_url) if category_url else []
        if not channels and category_id:
            channels = self.get_channels_by_id(category_id, feed_id)
        return channels

    # ------------------------------------------------------------------
    # PRICE
    # ------------------------------------------------------------------

    def apply_price_coefficient(self, base_price: str, coefficient: Decimal) -> str:
        """
        Застосовує коефіцієнт до ціни.

        Args:
            base_price: Базова ціна (може містити коми, пробіли).
            coefficient: Коефіцієнт для множення.

        Returns:
            Ціна після множення та округлення.
        """
        try:
            clean_price = str(base_price).replace(",", ".").replace(" ", "").strip()
            price_decimal = Decimal(clean_price)
            new_price = price_decimal * coefficient

            if self.decimal_places == 0:
                return str(int(new_price.quantize(Decimal("1"))))
            else:
                fmt = f"0.{'0' * self.decimal_places}"
                return str(new_price.quantize(Decimal(fmt)))

        except Exception as e:
            if self.logger:
                self.logger.warning(
                    f"⚠️ Помилка застосування коефіцієнта до ціни {base_price}: {e}"
                )
            return base_price
