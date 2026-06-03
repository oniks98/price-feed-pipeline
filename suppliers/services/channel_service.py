"""
Сервіс для роботи з мультиканальними категоріями.
Підтримує site, prom, rozetka та інші канали продажу.
"""
import csv
from pathlib import Path
from typing import List, Dict, Optional
from decimal import Decimal


class ChannelConfig:
    """Конфігурація одного каналу продажу."""

    def __init__(
        self,
        channel: str,
        prefix: str,
        coefficient: Decimal,
        coefficient_feed: Decimal,
        coef: Decimal,
        threshold: Decimal,
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
        # Dealer pricing: coef і threshold з CSV (рядок каналу)
        self.coef = coef           # множник до retail  (напр. 0.95)
        self.threshold = threshold  # мін. наценка на dealer (напр. 1.3)
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
            f"coef={self.coef}, threshold={self.threshold}, "
            f"coef_legacy={self.coefficient}, coef_feed={self.coefficient_feed}, "
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
                    if channel_config is None:
                        # Помилка вже залогована в _build_channel_config
                        continue

                    # Індекс за URL (для retail-пауків)
                    if category_url:
                        self.category_channels.setdefault(category_url, []).append(channel_config)

                    # Індекс за category id (для feed-пауків)
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

    def _build_channel_config(self, row: dict) -> "ChannelConfig | None":
        """
        Будує ChannelConfig з рядка CSV.

        Повертає None якщо coef або threshold відсутні або некоректні
        (рядок буде пропущено при завантаженні).
        """
        channel = row.get("channel", "?").strip()
        url_short = row.get("Линк категории поставщика", "")[:60]

        # Строки-маркери з subdivision_id="delete" — спеціальні рядки для фільтрації
        # категорій (використовуються пауком безпосередньо). Цінова конфігурація ім не потрібна.
        if row.get("Ідентифікатор_підрозділу", "").strip().lower() == "delete":
            return None

        # ── Обов'язкові поля для dealer pricing ────────────────────────
        coef_raw = row.get("coef", "").strip().strip('"')
        threshold_raw = row.get("threshold", "").strip().strip('"')

        if not coef_raw or not threshold_raw:
            if self.logger:
                self.logger.error(
                    f"❌ CSV: відсутній coef або threshold | "
                    f"channel={channel!r} url={url_short!r} — рядок пропущено"
                )
            return None

        coef = self._parse_decimal_strict(coef_raw, "coef", channel, url_short)
        threshold = self._parse_decimal_strict(threshold_raw, "threshold", channel, url_short)
        if coef is None or threshold is None:
            return None

        # ── Legacy коефіцієнти (зворотна сумісність) ───────────────────
        coefficient = self._parse_decimal_safe(row.get("coefficient", "1.0"))
        coefficient_feed = self._parse_decimal_safe(row.get("coefficient_feed", "1.0"))

        return ChannelConfig(
            channel=channel,
            prefix=row.get("prefix", "").strip(),
            coefficient=coefficient,
            coefficient_feed=coefficient_feed,
            coef=coef,
            threshold=threshold,
            group_number=row.get("Номер_групи", "").strip(),
            group_name=row.get("Назва_групи", "").strip(),
            subdivision_id=row.get("Ідентифікатор_підрозділу", "").strip(),
            subdivision_link=row.get("Посилання_підрозділу", "").strip(),
            personal_notes=row.get("Особисті_нотатки", "").strip(),
            label=row.get("Ярлик", "").strip(),
            feed=row.get("feed", "").strip(),
        )

    def _parse_decimal_strict(
        self,
        raw: str,
        field_name: str,
        channel: str,
        url_short: str,
    ) -> Decimal | None:
        """
        Парсинг Decimal з CSV; повертає None і логує error при помилці.
        Використовується для обов'язкових полів (coef, threshold).
        """
        clean = raw.strip().strip('"').replace(",", ".")
        try:
            return Decimal(clean) if clean else None
        except Exception:
            if self.logger:
                self.logger.error(
                    f"❌ CSV: некоректний {field_name}={raw!r} | "
                    f"channel={channel!r} url={url_short!r} — рядок пропущено"
                )
            return None

    def _parse_decimal_safe(self, raw: str, fallback: Decimal = Decimal("1.0")) -> Decimal:
        """Безпечний парсинг Decimal з CSV; повертає fallback при помилці."""
        clean = raw.strip().strip('"').replace(",", ".")
        try:
            return Decimal(clean) if clean else fallback
        except Exception:
            return fallback

    def _parse_decimal(
        self,
        raw: str,
        row: dict,
        fallback: Decimal | None = None,
    ) -> Decimal:
        """Legacy-метод для зворотної сумісності."""
        if fallback is None:
            fallback = Decimal("1.0")
        return self._parse_decimal_safe(raw, fallback)

    # ------------------------------------------------------------------
    # LOOKUPS
    # ------------------------------------------------------------------

    def get_channels(self, category_url: str) -> List[ChannelConfig]:
        """Повертає канали за URL категорії (для retail-пауків)."""
        return self.category_channels.get(category_url, [])

    def get_channels_by_id(self, category_id: str, feed_id: str = "") -> List[ChannelConfig]:
        """Повертає канали за category id, з фільтрацією по feed_id."""
        all_channels = self.category_id_channels.get(str(category_id).strip(), [])
        if not feed_id:
            return all_channels
        return [c for c in all_channels if not c.feed or c.feed == feed_id]

    def resolve_channels(self, category_url: str, category_id: str = "", feed_id: str = "") -> List[ChannelConfig]:
        """
        Повертає канали: спочатку шукає за URL, потім за category id.
        """
        channels = self.get_channels(category_url) if category_url else []
        if not channels and category_id:
            channels = self.get_channels_by_id(category_id, feed_id)
        return channels

    # ------------------------------------------------------------------
    # PRICE (legacy coefficient mode)
    # ------------------------------------------------------------------

    def apply_price_coefficient(self, base_price: str, coefficient: Decimal) -> str:
        """
        Застосовує коефіцієнт до ціни (legacy-режим для non-dealer пауків).
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
