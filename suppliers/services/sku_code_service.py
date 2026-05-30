"""
Персистентний сервіс маппінгу SKU → Код_товару.

ФАЙЛ МАППІНГУ: data/{supplier}/sku_map.json
ФОРМАТ: {"99-00020130": 200001, "10000000824": 200002, ...}

Особливості:
- Атомарна запис через .tmp → replace (захист від битого JSON при падінні)
- Автозбереження кожні AUTOSAVE_EVERY нових SKU
- start_code ініціалізується до _load() (коректний порядок)
- Guard на порожній SKU
"""

import json
import logging
from pathlib import Path


class SkuCodeService:
    """Маппінг SKU постачальника → наш внутрішній Код_товару."""

    AUTOSAVE_EVERY = 10
    META_PREFIX = "__meta__"

    def __init__(self, map_file: Path, start_code: int = 200001, logger=None):
        self._map_file = map_file
        self._logger = logger or logging.getLogger(__name__)
        self._start_code = int(start_code)  # ініціалізуємо ДО _load()

        self._map: dict[str, int] = {}
        self._meta: dict[str, str] = {}
        self._load()  # заповнює _map і _meta
        self._dirty = False
        self._new_since_save = 0

    # ------------------------------------------------------------------ #
    # PUBLIC API
    # ------------------------------------------------------------------ #

    def get_or_create(self, supplier_sku: str) -> int:
        """
        Повертає Код_товару для SKU постачальника.
        Якщо SKU новий — призначає наступний вільний код.
        """
        supplier_sku = (supplier_sku or "").strip()
        if not supplier_sku:
            raise ValueError("supplier_sku is empty")

        existing = self._map.get(supplier_sku)
        if existing is not None:
            return existing

        next_code = self._next_free_code()
        self._map[supplier_sku] = next_code
        self._dirty = True
        self._new_since_save += 1

        self._logger.info(f"🆕 Новий SKU [{supplier_sku}] → Код {next_code}")

        # Автозбереження кожні AUTOSAVE_EVERY нових SKU
        if self._new_since_save >= self.AUTOSAVE_EVERY:
            self.save()

        return next_code

    def get_meta(self, key: str) -> str:
        """Повертає мета-значення за ключем або порожній рядок."""
        return self._meta.get(key, "")

    def set_meta(self, key: str, value: str) -> None:
        """Зберігає мета-значення. Буде записано при наступному save()."""
        self._meta[key] = str(value)
        self._dirty = True

    def save(self) -> None:
        """
        Атомарне збереження на диск: пише в .tmp → робить replace.
        Гарантує що sku_map.json ніколи не буде в битому стані.
        """
        if not self._dirty:
            return

        self._map_file.parent.mkdir(parents=True, exist_ok=True)

        tmp_file = self._map_file.with_suffix(self._map_file.suffix + ".tmp")
        payload: dict = {str(k): int(v) for k, v in self._map.items()}
        for mk, mv in self._meta.items():
            payload[f"{self.META_PREFIX}{mk}"] = mv

        with open(tmp_file, "w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False, indent=2)

        tmp_file.replace(self._map_file)  # атомарна операція на рівні ОС

        self._logger.info(
            f"💾 sku_map збережено: {len(self._map)} записів → {self._map_file.name}"
        )
        self._dirty = False
        self._new_since_save = 0

    @property
    def total_mapped(self) -> int:
        return len(self._map)

    # ------------------------------------------------------------------ #
    # PRIVATE
    # ------------------------------------------------------------------ #

    def _load(self) -> None:
        """Завантажує SKU → Код у self._map та мета-дані у self._meta."""
        if not self._map_file.exists():
            self._logger.info(
                f"📋 sku_map не знайдено ({self._map_file.name}), починаємо новий"
            )
            return

        try:
            with open(self._map_file, encoding="utf-8") as f:
                data = json.load(f)
            for k, v in data.items():
                sk = str(k)
                if sk.startswith(self.META_PREFIX):
                    self._meta[sk[len(self.META_PREFIX):]] = str(v)
                else:
                    try:
                        self._map[sk] = int(v)
                    except (ValueError, TypeError):
                        pass
            self._logger.info(
                f"✅ sku_map завантажено: {len(self._map)} записів з {self._map_file.name}"
            )
        except Exception as e:
            self._logger.error(
                f"❌ Помилка читання sku_map ({self._map_file.name}): {e}. Починаємо новий."
            )

    def _next_free_code(self) -> int:
        """Наступний вільний код: max існуючих + 1, або start_code."""
        if not self._map:
            return self._start_code
        return max(self._map.values()) + 1
