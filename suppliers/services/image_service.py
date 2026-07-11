"""
Сервіс трансформації зображень постачальників.

Алгоритм для кожного URL:
    1. Перевіряємо кеш → повертаємо збережений результат.
    2. Якщо немає — читаємо тільки заголовок файлу (512 Б – 64 КБ),
       щоб отримати розміри та наявність альфа-каналу без завантаження
       всього зображення.
    3. Зберігаємо (w, h, transform_url | None) в кеш.
    4. has_alpha (реальна прозорість — фінальний колір фону залежить
       від дефолту конкретного споживача, непередбачувано) АБО
       min(w, h) < SMALL_THRESHOLD → повертаємо wsrv.nl URL з
       примусовим flatten на білий фон, інакше — оригінальний URL.

Будь-яка помилка (таймаут, 404, невідомий формат) →
    safe fallback: повертаємо оригінальний URL, не падаємо.

Кеш: data/{supplier}/image_cache.json  (per-supplier, як sku_map.json)
    {original_url: {"w": int|null, "h": int|null, "transform_url": str|null}}
"""
from __future__ import annotations

import json
import urllib.parse
from io import BytesIO
from pathlib import Path
from typing import Final

import requests
from PIL import Image

# ---------------------------------------------------------------------------
# Конфігурація
# ---------------------------------------------------------------------------

# Поріг: якщо min(w, h) < SMALL_THRESHOLD → трансформуємо
SMALL_THRESHOLD: Final[int] = 520

# Цільові розміри трансформації
TRANSFORM_W: Final[int] = 640
TRANSFORM_H: Final[int] = 640

# wsrv.nl — безкоштовний open-source image proxy (github.com/weserv/images)
_WSRV_BASE: Final[str] = "https://wsrv.nl/"
_WSRV_STATIC_PARAMS: Final[dict[str, str]] = {
    "w":      str(TRANSFORM_W),
    "h":      str(TRANSFORM_H),
    "fit":    "contain",     # вписати в бокс зі збереженням пропорцій
    "bg":     "white",       # flatten альфа-каналу (PNG/WebP з прозорістю) → білий
    "cbg":    "white",       # фон letterbox-зон саме для fit=contain (офіційний параметр)
    "output": "jpg",         # jpg не зберігає альфу → flatten гарантований
    "q":      "90",
}

# HTTP
_MIN_CHUNK: Final[int]        = 512       # початковий chunk для PNG (заголовок ~24 Б)
_MAX_HEADER_BYTES: Final[int] = 65_536   # максимум для JPEG (SOF маркер)
_REQUEST_TIMEOUT: Final[int]  = 10       # секунд


class ImageService:
    """
    Перевіряє розмір зображення мінімальним HTTP-запитом і за потреби
    повертає wsrv.nl URL замість оригінального.

    Один екземпляр на постачальника (per-supplier, як SkuCodeService).
    Кеш зберігається на диск у close_spider() через pipeline.
    """

    def __init__(self, cache_path: Path, logger=None) -> None:
        self._cache_path = cache_path
        self._logger = logger
        self._cache: dict[str, dict] = self._load_cache()
        self._dirty = False

    # ------------------------------------------------------------------ #
    # Публічне API
    # ------------------------------------------------------------------ #

    def resolve_url(self, url: str) -> str:
        """
        Повертає фінальний URL(и) для поля Посилання_зображення в Prom CSV.

        Приймає як один URL, так і кілька URL розділених комою (Prom-формат).
        Кома в URL небуває рав — вона завжди percent-encoded (%2C), тому спліт по "," безпечний.

        При будь-якій помилці — safe fallback: повертає оригінальний URL.
        """
        if not url:
            return url
        parts = [u.strip() for u in url.split(",") if u.strip()]
        if len(parts) == 1:
            return self._resolve_single(parts[0])
        resolved = [self._resolve_single(u) for u in parts]
        return ", ".join(resolved)

    def _resolve_single(self, url: str) -> str:
        """
        Обробляє один URL: кеш → HTTP-читання розміру/альфи → рішення.
        Не викликати зовні.
        """
        if url.startswith(_WSRV_BASE):
            return url

        cached = self._cache.get(url)
        if cached is not None:
            return cached.get("transform_url") or url

        info = self.get_image_info(url)

        if info is None:
            self._put_cache(url, None, None, None)
            return url

        w, h, has_alpha = info

        if has_alpha or self.is_small(w, h):
            transform_url = self.build_transform_url(url)
            self._put_cache(url, w, h, transform_url)
            if self._logger:
                reason = "альфа-канал" if has_alpha else f"{w}×{h} < {SMALL_THRESHOLD}"
                self._logger.info(
                    f"🖼️  ImageService: {reason} → wsrv.nl | {url[:70]}"
                )
            return transform_url

        self._put_cache(url, w, h, None)
        return url

    def get_size(self, url: str) -> tuple[int, int] | None:
        """Backward-compatible: тільки (width, height). Див. get_image_info()."""
        info = self.get_image_info(url)
        return (info[0], info[1]) if info else None

    def get_image_info(self, url: str) -> tuple[int, int, bool] | None:
        """
        Читає мінімум байт для (width, height, has_alpha).

        PNG:  ~24 Б (IHDR chunk)
        JPEG: до ~64 КБ (до SOF маркера)
        WebP: ~30 Б

        has_alpha=True означає, що зображення має реальний альфа-канал
        (PNG/WebP з прозорістю). Фінальний колір фону при конвертації в
        JPEG у такому разі залежить від дефолту КОНКРЕТНОГО споживача
        (Prom / Kasta / Rozetka / браузер) — тобто непередбачуваний,
        доки ми не зафлетенимо його самі.

        Returns:
            (width, height, has_alpha) або None при помилці / невідомому форматі.
        """
        try:
            with requests.get(url, stream=True, timeout=_REQUEST_TIMEOUT) as r:
                r.raise_for_status()
                buf = b""
                for chunk in r.iter_content(_MIN_CHUNK):
                    buf += chunk
                    try:
                        img = Image.open(BytesIO(buf))
                        has_alpha = img.mode in ("RGBA", "LA") or (
                            img.mode == "P" and "transparency" in img.info
                        )
                        return (*img.size, has_alpha)
                    except Exception:
                        pass
                    if len(buf) >= _MAX_HEADER_BYTES:
                        return None
        except Exception as exc:
            if self._logger:
                self._logger.warning(
                    f"⚠️  ImageService.get_image_info: {exc} | {url[:70]}"
                )
        return None

    @staticmethod
    def is_small(w: int, h: int) -> bool:
        """True якщо найменша сторона менша за SMALL_THRESHOLD (520 px)."""
        return min(w, h) < SMALL_THRESHOLD

    @staticmethod
    def build_transform_url(original_url: str) -> str:
        """
        Будує wsrv.nl URL для трансформації до 640×640.

        Args:
            original_url: URL зображення постачальника.

        Returns:
            https://wsrv.nl/?url=<encoded>&w=640&h=640&fit=contain&...
        """
        params: dict[str, str] = {"url": original_url, **_WSRV_STATIC_PARAMS}
        return _WSRV_BASE + "?" + urllib.parse.urlencode(params, quote_via=urllib.parse.quote)

    def save_cache(self) -> None:
        """Зберігає кеш на диск. Викликати в close_spider()."""
        if not self._dirty:
            return
        try:
            self._cache_path.parent.mkdir(parents=True, exist_ok=True)
            self._cache_path.write_text(
                json.dumps(self._cache, ensure_ascii=False, indent=2),
                encoding="utf-8",
            )
            if self._logger:
                self._logger.info(
                    f"💾 ImageService: кеш збережено ({len(self._cache)} URL) → {self._cache_path}"
                )
        except Exception as exc:
            if self._logger:
                self._logger.warning(f"⚠️  ImageService.save_cache: {exc}")

    # ------------------------------------------------------------------ #
    # Приватне
    # ------------------------------------------------------------------ #

    def _load_cache(self) -> dict[str, dict]:
        if not self._cache_path.exists():
            return {}
        try:
            return json.loads(self._cache_path.read_text(encoding="utf-8"))
        except Exception:
            return {}

    def _put_cache(
        self,
        url: str,
        w: int | None,
        h: int | None,
        transform_url: str | None,
    ) -> None:
        self._cache[url] = {"w": w, "h": h, "transform_url": transform_url}
        self._dirty = True
