"""
Скрипт : python scripts/epicenter_import_feed.py

Автоматизує запуск імпорту XML-фіду товарів в Epicentre Admin через Playwright.

Флоу:
  1. Авторизація (EPICENTER_EMAIL / EPICENTER_PASSWORD з .env або environment)
  2. Sidebar: Товари → Імпорт
  3. Якщо попередній імпорт ще виконується (буває, що Epicenter обробляє його
     понад 2 год) — чекаємо звільнення в межах бюджету job, інакше пропускаємо
     цей запуск (наступний за розкладом спробує знову). Перевірка робиться
     двошарово: (a) poll-перевірка одразу після навігації (захист від
     асинхронного рендеру віджета статусу), (b) fallback у _submit_import,
     якщо кнопка «Імпортувати» лишилась disabled попри пройдену перевірку (a)
  4. Вибір джерела: Посилання
  5. Вибір режиму:  Оновити все
  6. Введення URL фіду → клік «Імпортувати»
  7. Перевірка статусу через 2 с

Вихід:
  exit 0 — «Імпорт товарів успішно запущений» знайдено АБО попередній імпорт
           ще виконується (пропущено, не помилка автоматизації)
  exit 1 — тег підтвердження не знайдено або виникла помилка

Запуск:
  python scripts/epicenter_import_feed.py
  python scripts/epicenter_import_feed.py --headed   # видимий браузер (debug)
"""
from __future__ import annotations

import argparse
import logging
import os
import sys
from pathlib import Path

from playwright.sync_api import Page, TimeoutError as PWTimeout, sync_playwright

# ---------------------------------------------------------------------------
# Конфігурація
# ---------------------------------------------------------------------------

LOGIN_URL  = (
    "https://admin.epicentrm.com.ua/auth/login"
    "?redirectUrl=%2Fpim%2Fproduct-import"
)
IMPORT_URL = "https://admin.epicentrm.com.ua/pim/product-import"
FEED_URL   = (
    "https://raw.githubusercontent.com/oniks98/price-feed-pipeline"
    "/data-latest/data/markets/epicenter_feed.xml"
)

PAGE_TIMEOUT_MS  = 30_000   # загальний таймаут Playwright
NAV_TIMEOUT_MS   = 15_000   # таймаут навігаційних переходів
POST_IMPORT_MS   = 2_000    # пауза після кліку «Імпортувати» перед перевіркою
SUCCESS_WAIT_MS  = 5_000    # додатковий таймаут очікування тегу підтвердження

# Epicenter інколи обробляє попередній імпорт понад 2 год — віджет статусу
# лишається на сторінці й блокує кнопку «Імпортувати». Чекаємо звільнення
# обмежений час (бюджет job у pipeline.yml: timeout-minutes: 15), інакше
# пропускаємо запуск і покладаємось на наступний за розкладом.
IMPORT_BUSY_MAX_WAIT_MS = int(
    os.environ.get("EPICENTER_IMPORT_BUSY_WAIT_MS", 600_000)  # 10 хв за замовчуванням
)

# Віджет статусу підвантажується Angular-ом асинхронно (окремий запит на
# ngOnInit), тоді як статичні поля форми (radio "Посилання") рендеряться
# одразу. Одноразова синхронна перевірка одразу після навігації може дати
# false-negative — тому перевіряємо наявність кілька разів з паузою, перш
# ніж вважати сторінку вільною.
BUSY_CHECK_POLL_ATTEMPTS     = 3
BUSY_CHECK_POLL_INTERVAL_MS  = 1_000

# Селектори — стабільні атрибути, незалежні від динамічних Angular ID
SEL_EMAIL_INPUT    = 'input[placeholder="E-mail або номер телефону"]'
SEL_PASSWORD_INPUT = 'input[placeholder="Пароль"]'
SEL_SUBMIT_BTN     = 'button[type="submit"]'
SEL_TOVARY_LINK    = 'a[href="/pim/product"]'
SEL_IMPORT_LINK    = 'a[href="/pim/product-import"]'
SEL_RADIO_LINK     = 'input[value="link"]'
SEL_RADIO_FULL     = 'input[value="full"]'
SEL_FEED_URL_INPUT = 'input[formcontrolname="url"]'
SEL_IMPORT_BTN     = 'button.import-button'
# Той самий елемент означає і "процес щойно запущено" (після нашого кліку),
# і "процес досі виконується" (якщо вже висить при заході на сторінку) —
# Epicenter не прибирає його, поки імпорт не завершиться.
SEL_IMPORT_STATUS  = "em-products-import-status-process"

# ---------------------------------------------------------------------------
# Логування
# ---------------------------------------------------------------------------

logging.basicConfig(
    format="%(asctime)s [%(levelname)s] %(message)s",
    level=logging.INFO,
    stream=sys.stdout,
)
log = logging.getLogger(__name__)


class ImportStillBusy(Exception):
    """
    Попередній імпорт досі виконується — виявлено вже під час сабміту
    (кнопка «Імпортувати» лишилась disabled). Це не помилка автоматизації,
    а сигнал для graceful skip у main().
    """


# ---------------------------------------------------------------------------
# Credentials
# ---------------------------------------------------------------------------

# .env живе в suppliers/ (поряд з viatec-кредами та іншими)
_ENV_PATH: Path = Path(__file__).resolve().parents[1] / "suppliers" / ".env"


def _load_dotenv() -> None:
    """Завантажує suppliers/.env через python-dotenv."""
    log.info(".env: %s | exists=%s", _ENV_PATH, _ENV_PATH.exists())
    if not _ENV_PATH.exists():
        log.warning(".env не знайдено — використовуємо лише os.environ")
        return
    try:
        from dotenv import load_dotenv  # type: ignore[import-untyped]
        loaded = load_dotenv(_ENV_PATH, override=True)
        log.info("load_dotenv → %s", loaded)
    except ImportError:
        log.warning("python-dotenv не встановлено: pip install python-dotenv")


def _get_credentials() -> tuple[str, str]:
    _load_dotenv()
    email    = os.environ.get("EPICENTER_EMAIL", "").strip()
    password = os.environ.get("EPICENTER_PASSWORD", "").strip()
    if not email or not password:
        log.error(
            "EPICENTER_EMAIL та/або EPICENTER_PASSWORD не задані. "
            "Перевірте .env або змінні середовища."
        )
        sys.exit(1)
    return email, password


# ---------------------------------------------------------------------------
# Playwright-кроки
# ---------------------------------------------------------------------------

def _login(page: Page, email: str, password: str) -> None:
    """Авторизація: заповнення форми + клік «Увійти»."""
    log.info("Перехід на сторінку авторизації")
    page.goto(LOGIN_URL, timeout=PAGE_TIMEOUT_MS, wait_until="domcontentloaded")

    email_input = page.locator(SEL_EMAIL_INPUT)
    email_input.wait_for(state="visible", timeout=PAGE_TIMEOUT_MS)
    email_input.fill(email)
    log.info("Email введено")

    password_input = page.locator(SEL_PASSWORD_INPUT)
    password_input.wait_for(state="visible", timeout=PAGE_TIMEOUT_MS)
    password_input.fill(password)
    log.info("Пароль введено")

    submit_btn = page.locator(SEL_SUBMIT_BTN)
    submit_btn.wait_for(state="visible", timeout=PAGE_TIMEOUT_MS)
    submit_btn.click()
    log.info("Клік «Увійти»")

    # Чекаємо виходу зі сторінки логіну
    page.wait_for_url(
        lambda url: "/auth/login" not in url,
        timeout=PAGE_TIMEOUT_MS,
    )
    if "/auth/login" in page.url:
        raise RuntimeError(
            "Авторизація не вдалася — перевірте EPICENTER_EMAIL / EPICENTER_PASSWORD"
        )
    log.info("Авторизація успішна — URL: %s", page.url)


def _wait_import_form_ready(page: Page) -> None:
    """Чекаємо поки форма імпорту відрендериться (наявність radio «Посилання»)."""
    page.locator(SEL_RADIO_LINK).wait_for(state="attached", timeout=PAGE_TIMEOUT_MS)


def _navigate_to_import(page: Page) -> None:
    """
    Навігація через sidebar: Товари → Імпорт.
    Якщо redirectUrl спрацював і ми вже на /pim/product-import — пропускаємо sidebar.
    """
    if "/pim/product-import" in page.url:
        log.info("Вже на сторінці імпорту (redirectUrl спрацював)")
        _wait_import_form_ready(page)
        return

    # Клік «Товари» в лівому sidebar
    tovary = page.locator(SEL_TOVARY_LINK)
    tovary.wait_for(state="visible", timeout=NAV_TIMEOUT_MS)
    tovary.click()
    log.info("Клік «Товари» в sidebar")

    # Клік «Імпорт» в підменю
    import_link = page.locator(SEL_IMPORT_LINK)
    import_link.wait_for(state="visible", timeout=NAV_TIMEOUT_MS)
    import_link.click()
    log.info("Клік «Імпорт» в підменю")

    page.wait_for_url(
        lambda url: "/pim/product-import" in url,
        timeout=NAV_TIMEOUT_MS,
    )
    _wait_import_form_ready(page)
    log.info("Сторінка імпорту готова")


def _is_import_busy(page: Page) -> bool:
    """
    Перевіряє наявність віджета статусу імпорту (SEL_IMPORT_STATUS) з кількома
    спробами замість одноразового count(). Захист від race condition:
    Angular підвантажує статус окремим асинхронним запитом, тому одразу
    після навігації віджет може ще не встигнути з'явитись у DOM, навіть
    якщо попередній імпорт фактично виконується.
    """
    status = page.locator(SEL_IMPORT_STATUS)
    for attempt in range(1, BUSY_CHECK_POLL_ATTEMPTS + 1):
        if status.count() > 0:
            return True
        if attempt < BUSY_CHECK_POLL_ATTEMPTS:
            page.wait_for_timeout(BUSY_CHECK_POLL_INTERVAL_MS)
    return False


def _wait_previous_import_finished(page: Page) -> bool:
    """
    Перевіряє (через _is_import_busy), чи вже висить на сторінці віджет
    активного імпорту — ознака того, що попередній запуск ще не завершився
    (буває, триває понад 2 год на боці Epicenter). Якщо так — чекаємо його
    зникнення в межах IMPORT_BUSY_MAX_WAIT_MS.

    Повертає True, якщо форму можна заповнювати новим імпортом, False —
    якщо бюджет очікування вичерпано і попередній імпорт досі активний.

    Примітка: навіть при True тут немає стовідсоткової гарантії — фінальна
    перевірка знаходиться в _submit_import (де реальна поведінка кнопки
    є достовірнішим сигналом, ніж наявність DOM-елемента).
    """
    if not _is_import_busy(page):
        return True

    log.warning(
        "Попередній імпорт ще виконується (%s присутній на сторінці) — "
        "чекаємо звільнення до %d хв...",
        SEL_IMPORT_STATUS,
        IMPORT_BUSY_MAX_WAIT_MS // 60_000,
    )
    status = page.locator(SEL_IMPORT_STATUS)
    try:
        status.first.wait_for(state="detached", timeout=IMPORT_BUSY_MAX_WAIT_MS)
    except PWTimeout:
        return False

    log.info("Попередній імпорт завершився — продовжуємо.")
    return True


def _click_radio_label(page: Page, label_text: str) -> None:
    """
    Клік по label.mdc-label із потрібним текстом — нативний Playwright click.
    JS evaluate() не тригерить Angular zone; клік по label — тригерить.
    """
    page.locator("label.mdc-label").filter(has_text=label_text).first.click()
    page.wait_for_timeout(400)


def _configure_form(page: Page) -> None:
    """Вибір режиму: Посилання + Оновити все з перевіркою результату."""
    # Чекаємо появи radio-групи
    page.locator(SEL_RADIO_LINK).wait_for(state="attached", timeout=PAGE_TIMEOUT_MS)

    # Клік «Посилання» → має з'явитись input[formcontrolname="url"]
    _click_radio_label(page, "Посилання")
    page.locator(SEL_FEED_URL_INPUT).wait_for(state="visible", timeout=10_000)
    log.info("Вибрано: Посилання (URL input з'явився)")

    # Клік «Оновити все»
    page.locator(SEL_RADIO_FULL).wait_for(state="attached", timeout=PAGE_TIMEOUT_MS)
    _click_radio_label(page, "Оновити все")
    log.info("Вибрано: Оновити все")


def _submit_import(page: Page, feed_url: str) -> None:
    """
    Введення URL фіду і клік «Імпортувати».

    Raises:
        ImportStillBusy: кнопка лишилась disabled і на сторінці присутній
            віджет активного імпорту — попередній прогін ще не завершився
            (виявлено пізніше, ніж _wait_previous_import_finished встиг
            це помітити). Не помилка автоматизації.
        PWTimeout: кнопка не розблокувалась з іншої причини (реальний баг
            форми, мережева проблема тощо) — це вже справжня помилка.
    """
    url_input = page.locator(SEL_FEED_URL_INPUT)
    url_input.wait_for(state="visible", timeout=PAGE_TIMEOUT_MS)
    url_input.fill(feed_url)
    log.info("URL фіду введено: %s", feed_url)

    # Чекаємо поки Angular зніме disabled із кнопки «Імпортувати»
    try:
        page.wait_for_function(
            f"() => {{ const b = document.querySelector('{SEL_IMPORT_BTN}'); return b && !b.disabled; }}",
            timeout=PAGE_TIMEOUT_MS,
        )
    except PWTimeout:
        if _is_import_busy(page):
            raise ImportStillBusy(
                "Кнопка «Імпортувати» лишилась disabled — попередній імпорт "
                "ще виконується (виявлено під час сабміту)"
            ) from None
        raise

    import_btn = page.locator(SEL_IMPORT_BTN)
    import_btn.wait_for(state="visible", timeout=PAGE_TIMEOUT_MS)
    import_btn.click()
    log.info("Клік «Імпортувати»")


def _check_status(page: Page) -> bool:
    """
    Через POST_IMPORT_MS перевіряє наявність тегу підтвердження.
    Повертає True якщо «Імпорт товарів успішно запущений».
    """
    log.info("Очікування %d мс перед перевіркою статусу...", POST_IMPORT_MS)
    page.wait_for_timeout(POST_IMPORT_MS)

    try:
        page.locator(SEL_IMPORT_STATUS).wait_for(state="visible", timeout=SUCCESS_WAIT_MS)
        log.info("✓ Імпорт товарів успішно запущений")
        return True
    except PWTimeout:
        log.error(
            "✗ Тег підтвердження <%s> не знайдено — "
            "імпорт не запущено або виникла помилка на сторінці",
            SEL_IMPORT_STATUS,
        )
        return False


# ---------------------------------------------------------------------------
# Точка входу
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(description="Epicentre Admin: запуск імпорту XML-фіду")
    parser.add_argument(
        "--headed",
        action="store_true",
        help="Запустити видимий браузер (режим debug)",
    )
    args = parser.parse_args()

    email, password = _get_credentials()

    headless = not args.headed
    log.info("Запуск Chromium (headless=%s)", headless)

    success = False
    skipped = False
    with sync_playwright() as pw:
        browser = pw.chromium.launch(headless=headless)
        page    = browser.new_page(viewport={"width": 1440, "height": 900})
        try:
            _login(page, email, password)
            _navigate_to_import(page)

            if _wait_previous_import_finished(page):
                _configure_form(page)
                _submit_import(page, FEED_URL)
                success = _check_status(page)
            else:
                skipped = True
                log.warning(
                    "Попередній імпорт все ще виконується після %d хв очікування — "
                    "пропускаємо цей запуск. Наступний запуск за розкладом спробує знову.",
                    IMPORT_BUSY_MAX_WAIT_MS // 60_000,
                )
        except ImportStillBusy as exc:
            skipped = True
            log.warning(
                "%s — пропускаємо цей запуск. Наступний запуск за розкладом "
                "спробує знову.",
                exc,
            )
        except PWTimeout as exc:
            log.error("Timeout: %s", exc)
        except RuntimeError as exc:
            log.error("%s", exc)
        finally:
            browser.close()

    if skipped:
        sys.exit(0)
    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()
