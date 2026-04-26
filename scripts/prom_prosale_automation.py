# prom_prosale_automation.py
# Python 3.10+ | Playwright sync
#
# Масово додає ProSale до товарів в Prom.ua через браузер.
# Читає список SKU з PROSALE_SKU_LIST, відкриває кажен товар і активує ProSale.
#
# Запуск:
#   python scripts/prom_prosale_automation.py
#
# - ВИПРАВЛЕНО: close_modal_if_open і _close_modal теж оновлені під новий селектор.
# - ВИПРАВЛЕНО: fLkiL.click() падав через "input intercepts pointer events".
#   Замінено на fLkiL.dispatch_event('click') — пряма JS-подія.

from __future__ import annotations

import logging
import os
import time
from dataclasses import dataclass
from typing import List

from playwright.sync_api import Locator, Page, TimeoutError as PWTimeoutError, sync_playwright


# =========================
# CONFIG
# =========================

PROSALE_URL = "https://my.prom.ua/cms/prosale"
PROFILE_DIR = "./pw-profile"

PROM_LOGIN = os.environ.get("PROM_LOGIN", "")
PROM_PASSWORD = os.environ.get("PROM_PASSWORD", "")
PROM_HEADLESS = os.environ.get("PROM_HEADLESS", "").lower() in ("1", "true", "yes")
PROM_COOKIES_JSON = os.environ.get("PROM_COOKIES", "")

CI_MODE = bool(PROM_COOKIES_JSON or (PROM_LOGIN and PROM_PASSWORD))


@dataclass
class Campaign:
    name: str
    tag: str


CAMPAIGNS: List[Campaign] = [
    Campaign(name="SECUR CPA",      tag="Sprom"),
    Campaign(name="VIATEC MAX CPA", tag="VMAX"),
    Campaign(name="VIATEC MIN CPA", tag="Vmin"),
    Campaign(name="SECUR CPC",      tag="Sprom"),
    Campaign(name="VIATEC MAX CPC", tag="VMAX"),
    Campaign(name="VIATEC MIN CPC", tag="Vmin"),
]

LOG_FILE = "prom_prosale.log"

MODAL_TIMEOUT  = 20_000   # чекання появи модалки
CONTENT_TIMEOUT = 25_000  # чекання завантаження контенту всередині модалки
TABLE_TIMEOUT  = 20_000


# =========================
# LOGGING
# =========================

logging.basicConfig(
    filename=LOG_FILE,
    filemode="w",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger("prom_prosale")


# =========================
# COUNTERS
# =========================

stats = {
    "added": 0,
    "empty": 0,
    "not_found": 0,
    "errors": 0,
}


# =========================
# MODAL PRESENCE HELPERS
# =========================

def is_modal_open(page: Page) -> bool:
    """Перевіряє чи модалка відкрита за data-qaid."""
    return page.locator('[data-qaid="add_product_popup"]').count() > 0


def wait_for_modal_content(page: Page) -> None:
    """
    Чекає поки модалка повністю завантажить свій контент.
    Критерій: дропдаун нотаток [data-qaid="select_dropdown"] став visible.
    Це останній елемент що з'являється після async-завантаження.
    """
    page.wait_for_selector(
        '[data-qaid="select_dropdown"]',
        state="visible",
        timeout=CONTENT_TIMEOUT,
    )
    logger.debug("Modal content fully loaded (select_dropdown visible)")


# =========================
# PROM CUSTOM CHECKBOX HELPER
# =========================

def click_prom_checkbox(container: Locator) -> bool:
    """
    Клікає кастомний readonly-чекбокс Prom всередині container.

    Структура:
        <input type="checkbox" readonly="">   ← перекриває fLkiL у DOM (pointer interception)
        <div class="fLkiL"></div>             ← React-обробники, але pointer blocked

    Рішення: dispatch_event('click') — пряма JS-подія на fLkiL.
    Fallback: JS el.click() на input — обходить readonly через DOM API.
    """
    fLkiL = container.locator('xpath=.//*[contains(@class,"fLkiL")]').first
    if fLkiL.count() > 0:
        fLkiL.wait_for(state="attached", timeout=3_000)
        fLkiL.dispatch_event("click")
        logger.debug("dispatch_event('click') on fLkiL")
        return True

    input_el = container.locator(
        'input[data-qaid="item_chbx"], input[data-qaid="select_all"], input[type="checkbox"]'
    ).first
    if input_el.count() > 0:
        input_el.evaluate("el => el.click()")
        logger.debug("JS el.click() on input (fallback)")
        return True

    logger.warning("click_prom_checkbox: no checkbox found in container")
    return False


# =========================
# AUTH
# =========================

def ensure_logged_in(page: Page) -> None:
    if page.locator('input[type="password"]').count() > 0:
        raise RuntimeError("[X] Не залогінено. Зайди вручну, потім відкрий /cms/prosale.")
    if any(x in page.url.lower() for x in ("login", "auth", "signin")):
        raise RuntimeError("[X] Не залогінено. Зайди вручну, потім відкрий /cms/prosale.")


def login_with_credentials(page: Page, login: str, password: str) -> None:
    logger.info("CI_MODE: logging in as %s", login)
    print(f"\n[АВТОЛОГІН] Вхід як {login}...")

    page.goto("https://prom.ua/", wait_until="domcontentloaded")
    page.wait_for_timeout(1500)

    page.locator('[data-qaid="show_sidebar"]').wait_for(state="visible", timeout=15_000)
    page.locator('[data-qaid="show_sidebar"]').click()
    page.wait_for_timeout(800)

    page.locator('[data-qaid="sign_in_mob_sidebar"]').wait_for(state="visible", timeout=10_000)
    page.locator('[data-qaid="sign_in_mob_sidebar"]').click()
    page.wait_for_timeout(800)

    page.locator('[data-qaid="email_btn"]').wait_for(state="visible", timeout=10_000)
    page.locator('[data-qaid="email_btn"]').click()
    page.wait_for_timeout(600)

    page.locator('#email_field').wait_for(state="visible", timeout=10_000)
    page.locator('#email_field').fill(login)
    page.wait_for_timeout(400)

    page.locator('#emailConfirmButton').wait_for(state="visible", timeout=10_000)
    page.wait_for_function(
        "document.querySelector('#emailConfirmButton') && !document.querySelector('#emailConfirmButton').disabled",
        timeout=10_000,
    )
    page.locator('#emailConfirmButton').click()
    page.wait_for_timeout(800)

    page.locator('#enterPassword').wait_for(state="visible", timeout=10_000)
    page.locator('#enterPassword').fill(password)
    page.wait_for_timeout(400)

    page.locator('#enterPasswordConfirmButton').wait_for(state="visible", timeout=10_000)
    page.wait_for_function(
        "document.querySelector('#enterPasswordConfirmButton') && !document.querySelector('#enterPasswordConfirmButton').disabled",
        timeout=10_000,
    )
    page.locator('#enterPasswordConfirmButton').click()

    page.wait_for_url(lambda url: "my.prom.ua" in url or "login" not in url, timeout=25_000)
    page.wait_for_timeout(3_000)

    if any(x in page.url.lower() for x in ("login", "auth", "signin")):
        raise RuntimeError("Логін не вдався.")

    logger.info("CI_MODE: login successful, url=%s", page.url)
    print("[ОК] Автологін успішний")


# =========================
# NAVIGATION
# =========================

def goto_prosale_list(page: Page) -> None:
    page.goto(PROSALE_URL, wait_until="domcontentloaded")
    page.wait_for_selector('[data-qaid="name"]', timeout=TABLE_TIMEOUT)
    page.wait_for_timeout(500)


def click_back_to_prosale_list(page: Page) -> None:
    back_link = page.locator('a[href="/cms/prosale"]').first
    if back_link.count() == 0:
        back_link = page.locator('[data-qaid="SvgArrowBack"]').locator('xpath=ancestor::a').first

    if back_link.count() == 0:
        logger.warning("Back arrow not found, navigating directly")
        goto_prosale_list(page)
        return

    back_link.click()
    page.wait_for_selector('[data-qaid="name"]', timeout=TABLE_TIMEOUT)
    page.wait_for_timeout(400)


# =========================
# CAMPAIGN PROCESSING
# =========================

def open_campaign(page: Page, campaign_name: str) -> bool:
    link = page.locator(f'[data-qaid="name"]:text-is("{campaign_name}")').first
    if link.count() == 0:
        link = page.locator('[data-qaid="name"]').filter(has_text=campaign_name).first

    if link.count() == 0:
        logger.warning("Campaign not found: %s", campaign_name)
        print(f"  [!] Кампанія не знайдена: {campaign_name}")
        return False

    logger.info("Opening campaign: %s", campaign_name)
    link.click()
    page.wait_for_selector('[data-qaid="add_product_link"]', timeout=20_000)
    page.wait_for_timeout(500)
    return True


def open_add_products_modal(page: Page) -> None:
    """
    Натискає кнопку "Додати товар або групу" і чекає ПОВНОГО завантаження модалки.

    Двоетапне очікування:
      1. [data-qaid="add_product_popup"] — сама модалка з'явилась у DOM
      2. [data-qaid="select_dropdown"]  — контент усередині завантажився
    """
    btn = page.locator('[data-qaid="add_product_link"]').first
    btn.wait_for(state="visible", timeout=15_000)
    btn.click()

    # Етап 1: модалка відкрилась
    page.wait_for_selector(
        '[data-qaid="add_product_popup"]',
        state="visible",
        timeout=MODAL_TIMEOUT,
    )
    logger.info("Modal appeared (add_product_popup visible)")

    # Етап 2: контент повністю завантажений
    wait_for_modal_content(page)
    logger.info("Modal content ready (select_dropdown visible)")

    page.wait_for_timeout(300)


def select_tag_in_dropdown(page: Page, tag_name: str) -> bool:
    """
    Відкриває дропдаун нотаток, dispatch_event('click') на fLkiL потрібного тегу,
    чекає активації кнопки "Додати" і натискає її.
    """
    # 1. Відкриваємо дропдаун (вже гарантовано visible після wait_for_modal_content)
    dropdown = page.locator('[data-qaid="select_dropdown"]').first
    dropdown.click()
    page.wait_for_timeout(400)

    # 2. Чекаємо popup
    tag_popup = page.locator('[data-qaid="add_tag_popup"]').first
    try:
        tag_popup.wait_for(state="visible", timeout=5_000)
    except PWTimeoutError:
        page.keyboard.press("Escape")
        logger.warning("Tag popup did not appear for: %s", tag_name)
        return False

    # 3. Знаходимо тег
    tag_span = page.locator(f'[data-qaid="add_tag_name"]:text-is("{tag_name}")').first
    if tag_span.count() == 0:
        page.keyboard.press("Escape")
        logger.warning("Tag '%s' not found in dropdown", tag_name)
        print(f"  [!] Тег '{tag_name}' не знайдений")
        return False

    # 4. dispatch_event на fLkiL батьківського <li>
    li_item = tag_span.locator('xpath=ancestor::li[1]').first
    li_item.wait_for(state="visible", timeout=5_000)

    clicked = click_prom_checkbox(li_item)
    if not clicked:
        page.keyboard.press("Escape")
        logger.warning("Could not dispatch click for tag '%s'", tag_name)
        return False

    page.wait_for_timeout(350)

    # 5. Чекаємо активації save_btn
    try:
        page.wait_for_function(
            "document.querySelector('[data-qaid=\"save_btn\"]') && "
            "!document.querySelector('[data-qaid=\"save_btn\"]').disabled",
            timeout=5_000,
        )
    except PWTimeoutError:
        page.keyboard.press("Escape")
        logger.warning("Save button stayed disabled for tag '%s'", tag_name)
        print(f"  [!] Кнопка 'Додати' не активувалась для '{tag_name}'")
        return False

    # 6. Підтверджуємо вибір тегу
    save_btn = page.locator('[data-qaid="save_btn"]').first
    save_btn.click()
    page.wait_for_timeout(800)
    logger.info("Tag '%s' selected and confirmed", tag_name)
    return True


def handle_products_in_modal(page: Page, campaign_name: str) -> bool:
    """
    Після застосування фільтру тегу:
    - є товари → dispatch_event на "Вибрати все" → "Додати до кампанії"
    - немає → закрити модалку
    """
    page.wait_for_timeout(1_000)

    select_all_input = page.locator('[data-qaid="select_all"]').first

    if select_all_input.count() == 0:
        logger.info("No products for campaign '%s'", campaign_name)
        print(f"  [—] Нових товарів немає для '{campaign_name}'")
        _close_modal(page)
        return False

    # dispatch_event на fLkiL поряд із select_all
    select_all_container = select_all_input.locator('xpath=parent::*').first
    clicked = click_prom_checkbox(select_all_container)
    if not clicked:
        select_all_input.evaluate("el => el.click()")

    page.wait_for_timeout(400)

    # "Додати до кампанії"
    add_btn = page.locator('[data-qaid="add_product_btn"]').first
    add_btn.wait_for(state="visible", timeout=10_000)
    add_btn.click()

    # Чекаємо закриття модалки за новим селектором
    try:
        page.locator('[data-qaid="add_product_popup"]').wait_for(
            state="hidden", timeout=15_000
        )
    except PWTimeoutError:
        pass

    page.wait_for_timeout(600)
    logger.info("Products added for '%s'", campaign_name)
    print(f"  [✓] Товари додані до кампанії '{campaign_name}'")
    return True


def _close_modal(page: Page) -> None:
    close_btn = page.locator('[data-qaid="close-icon"]').first
    if close_btn.count() == 0:
        # Запасний варіант: кнопка "Скасувати"
        close_btn = page.locator('[data-qaid="cancel_btn"]').first
    if close_btn.count() > 0:
        close_btn.click()
    else:
        page.keyboard.press("Escape")
    page.wait_for_timeout(500)


def close_modal_if_open(page: Page) -> None:
    if page.locator('[data-qaid="add_product_popup"]').count() > 0:
        _close_modal(page)


def process_campaign(page: Page, campaign: Campaign) -> None:
    print(f"\n[→] Кампанія: {campaign.name!r} | Тег: {campaign.tag!r}")
    logger.info("Processing campaign: %s (tag: %s)", campaign.name, campaign.tag)

    try:
        if not open_campaign(page, campaign.name):
            stats["not_found"] += 1
            return

        open_add_products_modal(page)

        tag_selected = select_tag_in_dropdown(page, campaign.tag)
        if not tag_selected:
            close_modal_if_open(page)
            stats["errors"] += 1
            click_back_to_prosale_list(page)
            return

        added = handle_products_in_modal(page, campaign.name)
        stats["added" if added else "empty"] += 1

        click_back_to_prosale_list(page)

    except Exception as e:
        logger.error("Error processing '%s': %s", campaign.name, e)
        print(f"  [✗] Помилка для '{campaign.name}': {e}")
        stats["errors"] += 1
        try:
            close_modal_if_open(page)
        except Exception:
            pass
        try:
            goto_prosale_list(page)
        except Exception as e2:
            logger.error("Failed to recover: %s", e2)


# =========================
# MAIN
# =========================

def main() -> None:
    start_time = time.time()

    print("=" * 60)
    print("  PROM PROSALE AUTOMATION v1.3.0 — старт")
    print("=" * 60)
    logger.info("Starting, CI_MODE=%s", CI_MODE)

    with sync_playwright() as p:
        if CI_MODE:
            browser = p.chromium.launch(
                headless=True,
                args=["--no-sandbox", "--disable-setuid-sandbox"],
            )
            context = browser.new_context(viewport={"width": 1500, "height": 900})
            page = context.new_page()

            if PROM_COOKIES_JSON:
                import json as _json
                cookies = _json.loads(PROM_COOKIES_JSON)
                context.add_cookies(cookies)
                logger.info("Loaded %s cookies", len(cookies))
                print(f"[COOKIES] Завантажено {len(cookies)} cookies")
            else:
                login_with_credentials(page, PROM_LOGIN, PROM_PASSWORD)
        else:
            context = p.chromium.launch_persistent_context(
                PROFILE_DIR,
                headless=PROM_HEADLESS,
                viewport={"width": 1500, "height": 900},
                channel="chrome",
            )
            page = context.new_page()
            ensure_logged_in(page)

        goto_prosale_list(page)

        for campaign in CAMPAIGNS:
            process_campaign(page, campaign)

        context.close()

    elapsed = time.time() - start_time
    logger.info(
        "FINISH added=%s empty=%s not_found=%s errors=%s time=%.0fs",
        stats["added"], stats["empty"], stats["not_found"], stats["errors"], elapsed,
    )

    print("\n" + "=" * 60)
    print("  PROM PROSALE AUTOMATION — ЗАВЕРШЕНО")
    print("=" * 60)
    print(f"[✓] Товари додано:          {stats['added']}")
    print(f"[—] Нових товарів не було:  {stats['empty']}")
    print(f"[!] Кампанія не знайдена:   {stats['not_found']}")
    print(f"[✗] Помилок:                {stats['errors']}")
    print(f"[⏱] Час виконання:          {elapsed:.0f}s ({elapsed / 60:.1f} хв)")
    print(f"[📄] Лог: {LOG_FILE}")
    print("=" * 60)


if __name__ == "__main__":
    try:
        main()
    except PWTimeoutError as e:
        raise SystemExit(f"Timeout: {e}") from e
    except KeyboardInterrupt:
        print("\n\n[!] Скрипт перервано (Ctrl+C)")
    except RuntimeError as e:
        print(f"\n[X] {e}")
        print("Залогінься вручну і запусти скрипт знову.")
