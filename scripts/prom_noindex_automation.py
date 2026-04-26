# prom_noindex_automation.py
# Python 3.10+ | Playwright sync
#
# Масово виставляє noindex на товари в Prom.ua через браузер.
# Читає список SKU з NOINDEX_SKU_LIST, відкриває кажен товар і знімає з індексуації.
#
# Запуск:
#   python scripts/prom_noindex_automation.py
#
# 2. Проверка тега в списке: берём ТОЛЬКО строки где реально есть tag в DOM
# 3. FATAL blacklist: защита от проблемных товаров
# 4. Fresh reload между pass: защита от SPA-кеша
# 5. Обработка блокировки PROM: выход через стрелку + работа в списке
#
# ЛОГИКА:
# - Фильтр списка по QUEUE_TAG ("noindex")
# - На каждой странице списка снимаем "actionable snapshot":
#   берём ТОЛЬКО те product_row, где в DOM строки реально есть tag_name == "noindex"
#   => призраки НЕ попадают в слепок и не открываются
# - Обрабатываем href из слепка напрямую (не зависим от обновления списка)
#
# УСПЕХ:
#   1) открыть карточку
#   2) поставить SEO чекбокс noindex
#   3) удалить тег QUEUE_TAG
#   4) save&return
#
# ОШИБКА ВАЛИДАЦИИ (PROM блокирует сохранение):
#   1) обнаружение timeout на save_and_return
#   2) выход через стрелку "Повернутися назад"
#   3) добавление ERROR_TAG прямо в списке
#   4) удаление QUEUE_TAG прямо в списке
#
# ОШИБКА (первая попытка):
#   - делаем скрин
#   - помечаем ошибкой: переносим QUEUE_TAG -> ERROR_TAG (создается автоматически если нет)
#   - save&return
#
# FATAL (повторная ошибка):
#   - добавляем в blacklist
#   - пропускаем навсегда
#
# АВТОСОЗДАНИЕ ERROR_TAG:
# При первой ошибке скрипт автоматически создаст тег 'noindex_error' если его нет.
# Никакой ручной подготовки не требуется - все автоматически!

from __future__ import annotations

import json
import logging
import time
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import List, Set

from playwright.sync_api import Page, TimeoutError as PWTimeoutError, sync_playwright


# =========================
# CONFIG
# =========================

START_URL = "https://my.prom.ua/cms/product"
PROFILE_DIR = "./pw-profile"

# Режим запуску:
#   Локально:          PROM_LOGIN/PROM_PASSWORD не задані → користується збережена сесія з PROFILE_DIR
#   GitHub Actions: PROM_LOGIN/PROM_PASSWORD задані → автоматичний логін, headless=True
import os as _os
PROM_LOGIN = _os.environ.get("PROM_LOGIN", "")
PROM_PASSWORD = _os.environ.get("PROM_PASSWORD", "")
PROM_HEADLESS = _os.environ.get("PROM_HEADLESS", "").lower() in ("1", "true", "yes")
PROM_COOKIES_JSON = _os.environ.get("PROM_COOKIES", "")  # JSON-рядок з cookies (GitHub Secret)

# CI_MODE=True коли є PROM_COOKIES або PROM_LOGIN+PASSWORD.
# Пріоритет: PROM_COOKIES > PROM_LOGIN (cookies надійніші, не потребує 2FA)
CI_MODE = bool(PROM_COOKIES_JSON or (PROM_LOGIN and PROM_PASSWORD))

QUEUE_TAG = "noindex"
ERROR_TAG = "noindex_error"  # создается автоматически при первой ошибке

PER_PAGE = 100

DELAY_BETWEEN_ITEMS_SEC = 0.15
DELAY_AFTER_LIST_FILTER_MS = 650
DELAY_BEFORE_BACK_ARROW_SEC = 2.5  # v2.6.1: задержка перед выходом через стрелку

# safety
MAX_PASSES = 500
MAX_ITEMS_TOTAL = 50000
MAX_ITEMS_PER_PAGE = 150

LOG_FILE = "prom_noindex.log"
FATAL_FILE = Path("prom_noindex_fatal_hrefs.json")


# =========================
# LOGGING
# =========================

logging.basicConfig(
    filename=LOG_FILE,
    filemode="w",  # 'w' — перезаписывать при каждом запуске (не дописывать)
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger("prom_noindex")


# =========================
# STATE
# =========================

processed_ok = 0
processed_err = 0
processed_ghost = 0
processed_fatal = 0
processed_validation_err = 0  # v2.6.1: счётчик ошибок валидации

start_time = 0.0

# защита от дублей href внутри запуска
processed_hrefs: Set[str] = set()


# =========================
# FATAL BLACKLIST
# =========================

def load_fatal_hrefs() -> Set[str]:
    """Загрузить список фатальных товаров из blacklist"""
    if not FATAL_FILE.exists():
        return set()
    try:
        data = json.loads(FATAL_FILE.read_text(encoding="utf-8"))
        if isinstance(data, list):
            return set(map(str, data))
        return set(map(str, data.get("hrefs", [])))
    except Exception as e:
        logger.error("Failed to load fatal hrefs: %s", e)
        return set()


def save_fatal_href(href: str) -> None:
    """Добавить товар в blacklist"""
    hrefs = load_fatal_hrefs()
    hrefs.add(str(href))
    FATAL_FILE.write_text(
        json.dumps(sorted(hrefs), ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    logger.info("FATAL_SAVED %s -> %s", href, FATAL_FILE)


# =========================
# BASIC UI
# =========================

def ensure_logged_in(page: Page) -> None:
    """Fallback: перевірка для локального режиму (збережена сесія)."""
    if page.locator('input[type="password"]').count() > 0:
        raise RuntimeError("[X] Не залогінено. Зайди вручну в цьому вікні, потім відкрий /cms/product.")
    if any(x in page.url.lower() for x in ("login", "auth", "signin")):
        raise RuntimeError("[X] Не залогінено. Зайди вручну в цьому вікні, потім відкрий /cms/product.")


def login_with_credentials(page: Page, login: str, password: str) -> None:
    """
    Автоматичний логін через форму prom.ua (повний UI-флоу).
    Використовується в GitHub Actions (через PROM_LOGIN / PROM_PASSWORD env).
    Локально не використовується — замість нього працює збережена сесія з PROFILE_DIR.

    Флоу:
      1. prom.ua → кнопка «Кабінет» (show_sidebar)
      2. Сайдбар → «Увійти або зареєструватись» (sign_in_mob_sidebar)
      3. Модалка → іконка «Email» (email_btn)
      4. Поле email (#email_field) → кнопка «Далі» (#emailConfirmButton)
      5. Поле пароля (#enterPassword) → кнопка «Увійти» (#enterPasswordConfirmButton)
      6. Чекаємо переходу на my.prom.ua
    """
    logger.info("CI_MODE: logging in as %s", login)
    print(f"\n[АВТОЛОГІН] Вход як {login}...")

    # 1. Відкриваємо головну сторінку prom.ua
    page.goto("https://prom.ua/", wait_until="domcontentloaded")
    page.wait_for_timeout(1500)

    # 2. Клік «Кабінет» — відкриває сайдбар
    cabinet_btn = page.locator('[data-qaid="show_sidebar"]')
    cabinet_btn.wait_for(state="visible", timeout=15_000)
    cabinet_btn.click()
    page.wait_for_timeout(800)

    # 3. Клік «Увійти або зареєструватись» в сайдбарі
    signin_btn = page.locator('[data-qaid="sign_in_mob_sidebar"]')
    signin_btn.wait_for(state="visible", timeout=10_000)
    signin_btn.click()
    page.wait_for_timeout(800)

    # 4. Клік на іконку Email у модалці логіну
    email_icon = page.locator('[data-qaid="email_btn"]')
    email_icon.wait_for(state="visible", timeout=10_000)
    email_icon.click()
    page.wait_for_timeout(600)

    # 5. Вводимо email
    email_input = page.locator('#email_field')
    email_input.wait_for(state="visible", timeout=10_000)
    email_input.fill(login)
    page.wait_for_timeout(400)

    # 6. Кнопка «Далі» (стає активною після введення email)
    next_btn = page.locator('#emailConfirmButton')
    next_btn.wait_for(state="visible", timeout=10_000)
    # Чекаємо поки кнопка стане клікабельною (знімається disabled)
    page.wait_for_function(
        "document.querySelector('#emailConfirmButton') && !document.querySelector('#emailConfirmButton').disabled",
        timeout=10_000,
    )
    next_btn.click()
    page.wait_for_timeout(800)

    # 7. Вводимо пароль
    password_input = page.locator('#enterPassword')
    password_input.wait_for(state="visible", timeout=10_000)
    password_input.fill(password)
    page.wait_for_timeout(400)

    # 8. Кнопка «Увійти» (стає активною після введення пароля)
    login_btn = page.locator('#enterPasswordConfirmButton')
    login_btn.wait_for(state="visible", timeout=10_000)
    page.wait_for_function(
        "document.querySelector('#enterPasswordConfirmButton') && !document.querySelector('#enterPasswordConfirmButton').disabled",
        timeout=10_000,
    )
    login_btn.click()

    # 9. Чекаємо переходу на my.prom.ua (адмінка)
    page.wait_for_url(lambda url: "my.prom.ua" in url or "login" not in url, timeout=25_000)
    page.wait_for_timeout(3_000)

    if any(x in page.url.lower() for x in ("login", "auth", "signin")):
        raise RuntimeError("Логін не вдався. Перевірте PROM_LOGIN / PROM_PASSWORD у GitHub Secrets.")

    logger.info("CI_MODE: login successful, url=%s", page.url)
    print("[ОК] Автологін успішний")


def wait_list(page: Page) -> None:
    page.wait_for_selector("text=Перелік позицій", timeout=30_000)


def wait_list_short(page: Page, timeout_ms: int) -> None:
    """v2.6.2: Короткое ожидание списка — для обнаружения блокировки валидации."""
    page.wait_for_selector("text=Перелік позицій", timeout=timeout_ms)


def wait_edit(page: Page) -> None:
    page.wait_for_selector('[data-qaid="noindex_chbx"]', timeout=30_000)


def recover_ui(page: Page) -> None:
    """Закрыть неожиданные оверлеи/попапы"""
    try:
        page.keyboard.press("Escape")
    except Exception:
        pass
    try:
        page.locator("body").click(position={"x": 5, "y": 5})
    except Exception:
        pass


def goto_list(page: Page) -> None:
    page.goto(START_URL, wait_until="domcontentloaded")
    wait_list(page)


def goto_list_fresh(page: Page) -> None:
    """Cache-buster: реальная перезагрузка списка"""
    page.goto(f"{START_URL}?_={int(time.time() * 1000)}", wait_until="domcontentloaded")
    wait_list(page)


def return_to_list_via_back_arrow(page: Page) -> None:
    """
    v2.6.1: Возврат в список через стрелку "Повернутися назад"
    Используется когда PROM блокирует сохранение (валидация)
    """
    # Задержка чтобы PROM показал ошибку валидации
    logger.info("Waiting %ss for validation error display", DELAY_BEFORE_BACK_ARROW_SEC)
    page.wait_for_timeout(int(DELAY_BEFORE_BACK_ARROW_SEC * 1000))
    
    # Ищем стрелку "Повернутися назад"
    arrow = page.locator('[data-qaid="previous-icon"]').first
    if arrow.count() == 0:
        # Альтернативный селектор
        arrow = page.locator('div.b-content__header-icon-arrow').first
    
    if arrow.count() == 0:
        logger.warning("Back arrow not found, using goto_list instead")
        goto_list(page)
        return
    
    logger.info("Returning via back arrow (validation blocked save)")
    print("\n[VALIDATION] Повертаюся через стрілку (PROM заблокував збереження)")
    arrow.click()
    wait_list(page)
    page.wait_for_timeout(500)


# =========================
# LIST: PER PAGE + FILTER
# =========================

def set_per_page(page: Page, per_page: int = PER_PAGE) -> None:
    if page.locator(f'text=/по\\s*{per_page}\\s*позицій/i').count() > 0:
        return

    dd = page.locator('text=/по\\s*\\d+\\s*позицій/i').first
    if dd.count() == 0:
        return

    dd.click()
    page.locator(f'text=/по\\s*{per_page}\\s*позицій/i').first.click()
    page.wait_for_timeout(400)


def open_filter_dropdown(page: Page) -> None:
    dd = page.locator("div.b-smart-filter__dd-value.qa_filter_dd_value").first
    dd.wait_for(state="visible", timeout=30_000)
    dd.click()


def close_overlays(page: Page) -> None:
    """Закрити дропдауни/оверлеї перед кліком — лише Escape, без кліку по body."""
    try:
        page.keyboard.press("Escape")
        page.wait_for_timeout(150)
        page.keyboard.press("Escape")
        page.wait_for_timeout(150)
    except Exception:
        pass


def apply_queue_filter(page: Page, tag_name: str) -> bool:
    """
    Застосовує фільтр по тегу.
    Повертає False якщо тег відсутній в дропдауні (тобто товарів з цим тегом більше немає).
    """
    open_filter_dropdown(page)
    page.locator('div[data-qaid="menu-item"]:has-text("Нотатки")').first.click()
    # Prom підвантажує список нотаток 1-2 сек — чекаємо появи будь-якого пункту в підменю
    try:
        page.locator('[data-qaid="menu-item"]').nth(1).wait_for(state="visible", timeout=5_000)
    except Exception:
        page.wait_for_timeout(2_500)

    close_overlays(page)

    option = page.locator(
        f'xpath=//span[normalize-space(text())="{tag_name}"]'
        f'/ancestor::*[@role="menuitem" or @data-qaid="menu-item"][1]'
    ).first
    if option.count() == 0:
        option = page.locator(f'xpath=//span[normalize-space(text())="{tag_name}"]').first

    # Якщо тег не зтявився в дропдауні за 5 секунди — товарів з цим тегом більше немає, закриваємо дропдаун
    try:
        option.wait_for(state="visible", timeout=5_000)
    except Exception:
        page.keyboard.press("Escape")
        page.wait_for_timeout(200)
        logger.info("Tag '%s' not found in filter dropdown — no items left", tag_name)
        print(f"\n[ІНФО] Тег '{tag_name}' зник з фільтру — товарів більше немає.")
        return False

    option.scroll_into_view_if_needed()
    option.click()

    # Чекаємо поки теги реально з'являться в рядках списку (Prom рендерить їх асинхронно)
    try:
        page.locator(
            f'[data-qaid="product_tag"] [data-qaid="tag_name"]:text-is("{tag_name}")'
        ).first.wait_for(state="visible", timeout=8_000)
    except Exception:
        # Fallback: фіксована пауза якщо тег так і не з'явився
        page.wait_for_timeout(3_000)
        logger.warning("Tag '%s' did not appear in product rows after filter — using fallback wait", tag_name)

    page.wait_for_timeout(DELAY_AFTER_LIST_FILTER_MS)
    return True


def count_rows(page: Page) -> int:
    return page.locator('[data-qaid="product_row"]').count()


def list_empty(page: Page) -> bool:
    return count_rows(page) == 0


# =========================
# LIST: ACTIONABLE SNAPSHOT (NO GHOSTS)
# =========================

def snapshot_actionable_hrefs(page: Page, tag_name: str, fatal_hrefs: Set[str]) -> List[str]:
    """
    v2.6: Гибридный подход
    - Берём href только из строк, где реально есть tag_name в DOM (отсекает "призраков")
    - Пропускаем href из blacklist (FATAL)
    """
    rows = page.locator('[data-qaid="product_row"]').filter(
        has=page.locator(f'[data-qaid="product_tag"] [data-qaid="tag_name"]:text-is("{tag_name}")')
    )

    n = min(rows.count(), MAX_ITEMS_PER_PAGE)
    hrefs: List[str] = []
    
    for i in range(n):
        row = rows.nth(i)
        link = row.locator('a[href^="/cms/product/edit/"]').first
        if link.count() == 0:
            continue

        href = link.get_attribute("href") or ""
        if not href:
            continue

        # Пропускаем дубли и FATAL
        if href in processed_hrefs or href in hrefs or href in fatal_hrefs:
            continue

        hrefs.append(href)

    return hrefs


def click_next_page(page: Page) -> bool:
    next_btn = page.get_by_role("button", name="Наступна →")
    if next_btn.count() == 0:
        next_btn = page.locator("text=Наступна →").first
    if next_btn.count() == 0:
        return False

    try:
        if next_btn.is_disabled():
            return False
    except Exception:
        pass

    next_btn.click()
    page.wait_for_timeout(600)
    return True


# =========================
# TAG OPS IN LIST (v2.6.1)
# =========================

def add_tag_in_list(page: Page, href: str, tag_name: str) -> None:
    """
    v2.6.1: Добавить тег товару прямо В СПИСКЕ (без открытия)
    """
    logger.info("Adding tag '%s' in list for %s", tag_name, href)
    
    # Найти строку товара по href
    row = page.locator(f'[data-qaid="product_row"]:has(a[href="{href}"])').first
    if row.count() == 0:
        raise RuntimeError(f"Product row not found in list for {href}")
    
    # Кликнуть на кнопку добавления тега в строке
    add_btn = row.locator('[data-qaid="add_tag_icon"]').first
    if add_btn.count() == 0:
        raise RuntimeError(f"Add tag button not found in row for {href}")
    
    add_btn.click()
    page.wait_for_timeout(300)
    
    # Ввести название тега
    inp = page.locator('[data-qaid="search_tag_input"]').first
    inp.wait_for(state="visible", timeout=30_000)
    inp.fill(tag_name)
    page.wait_for_timeout(250)
    
    # Выбрать существующий тег или создать новый
    option = page.locator(f'xpath=//span[normalize-space(text())="{tag_name}"]').first
    if option.count() == 0:
        option = page.locator(f'text="{tag_name}"').first
    
    if option.count() > 0:
        # Существующий тег
        option.click()
    else:
        # Создать новый
        create_link = page.locator('[data-qaid="new_tag_link"]').filter(has_text=tag_name).first
        if create_link.count() == 0:
            create_link = page.locator(f'text="Створити мітку {tag_name}"').first
        if create_link.count() == 0:
            create_link = page.locator(f'text="Создать метку {tag_name}"').first
        
        if create_link.count() > 0:
            create_link.click()
            page.wait_for_timeout(200)
    
    # Сохранить (если требуется кнопка)
    save_btn = page.locator('[data-qaid="save_btn"]').first
    if save_btn.count() > 0:
        save_btn.click()
        page.wait_for_timeout(200)
    
    page.keyboard.press("Escape")
    page.wait_for_timeout(400)
    
    logger.info("Added tag '%s' in list for %s", tag_name, href)


def delete_tag_in_list(page: Page, href: str, tag_name: str) -> None:
    """
    v2.6.4: Удалить тег товару прямо В СПИСКЕ.
    Ищем через XPath: находим del_tag_icon рядом с нужным tag_name внутри строки.
    """
    logger.info("Deleting tag '%s' in list for %s", tag_name, href)

    # Найти строку товара
    row = page.locator(f'[data-qaid="product_row"]:has(a[href="{href}"])').first
    if row.count() == 0:
        logger.warning("Product row not found in list for %s", href)
        return

    # XPath: найди del_tag_icon внутри product_tag где tag_name == tag_name
    # normalize-space обходит проблемы с whitespace/переносами строк
    del_icon = row.locator(
        f'xpath=.//*[@data-qaid="product_tag"]'
        f'[normalize-space(.//*[@data-qaid="tag_name"]/text())="{tag_name}"]'
        f'//*[@data-qaid="del_tag_icon"]'
    ).first

    if del_icon.count() == 0:
        logger.warning("Tag '%s' del_icon not found in list for %s", tag_name, href)
        return

    del_icon.click()
    page.wait_for_timeout(400)
    logger.info("Deleted tag '%s' in list for %s", tag_name, href)


# =========================
# TAG OPS IN CARD
# =========================

def tag_exists(page: Page, tag_name: str) -> bool:
    return page.locator('[data-qaid="product_tag"]').filter(
        has=page.locator(f'[data-qaid="tag_name"]:text-is("{tag_name}")')
    ).count() > 0


def delete_tag(page: Page, tag_name: str) -> bool:
    chip = page.locator('[data-qaid="product_tag"]').filter(
        has=page.locator(f'[data-qaid="tag_name"]:text-is("{tag_name}")')
    ).first
    if chip.count() == 0:
        return False

    chip.locator('[data-qaid="del_tag_icon"]').first.click()
    page.wait_for_timeout(180)
    return True


def add_existing_tag_via_panel(page: Page, tag_name: str) -> None:
    """
    Добавляет СУЩЕСТВУЮЩИЙ тег через панель тегов.
    Мы не создаём тег, а выбираем существующий из списка.
    Raises RuntimeError если тег не найден (нужно создать).
    """
    plus = page.locator('[data-qaid="add_tag_icon"]').first
    if plus.count() == 0:
        raise RuntimeError("Не найдена кнопка добавления тега (+)")
    plus.click()

    inp = page.locator('[data-qaid="search_tag_input"]').first
    inp.wait_for(state="visible", timeout=30_000)
    inp.fill(tag_name)
    page.wait_for_timeout(200)

    # выбираем существующий тег (точное совпадение)
    option = page.locator(f'xpath=//span[normalize-space(text())="{tag_name}"]').first
    if option.count() == 0:
        option = page.locator(f'text="{tag_name}"').first

    if option.count() == 0:
        # Тег не найден в списке существующих
        page.keyboard.press("Escape")
        raise RuntimeError(f"Tag '{tag_name}' not found in existing tags list")

    option.click()

    # часто нужно подтверждение
    btn = page.get_by_role("button", name="Додати мітку")
    if btn.count() == 0:
        btn = page.get_by_role("button", name="Добавить метку")

    if btn.count() > 0:
        btn.first.click()
        page.wait_for_timeout(150)

    page.keyboard.press("Escape")
    page.wait_for_timeout(120)

    if not tag_exists(page, tag_name):
        raise RuntimeError(f"Failed to add tag '{tag_name}'")


def create_new_tag_via_panel(page: Page, tag_name: str) -> None:
    """
    Создает НОВЫЙ тег через панель тегов.
    Используется ТОЛЬКО для создания ERROR_TAG при первой ошибке.
    """
    plus = page.locator('[data-qaid="add_tag_icon"]').first
    if plus.count() == 0:
        raise RuntimeError("Не найдена кнопка добавления тега (+)")
    plus.click()

    inp = page.locator('[data-qaid="search_tag_input"]').first
    inp.wait_for(state="visible", timeout=30_000)
    inp.fill(tag_name)
    page.wait_for_timeout(300)

    # Кликаем на "Створити мітку" / "Создать метку"
    create_link = page.locator('[data-qaid="new_tag_link"]').filter(has_text=tag_name).first
    if create_link.count() == 0:
        # Альтернативный селектор
        create_link = page.locator(f'text="Створити мітку {tag_name}"').first
    if create_link.count() == 0:
        create_link = page.locator(f'text="Создать метку {tag_name}"').first
    
    if create_link.count() == 0:
        page.keyboard.press("Escape")
        raise RuntimeError(f"Не найдена кнопка создания метки '{tag_name}'")
    
    create_link.click()
    page.wait_for_timeout(200)

    # Подтверждаем создание
    save_btn = page.locator('[data-qaid="save_btn"]').first
    if save_btn.count() > 0:
        save_btn.click()
        page.wait_for_timeout(200)

    page.keyboard.press("Escape")
    page.wait_for_timeout(120)

    if not tag_exists(page, tag_name):
        raise RuntimeError(f"Failed to create tag '{tag_name}'")

    logger.info("Created new tag: %s", tag_name)


def move_queue_to_error(page: Page) -> None:
    """
    v2.6: Переносим noindex -> noindex_error с автосозданием:
    - Если error_tag уже есть → просто удаляем queue tag
    - Пытаемся добавить существующий error_tag
    - Если не получается → создаем новый (ОДИН РАЗ)
    - Удаляем queue tag
    """
    if tag_exists(page, ERROR_TAG):
        # уже помечено error tag'ом
        delete_tag(page, QUEUE_TAG)
        return

    try:
        # Пытаемся добавить существующий
        add_existing_tag_via_panel(page, ERROR_TAG)
    except RuntimeError as e:
        # Тег не существует → создаем ОДИН РАЗ
        logger.info("ERROR_TAG not found, creating: %s", ERROR_TAG)
        print(f"\n[INFO] Створюю тег '{ERROR_TAG}' (перший раз)...")
        try:
            create_new_tag_via_panel(page, ERROR_TAG)
            print(f"[OK] Тег '{ERROR_TAG}' створено успішно")
        except Exception as e2:
            logger.error("Failed to create ERROR_TAG: %s", e2)
            raise RuntimeError(f"Не вдалося створити тег '{ERROR_TAG}': {e2}")
    
    delete_tag(page, QUEUE_TAG)


# =========================
# EDIT OPS
# =========================

def ensure_seo_noindex_checked(page: Page) -> None:
    cb = page.locator('[data-qaid="noindex_chbx"]')
    cb.wait_for(state="visible", timeout=30_000)

    try:
        if not cb.is_checked():
            cb.check()
            page.wait_for_timeout(180)
    except Exception:
        recover_ui(page)
        cb.wait_for(state="visible", timeout=30_000)
        if not cb.is_checked():
            cb.check()
            page.wait_for_timeout(180)


def save_and_return(page: Page) -> None:
    """Сохранить и вернуться в список. Поднимает PWTimeoutError если список не появился."""
    btn = page.locator('[data-qaid="save_return_to_list"]').first
    btn.wait_for(state="visible", timeout=30_000)

    try:
        btn.click()
    except Exception:
        recover_ui(page)
        btn.click()

    wait_list(page)  # timeout=30_000 — штатное сохранение
    page.wait_for_timeout(240)


def is_still_on_edit_page(page: Page) -> bool:
    """v2.6.3: Проверяем что после Save мы всё ещё на странице редактирования.
    Это признак валидационной блокировки PROM.
    """
    return "/cms/product/edit/" in page.url


def click_save_and_detect_validation(page: Page) -> bool:
    """v2.6.3: Кликает Save, ждёт 4с и проверяет URL.
    Возвращает True если PROM заблокировал сохранение (остались на edit-странице).
    Возвращает False если сохранение прошло успешно (ушли со страницы).
    """
    btn = page.locator('[data-qaid="save_return_to_list"]').first
    btn.wait_for(state="visible", timeout=30_000)

    try:
        btn.click()
    except Exception:
        recover_ui(page)
        btn.click()

    # Ждём 4с — нормальное сохранение за это время точно уйдёт со страницы.
    # При валидационной блокировке PROM URL не изменится.
    page.wait_for_timeout(4_000)

    if is_still_on_edit_page(page):
        return True  # заблокировано

    # Успешно ушли — дожидаемся полной загрузки списка
    wait_list_short(page, timeout_ms=30_000)
    page.wait_for_timeout(240)
    return False  # всё ок


# =========================
# PROGRESS
# =========================

def print_progress() -> None:
    elapsed = time.time() - start_time
    total = processed_ok + processed_err + processed_ghost + processed_fatal + processed_validation_err
    avg = (elapsed / total) if total else 0.0
    status = f"[OK] {processed_ok} | [ERR] {processed_err}"
    if processed_validation_err > 0:
        status += f" | [VAL_ERR] {processed_validation_err}"
    if processed_ghost > 0:
        status += f" | [GHOST] {processed_ghost}"
    if processed_fatal > 0:
        status += f" | [FATAL] {processed_fatal}"
    status += f" | [TIME] {elapsed:.0f}s | Avg: {avg:.1f}s/item"
    print(f"\r{status}", end="", flush=True)


# =========================
# PROCESS ITEM
# =========================

@dataclass
class ItemResult:
    href: str
    status: str  # ok | err | ghost | skip | fatal | validation_err


def open_item(page: Page, href: str) -> None:
    page.goto("https://my.prom.ua" + href, wait_until="domcontentloaded")


# Счётчик ошибок для каждого href (для определения FATAL)
error_count: dict[str, int] = {}


def process_item(page: Page, href: str, fatal_hrefs: Set[str]) -> ItemResult:
    global processed_ok, processed_err, processed_ghost, processed_fatal, processed_validation_err

    # Проверка blacklist
    if href in fatal_hrefs:
        processed_fatal += 1
        logger.warning("SKIP_FATAL %s (in blacklist)", href)
        return ItemResult(href=href, status="fatal")

    if href in processed_hrefs:
        return ItemResult(href=href, status="skip")

    processed_hrefs.add(href)
    logger.info("OPEN %s", href)

    try:
        open_item(page, href)
        wait_edit(page)

        # Проверка на "призрака"
        if not tag_exists(page, QUEUE_TAG):
            processed_ghost += 1
            logger.warning("GHOST %s - queue tag missing on edit page", href)
            save_and_return(page)
            return ItemResult(href=href, status="ghost")

        # УСПЕХ: ставим noindex и удаляем тег
        ensure_seo_noindex_checked(page)
        delete_tag(page, QUEUE_TAG)
        
        # v2.6.3: Кликаем Save и определяем блокировку по URL (не по timeout)
        validation_blocked = click_save_and_detect_validation(page)

        if validation_blocked:
            processed_validation_err += 1
            logger.warning("VALIDATION_ERROR %s - PROM blocked save (still on edit page)", href)
            print(f"\n[VALIDATION] PROM заблокував збереження для {href}")

            # Выходим через стрелку (задержка уже не нужна — мы уже ждали 4с выше)
            try:
                arrow = page.locator('[data-qaid="previous-icon"]').first
                if arrow.count() == 0:
                    arrow = page.locator('div.b-content__header-icon-arrow').first
                if arrow.count() > 0:
                    print("\n[VALIDATION] Повертаюся через стрілку (PROM заблокував збереження)")
                    arrow.click()
                    wait_list(page)
                    page.wait_for_timeout(500)
                else:
                    goto_list(page)
            except Exception as e:
                logger.error("Failed to return via arrow: %s", e)
                goto_list(page)

            # Помечаем ERROR в списке: сначала текущий список, потом fresh
            def _mark_error_in_list() -> None:
                add_tag_in_list(page, href, ERROR_TAG)
                delete_tag_in_list(page, href, QUEUE_TAG)

            try:
                _mark_error_in_list()
            except Exception:
                logger.warning("Row not found after back arrow, doing fresh reload")
                goto_list_fresh(page)
                set_per_page(page, PER_PAGE)
                apply_queue_filter(page, QUEUE_TAG)
                try:
                    _mark_error_in_list()
                except Exception as e2:
                    logger.error("Failed to mark ERROR in list for %s: %s", href, e2)
                    processed_err += 1
                    return ItemResult(href=href, status="err")

            logger.info("Marked VALIDATION_ERROR in list: %s", href)
            print("[OK] Помічено error в списку")
            return ItemResult(href=href, status="validation_err")

        processed_ok += 1
        logger.info("OK %s total_ok=%s", href, processed_ok)
        
        # Сбрасываем счётчик ошибок после успеха
        if href in error_count:
            del error_count[href]
        
        return ItemResult(href=href, status="ok")

    except Exception as e:
        processed_err += 1
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        logger.error("ERROR %s: %s", href, e)

        # Увеличиваем счётчик ошибок
        error_count[href] = error_count.get(href, 0) + 1

        try:
            page.screenshot(path=f"error_{ts}.png", full_page=True)
        except Exception:
            pass

        # Если это повторная ошибка (2+) -> FATAL
        if error_count[href] >= 2:
            processed_fatal += 1
            logger.error("FATAL %s - repeated error (count=%s)", href, error_count[href])
            try:
                save_fatal_href(href)
                print(f"\n[FATAL] {href} -> добавлено в blacklist")
            except Exception as e2:
                logger.error("Failed to save fatal href %s: %s", href, e2)
            
            # Пытаемся пометить error-tagом
            try:
                recover_ui(page)
                wait_edit(page)
                move_queue_to_error(page)
            except Exception as e2:
                logger.error("ERROR_TAGGING_FAILED %s: %s", href, e2)

            try:
                save_and_return(page)
            except Exception:
                goto_list_fresh(page)

            return ItemResult(href=href, status="fatal")

        # Первая ошибка - помечаем error-tagом и продолжаем
        try:
            recover_ui(page)
            wait_edit(page)
            move_queue_to_error(page)
        except Exception as e2:
            logger.error("ERROR_TAGGING_FAILED %s: %s", href, e2)

        try:
            save_and_return(page)
        except Exception:
            goto_list_fresh(page)

        return ItemResult(href=href, status="err")


# =========================
# PASS: PROCESS ALL PAGES
# =========================

def process_all_pages_once(page: Page, fatal_hrefs: Set[str]) -> int:
    """
    Один проход по всем страницам списка (после фильтра).
    Возвращает количество actionable href, найденных в этом проходе.
    """
    actionable_total = 0

    while True:
        if list_empty(page):
            break

        hrefs = snapshot_actionable_hrefs(page, QUEUE_TAG, fatal_hrefs)
        actionable_total += len(hrefs)

        rows_total = count_rows(page)
        print(f"\n[PAGE] rows={rows_total} actionable={len(hrefs)}")

        # Если на странице есть строки, но actionable==0 — полностью "призрачная" страница
        if rows_total > 0 and len(hrefs) == 0:
            if not click_next_page(page):
                break
            apply_queue_filter(page, QUEUE_TAG)
            continue

        for href in hrefs:
            if (processed_ok + processed_err + processed_ghost + processed_fatal + processed_validation_err) >= MAX_ITEMS_TOTAL:
                raise RuntimeError("MAX_ITEMS_TOTAL reached, stopping safety.")

            process_item(page, href, fatal_hrefs)
            print_progress()
            time.sleep(DELAY_BETWEEN_ITEMS_SEC)

        # Возвращаемся на список, применяем фильтр
        apply_queue_filter(page, QUEUE_TAG)

        if not click_next_page(page):
            break

        apply_queue_filter(page, QUEUE_TAG)

    return actionable_total


# =========================
# MAIN
# =========================

def main() -> None:
    global start_time
    start_time = time.time()

    # Загрузка blacklist
    fatal_hrefs = load_fatal_hrefs()
    if fatal_hrefs:
        logger.info("Loaded %s FATAL hrefs from blacklist", len(fatal_hrefs))
        print(f"[INFO] Завантажено {len(fatal_hrefs)} FATAL товарів з blacklist")

    with sync_playwright() as p:
        if CI_MODE:
            # GitHub Actions: headless Chromium, без профілю.
            # Сесія відновлюється через cookies з PROM_COOKIES (JSON).
            browser = p.chromium.launch(
                headless=True,
                args=["--no-sandbox", "--disable-setuid-sandbox"],
            )
            context = browser.new_context(viewport={"width": 1500, "height": 900})
            page = context.new_page()

            if PROM_COOKIES_JSON:
                # Імпортуємо cookies — вхід без логіну/пароля/2FA
                import json as _json
                cookies = _json.loads(PROM_COOKIES_JSON)
                context.add_cookies(cookies)
                logger.info("CI_MODE: loaded %s cookies from PROM_COOKIES", len(cookies))
                print(f"[COOKIES] Завантажено {len(cookies)} cookies з PROM_COOKIES")
            else:
                # Fallback: логін через логін/пароль (якщо 2FA не ввімкнено)
                login_with_credentials(page, PROM_LOGIN, PROM_PASSWORD)
        else:
            # Локальний режим: збережена сесія з PROFILE_DIR, видимий Chrome
            context = p.chromium.launch_persistent_context(
                PROFILE_DIR,
                headless=PROM_HEADLESS,  # False локально, True якщо відповідно задано env
                viewport={"width": 1500, "height": 900},
                channel="chrome",
            )
            page = context.new_page()
            ensure_logged_in(page)

        goto_list_fresh(page)
        set_per_page(page, PER_PAGE)

        if not apply_queue_filter(page, QUEUE_TAG):
            print("\n[INFO] Товарів з тегом 'noindex' немає — завершую.")
        else:
            for pass_no in range(1, MAX_PASSES + 1):
                print(f"\n\n===== PASS {pass_no}/{MAX_PASSES} =====")
                actionable = process_all_pages_once(page, fatal_hrefs)

                goto_list_fresh(page)
                set_per_page(page, PER_PAGE)

                # Якщо тег зник з фільтру — все оброблено, виходим
                if not apply_queue_filter(page, QUEUE_TAG):
                    print("\n[INFO] Тег 'noindex' зник з фільтру — завершую.")
                    break

                if list_empty(page):
                    print("\n[INFO] Після fresh список порожній — завершую.")
                    break

                if actionable == 0:
                    first_page_actionable = len(snapshot_actionable_hrefs(page, QUEUE_TAG, fatal_hrefs))
                    if first_page_actionable == 0:
                        print("\n[INFO] Залишилися лише призраки (actionable=0). Завершую.")
                        break

        elapsed = time.time() - start_time
        logger.info(
            "FINISH ok=%s err=%s val_err=%s ghost=%s fatal=%s time=%.0fs",
            processed_ok, processed_err, processed_validation_err, processed_ghost, processed_fatal, elapsed
        )

        print("\n" + "=" * 60)
        print("[ГОТОВО] ОБРОБКА ЗАВЕРШЕНА")
        print("=" * 60)
        print(f"[OK] Успішно оброблено: {processed_ok}")
        print(f"[ERR] Помилок (помічено {ERROR_TAG}): {processed_err}")
        if processed_validation_err > 0:
            print(f"[VAL_ERR] Помилок валідації PROM (помічено в списку): {processed_validation_err}")
        if processed_ghost > 0:
            print(f"[GHOST] Аномалії (queue tag відсутній у картці): {processed_ghost}")
        if processed_fatal > 0:
            print(f"[FATAL] FATAL (в blacklist): {processed_fatal}")
        print(f"[TIME] Час виконання: {elapsed:.0f}s ({elapsed/60:.1f} хв)")
        total = processed_ok + processed_err + processed_ghost + processed_fatal + processed_validation_err
        if total > 0:
            print(f"[STATS] Середній час на товар: {elapsed/total:.1f}s")
        print(f"[INFO] Лог: {LOG_FILE}")
        if processed_fatal > 0:
            print(f"[NOTE] Blacklist: {FATAL_FILE.resolve()}")
        print("=" * 60)

        context.close()


if __name__ == "__main__":
    try:
        main()
    except PWTimeoutError as e:
        raise SystemExit(f"Timeout: {e}") from e
    except KeyboardInterrupt:
        print("\n\n[!] Скрипт перервано (Ctrl+C)")
        total = processed_ok + processed_err + processed_ghost + processed_fatal + processed_validation_err
        print(f"Оброблено: OK={processed_ok}, ERR={processed_err}, VAL_ERR={processed_validation_err}, GHOST={processed_ghost}, FATAL={processed_fatal}")
    except RuntimeError as e:
        print(f"\n[X] {e}")
        print("Залогінься вручну і запусти скрипт знову.")
