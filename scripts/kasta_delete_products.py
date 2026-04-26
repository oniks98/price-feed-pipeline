"""kasta_delete_products.py
Відмічає рівно KEEP_CHECKED рядків у ReactVirtualized-таблиці Kasta.

Архітектура:
  ReactVirtualized рендерить тільки ~20 рядків у вʼюпорті.
  Скролимо .ReactVirtualized__Grid (overflow:auto), не window.
  Загальну кількість беремо з innerScrollContainer.style.height / ROW_HEIGHT.
  Чекаємо стабілізації висоти перед стартом (дані можуть підвантажуватись).

Підготовка (один раз):
  1. Закрий всі вікна Chrome
  2. Запусти Chrome з дебагом (PowerShell):
     Start-Process "C:/Program Files/Google/Chrome/Application/chrome.exe" `
       -ArgumentList "--remote-debugging-port=9222","--user-data-dir=C:/chrome-kasta-debug"
  3. Залогінься на hub.kasta.ua вручну
  4. Запусти скрипт: python scripts/kasta_bulk_select.py
"""

import asyncio
import sys

from playwright.async_api import Browser, BrowserContext, Page, async_playwright

CDP_URL = "http://localhost:9222"
TARGET_URL = (
    "https://hub.kasta.ua/products"
    "?contract_id=bd045b2c-ceb9-4c9e-a3ba-cc414e5e76d9"
    "&status=OnSale&status=ZeroStock"
)

KEEP_CHECKED = 900  # ліміт Kasta <= 1000, беремо з запасом

# tabindex="0" є тільки на основному scrollable grid (не на лівій фіксованій колонці).
# Лівий grid (чекбокси, overflow:hidden, 23px) не має tabindex → querySelector
# без уточнення повертав його першим → height=40px (1 рядок).
GRID_SEL      = '.ReactVirtualized__Grid[tabindex="0"]'
INNER_SEL     = '.ReactVirtualized__Grid[tabindex="0"] .ReactVirtualized__Grid__innerScrollContainer'
ROW_HEIGHT_PX = 40
SCROLL_OVERLAP = 40        # 1 рядок перекриття між кроками

RENDER_WAIT_MS    = 400    # пауза після скролу (RV ре-рендерить)
CLICK_INTER_MS    = 50     # пауза між кліками в батчі (без DevTools React не встигає setState)
SELECTION_STABLE_ROUNDS = 3   # разів поспіль кількість чекбоксів не міняється
SELECTION_POLL_MS = 200   # інтервал опитування при стабілізації
SELECTION_TIMEOUT_MS = 8_000  # макс чекання стабілізації
CLICK_BATCH_SIZE  = 10     # React не встигає при >30 кліків підряд
LOAD_TIMEOUT_MS   = 30_000
STABLE_POLL_MS    = 800    # інтервал перевірки висоти при очікуванні
STABLE_ROUNDS     = 3      # скільки разів поспіль висота має не змінюватись

_MODAL             = ".ant-modal-content"
_MODAL_DISMISS     = ["Пізніше", "Позніше", "Later", "Dismiss"]
_MODAL_CLOSE_BTN   = "button.ant-modal-close, [aria-label='Close']"
# Клікаємо по .ant-checkbox (wrapper), не по input —
# React controlled checkbox вимагає event на wrapper, а не на input.
_ROW_UNCHECKED     = "div.checkbox .ant-checkbox:not(.ant-checkbox-checked)"
_ROW_CHECKED       = "div.checkbox .ant-checkbox-checked"


async def get_page(browser: Browser) -> Page:
    contexts = browser.contexts
    ctx: BrowserContext = contexts[0] if contexts else await browser.new_context()
    pages = ctx.pages
    return pages[0] if pages else await ctx.new_page()


async def dismiss_modal(page: Page) -> bool:
    """Закриває модалку і чекає поки вона зникне."""
    if not await page.locator(_MODAL).is_visible():
        return False
    print("  🔔 Виявлено модалку — закриваємо...")
    for text in _MODAL_DISMISS:
        btn = page.get_by_role("button", name=text, exact=True)
        if await btn.count() > 0:
            await btn.click()
            break
    else:
        close = page.locator(_MODAL_CLOSE_BTN).first
        if await close.count() > 0:
            await close.click()
        else:
            await page.keyboard.press("Escape")
    try:
        await page.locator(_MODAL).wait_for(state="hidden", timeout=5_000)
        print("  ✅ Модалку закрито")
    except Exception:
        print("  ⚠️  Модалка не зникла за 5с")
    await page.wait_for_timeout(300)
    return True


def _parse_height_px(raw: str) -> int:
    """'164600px' → 164600. Повертає 0 при помилці."""
    try:
        return int(raw.replace("px", "").strip())
    except (ValueError, AttributeError):
        return 0


async def _get_inner_height(page: Page) -> int:
    """Поточна висота innerScrollContainer в px."""
    raw: str = await page.evaluate(
        f"""
        () => {{
            const el = document.querySelector('{INNER_SEL}');
            return el ? el.style.height : '0px';
        }}
        """
    )
    return _parse_height_px(raw)


async def wait_for_grid_stable(page: Page) -> int:
    """Чекає поки innerScrollContainer.style.height перестане змінюватись.

    Повертає стабільну кількість рядків.
    Проблема: після завантаження сторінки RV виставляє висоту поступово
    (може починати з 40px і зростати до реального значення).
    """
    print("  ⏳ Чекаємо завантаження таблиці...")
    prev_height = -1
    stable = 0
    attempts = 0
    max_attempts = 30  # 30 * 800ms = 24s макс

    while stable < STABLE_ROUNDS and attempts < max_attempts:
        await page.wait_for_timeout(STABLE_POLL_MS)
        h = await _get_inner_height(page)
        attempts += 1
        if h == prev_height and h > 0:
            stable += 1
        else:
            stable = 0
            if h != prev_height:
                rows = h // ROW_HEIGHT_PX
                print(f"  ↕️  Висота: {h}px → {rows} рядків")
        prev_height = h

    total = prev_height // ROW_HEIGHT_PX if prev_height > 0 else 0
    if total == 0:
        print("  ⚠️  Grid не завантажився — debug:")
        await _debug(page)
    else:
        print(f"  📦 Стабільно: {prev_height}px → {total} рядків")
    return total


async def _click_batch(page: Page, locator, count: int) -> int:
    """Клікає перші `count` елементів через нативний Playwright click.

    Playwright генерує повний ланцюжок mousedown/mouseup/click з isTrusted=true.
    JS evaluate дає isTrusted=false → React/Kasta може ігнорувати подію.
    Після кожного кліку .first автоматично вказує на наступний unchecked.
    """
    clicked = 0
    for _ in range(count):
        try:
            await locator.first.click(timeout=2_000)
            clicked += 1
            await page.wait_for_timeout(CLICK_INTER_MS)  # React потребує час setState без DevTools
        except Exception:
            break  # елемент зник під час re-render — зупиняємо батч
    return clicked


async def wait_selection_stable(page: Page, expected: int) -> int:
    """Чекає поки кількість видимих чекбоксів не стабілізується.

    Race condition: без DevTools браузер працює швидше, React
    не встигає оновити selectedRowKeys до відправки запиту на видалення.
    Повертає стабільну кількість або останнє значення після таймауту.
    """
    rounds = SELECTION_TIMEOUT_MS // SELECTION_POLL_MS
    prev = -1
    stable = 0

    for _ in range(rounds):
        await page.wait_for_timeout(SELECTION_POLL_MS)
        count = await page.locator(_ROW_CHECKED).count()

        if count == prev:
            stable += 1
        else:
            stable = 0

        if stable >= SELECTION_STABLE_ROUNDS and count >= expected:
            return count

        prev = count

    print(f"  ⚠️  Стабілізація по selection не досягнута за {SELECTION_TIMEOUT_MS}мс, поточно: {prev}")
    return prev


async def select_rows_by_scrolling(page: Page, target: int) -> int:
    """Скролить ReactVirtualized__Grid і клікає по target незнятих рядків.

    Використовує нативні Playwright кліки (isTrusted=true) замість JS evaluate,
    щоб React synthetic event pipeline коректно оновлював selectedRowKeys.
    Повертає кількість клікнутих.
    """
    await page.evaluate(
        f"document.querySelector('{GRID_SEL}').scrollTop = 0"
    )
    await page.wait_for_timeout(RENDER_WAIT_MS)

    unchecked = page.locator(_ROW_UNCHECKED)
    total_clicked = 0
    step = 0

    while total_clicked < target:
        remaining = target - total_clicked
        step += 1

        visible = await unchecked.count()
        batch_size = min(visible, remaining, CLICK_BATCH_SIZE)

        clicked = await _click_batch(page, unchecked, batch_size)
        total_clicked += clicked

        if clicked > 0:
            await page.wait_for_timeout(150)
            print(f"  🖱️  Крок {step}: +{clicked}  → разом {total_clicked}/{target}")

        if total_clicked >= target:
            break

        if clicked == 0 and visible == 0:
            # Немає незнятих у вʼюпорті → скролимо
            pass

        result: dict = await page.evaluate(
            f"""
            () => {{
                const g = document.querySelector('{GRID_SEL}');
                const pageStep = g.clientHeight - {SCROLL_OVERLAP};
                g.scrollTop += pageStep;
                return {{
                    atBottom: g.scrollTop + g.clientHeight >= g.scrollHeight - 1,
                }};
            }}
            """
        )
        await page.wait_for_timeout(RENDER_WAIT_MS)

        if result["atBottom"]:
            remaining = target - total_clicked
            if remaining > 0:
                visible = await unchecked.count()
                batch_size = min(visible, remaining, CLICK_BATCH_SIZE)
                clicked = await _click_batch(page, unchecked, batch_size)
                if clicked > 0:
                    await page.wait_for_timeout(150)
                total_clicked += clicked
                if clicked > 0:
                    print(f"  🖱️  Дно: +{clicked}  → разом {total_clicked}")
            break

    return total_clicked


async def _debug(page: Page) -> None:
    info: dict = await page.evaluate(
        """
        () => ({
            rv_grid:      document.querySelectorAll('.ReactVirtualized__Grid').length,
            rv_inner:     document.querySelectorAll('.ReactVirtualized__Grid__innerScrollContainer').length,
            inner_height: (() => { const e = document.querySelector('.ReactVirtualized__Grid__innerScrollContainer'); return e ? e.style.height : 'n/a'; })(),
            all_input:    document.querySelectorAll('input.ant-checkbox-input').length,
            div_checkbox: document.querySelectorAll('div.checkbox input').length,
            modal:        document.querySelectorAll('.ant-modal-content').length,
        })
        """
    )
    print("  🔍 Debug:")
    for k, v in info.items():
        print(f"     {k}: {v}")


async def wait_for_page_ready(page: Page) -> None:
    try:
        await page.wait_for_load_state("networkidle", timeout=LOAD_TIMEOUT_MS)
    except Exception:
        pass
    await page.wait_for_timeout(1_000)
    await dismiss_modal(page)


async def main() -> None:
    async with async_playwright() as pw:
        print(f"🔌 Підключення до Chrome на {CDP_URL}...")
        try:
            browser = await pw.chromium.connect_over_cdp(CDP_URL)
        except Exception as exc:
            print(f"\n❌ Не вдалося підключитись: {exc}")
            sys.exit(1)

        page = await get_page(browser)
        print("🌐 Переходимо на сторінку товарів...")
        await page.goto(TARGET_URL, timeout=LOAD_TIMEOUT_MS)
        await wait_for_page_ready(page)

        batch = 0
        total_deleted = 0

        while True:
            batch += 1
            print(f"\n{'═' * 52}")
            print(f"🔄 Раунд {batch}")

            # Чекаємо стабільного розміру таблиці
            total = await wait_for_grid_stable(page)
            if total == 0:
                print("\n🏁 Товарів не залишилось або таблиця не завантажилась.")
                break

            target = min(KEEP_CHECKED, total)
            print(f"  🎯 Ціль: {target} з {total}")

            clicked = await select_rows_by_scrolling(page, target)

            # Чекаємо стабілізації React-стану до того як юзер натисне Delete
            checked = await wait_selection_stable(page, clicked)
            print(f"  ✅ Стабілізовано: {checked} чекбоксів в DOM")

            # Fallback: жорстка пауза якщо DOM-кількість не відповідає очікуваній
            if checked < clicked:
                print(
                    f"  ⚠️  DOM чекбоксів ({checked}) < клікнутих ({clicked}), "
                    "даємо 1.5с додатково..."
                )
                await page.wait_for_timeout(1_500)

            # Повертаємось на початок для перевірки
            await page.evaluate(
                f"document.querySelector('{GRID_SEL}').scrollTop = 0"
            )
            await page.wait_for_timeout(RENDER_WAIT_MS)
            visible_checked = await page.locator(_ROW_CHECKED).count()

            print(
                f"☑️  Клікнуто: {clicked}  |  "
                f"Видимих відмічених (top): {visible_checked}"
            )

            if clicked == 0:
                print("\n⚠️  Нічого не вибрано:")
                await _debug(page)
                break

            total_deleted += clicked
            print(f"\n👉 ~{clicked} товарів відмічено. Видаляй вручну в браузері.")
            print(f"   (всього за сесію: ~{total_deleted})")
            print("   Enter  — наступний раунд")
            print("   q      — вийти")

            try:
                answer = input("> ").strip().lower()
            except (EOFError, KeyboardInterrupt):
                answer = "q"

            if answer == "q":
                print("\n👋 Вихід.")
                break

            print("🔄 Оновлення сторінки...")
            await page.reload()
            await wait_for_page_ready(page)


if __name__ == "__main__":
    asyncio.run(main())
