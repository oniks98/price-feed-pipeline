"""
Custom Playwright download handler з інтеграцією playwright-stealth.

ЯК ПРАЦЮЄ:
  Cloudflare виконує JS-перевірку під час першого завантаження сторінки.
  PageMethod("add_init_script") спрацьовує ПІСЛЯ page.goto() — надто пізно.
  context.add_init_script() реєструє скрипт на рівні контексту → він
  виконується ДО будь-якого JS на кожній сторінці контексту, тобто ДО
  Cloudflare challenge.

  1. StealthPlaywrightDownloadHandler.`_create_browser_context`
     → викликає super() (оригінальний scrapy-playwright)
     → потім Stealth.apply_stealth_async(context)
       → context.add_init_script(stealth_payload)
  2. Кожна нова сторінка в цьому контексті автоматично отримує stealth-патчі.

ПАТЧІ playwright-stealth (за замовчуванням усі увімкнені):
  - navigator.webdriver → undefined (головний сигнал бота)
  - window.chrome.runtime → симулює реальний Chrome
  - navigator.plugins → правдоподібний список плагінів
  - navigator.permissions → обходить перевірку Notification API
  - navigator.userAgent / userAgentData → без "HeadlessChrome"
  - webgl vendor/renderer → Intel замість SwiftShader
  - та інші (~15 скриптів)
"""

from scrapy_playwright.handler import ScrapyPlaywrightDownloadHandler
from playwright_stealth import Stealth

# Має збігатися з user_agent у PLAYWRIGHT_CONTEXTS["default"] у spider'і.
# Stealth патчить JS navigator.userAgent / navigator.userAgentData до цього значення.
_SPIDER_UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/133.0.0.0 Safari/537.36"
)

# Єдиний глобальний інстанс — Stealth не зберігає стан між викликами.
_STEALTH = Stealth(
    navigator_user_agent_override=_SPIDER_UA,  # патчить JS navigator.userAgent
    navigator_languages_override=("uk-UA", "uk"),  # відповідає Accept-Language в фіді
    navigator_platform_override="Win32",
    chrome_runtime=False,  # True потрібен лише якщо сайт перевіряє chrome.runtime.id
)


class StealthPlaywrightDownloadHandler(ScrapyPlaywrightDownloadHandler):
    """
    Розширює scrapy-playwright: застосовує playwright-stealth на рівні
    BrowserContext одразу після його створення.

    Замінює стандартний ScrapyPlaywrightDownloadHandler у DOWNLOAD_HANDLERS.
    Більше жодних змін у spider не потрібно.
    """

    async def _create_browser_context(
        self,
        name: str,
        context_kwargs,
        spider=None,
    ):
        wrapper = await super()._create_browser_context(name, context_kwargs, spider)
        await _STEALTH.apply_stealth_async(wrapper.context)
        if spider is not None:
            spider.logger.debug(
                f"🥷 playwright-stealth applied → context '{name}'"
            )
        return wrapper
