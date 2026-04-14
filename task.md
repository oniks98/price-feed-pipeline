посмотри
C:\FullStack\PriceFeedPipeline\.github\workflows\pipeline.yml
C:\FullStack\PriceFeedPipeline\suppliers\spiders\secur\feed_full.py

возможно ли сделать паука чуть агресивнее - может 2 паралельніх запроса или сейчас сколько ? Все равно есть фолбек на фид и товар оттуда берется в лучае бана по запросу, а то сейчас 2ч 20мин паук работает, хотелось бі 1ч 30мин

Логичное ли єто решение?

Аналіз поточної ситуації
Вузьке місце — Playwright-запити. Поточні налаштування:

Параметр Зараз Ефект
CONCURRENT_REQUESTS_PER_DOMAIN 1 строго 1 запит одночасно
DOWNLOAD_DELAY 5 рандом 2.5–7.5 сек між запитами
AUTOTHROTTLE_TARGET_CONCURRENCY 1.0 AutoThrottle тримає ≈1 паралельний
CONCURRENT_REQUESTS 2 глобально 2, але per-domain=1 → не використовується
Реальний throughput: ≈ 1 сторінка / 5с = 12 товарів/хв

Щоб з 2год 20хв → 1год 30хв потрібно прискорення ≈55%, тобто ≈18–20 товарів/хв.

Ризик: є errback_product → при бані yield з фід-даними, товар не губиться ✅

Рішення: 2 паралельні Playwright-сесії
python

# suppliers/spiders/secur/feed_full.py

# ── Антибот: 2 паралельні запити, затримка 3–6 сек ──────────────

"CONCURRENT_REQUESTS": 4, # було 2
"CONCURRENT_REQUESTS_PER_DOMAIN": 2, # було 1 ← КЛЮЧОВА ЗМІНА
"DOWNLOAD_DELAY": 3, # було 5
"RANDOMIZE_DOWNLOAD_DELAY": True, # 3 × 0.5..1.5 → 1.5–4.5 сек

# ── AutoThrottle: тримає 2 паралельних, відступає при гальмуванні ──

"AUTOTHROTTLE_ENABLED": True,
"AUTOTHROTTLE_START_DELAY": 3, # було 5
"AUTOTHROTTLE_MAX_DELAY": 60,
"AUTOTHROTTLE_TARGET_CONCURRENCY": 2.0, # було 1.0 ← КЛЮЧОВА ЗМІНА
"AUTOTHROTTLE_DEBUG": False,
І один Playwright-контекст розширюємо щоб витримував 2 вкладки:

python
"PLAYWRIGHT_CONTEXTS": {
"default": {
"user_agent": (
"Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
"AppleWebKit/537.36 (KHTML, like Gecko) "
"Chrome/133.0.0.0 Safari/537.36"
),
"extra_http_headers": {
"Accept-Language": "uk-UA,uk;q=0.9,en-US;q=0.8,en;q=0.7",
}, # Дозволяємо 2 паралельні вкладки в одному браузері
"max_concurrent_pages": 2, # ← ДОДАТИ
}
},
Очікуваний результат
До Після
Паралельних Playwright 1 2
Затримка між запитами 2.5–7.5 сек 1.5–4.5 сек
Throughput ~12 товарів/хв ~20–24 товарів/хв
Час роботи ~2год 20хв ~1год 20–30хв
Чому це безпечно
Fallback є — errback_product yields з фід-даними при будь-якому бані
AutoThrottle сам знизить швидкість якщо secur.ua починає гальмувати (збільшить delay до 60с)
2 паралельних — це дуже помірно (не 5–10), Cloudflare зазвичай реагує на набагато агресивніше
Один браузер, 2 вкладки — менш підозріло ніж 2 окремих браузери
Конкретний diff для feed_full.py
python

# БУЛО:

"CONCURRENT_REQUESTS": 2,
"CONCURRENT_REQUESTS_PER_DOMAIN": 1,
"DOWNLOAD_DELAY": 5,
"AUTOTHROTTLE_START_DELAY": 5,
"AUTOTHROTTLE_TARGET_CONCURRENCY": 1.0,

# СТАЛО:

"CONCURRENT_REQUESTS": 4,
"CONCURRENT_REQUESTS_PER_DOMAIN": 2,
"DOWNLOAD_DELAY": 3,
"AUTOTHROTTLE_START_DELAY": 3,
"AUTOTHROTTLE_TARGET_CONCURRENCY": 2.0,
І в PLAYWRIGHT_CONTEXTS["default"] додати "max_concurrent_pages": 2.

Більше нічого міняти не потрібно — pipeline.yml, retry логіка, errback — все залишається як є.
