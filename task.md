stalk@DESKTOP-APMRUCI MINGW64 /c/FullStack/PriceFeedPipeline (main)
$ python scripts/ultra_clean_run.py viatec_dealer
Traceback (most recent call last):
File "C:\FullStack\PriceFeedPipeline\scripts\ultra_clean_run.py", line 50, in <module>
asyncioreactor.install(\_loop)
File "C:\Users\stalk\AppData\Roaming\Python\Python312\site-packages\twisted\internet\asyncioreactor.py", line 304, in install
reactor = AsyncioSelectorReactor(eventloop)
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
File "C:\Users\stalk\AppData\Roaming\Python\Python312\site-packages\twisted\internet\asyncioreactor.py", line 61, in **init**
raise TypeError(
TypeError: ProactorEventLoop is not supported, got: <ProactorEventLoop running=False closed=False debug=False>
Ошибка связана с несовместимостью event loop’а в Windows.

Причина

В Python 3.8+ на Windows по умолчанию используется:

ProactorEventLoop

А Twisted (через AsyncioSelectorReactor) поддерживает только:

SelectorEventLoop

Поэтому при:

asyncioreactor.install(\_loop)

получаешь:

TypeError: ProactorEventLoop is not supported
Решение (правильное)

Нужно принудительно переключить event loop policy ДО создания loop и ДО импорта reactor.

В начале ultra_clean_run.py:
import asyncio

asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
Важно (порядок критичен)

Правильная последовательность:

import asyncio

# 1. Сначала политика

asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

# 2. Потом создаем loop

\_loop = asyncio.new_event_loop()
asyncio.set_event_loop(\_loop)

# 3. Потом reactor

from twisted.internet import asyncioreactor
asyncioreactor.install(\_loop)
Частые ошибки
❌ Политику ставят после new_event_loop()
❌ Импортируют twisted.internet.reactor до install()
❌ Где-то в проекте reactor уже заинициализирован
Альтернатива (если не нужен asyncio)

Можно вообще не использовать AsyncioSelectorReactor, а оставить стандартный reactor:

# просто убрать install()

Но это зависит от архитектуры (если у тебя Scrapy + asyncio — тогда не вариант).

Итог

Проблема не в коде Scrapy, а в:

Windows loop policy (Proactor)
ограничении Twisted

Фикс — принудительно использовать WindowsSelectorEventLoopPolicy.

но через гит хаб актион работает норм паук , если сделать сейчас локально то перестанет там работать?
Нет, не перестанет — если сделать корректно.
Почему сейчас работает в GitHub Actions
В GitHub Actions (Linux runner):

используется SelectorEventLoop по умолчанию

нет ProactorEventLoop

поэтому Twisted работает без костылей

Почему падает локально
На Windows:

дефолт → ProactorEventLoop

Twisted его не поддерживает → ошибка

Правильный фикс (кроссплатформенный)
Добавь условие только для Windows, тогда поведение не изменится в CI:
import asyncioimport sysif sys.platform == "win32": asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())\_loop = asyncio.new_event_loop()asyncio.set_event_loop(\_loop)from twisted.internet import asyncioreactorasyncioreactor.install(\_loop)

Почему это безопасно

Windows → принудительно Selector (фикс ошибки)

Linux (GitHub Actions) → код даже не выполнится

Поведение в CI останется 100% таким же

Когда можно сломать CI
Только если:

убрать условие sys.platform

или начать вручную менять loop policy на Linux

Вывод
Фикс:

локально устранит ошибку

CI не затронет

соответствует нормальной практике для связки asyncio + Twisted на Windows

Что упростить (минимально и правильно)

Добавь platform guard:

import asyncio
import sys
from twisted.internet import asyncioreactor

if sys.platform == "win32":
asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

\_loop = asyncio.new_event_loop()
asyncio.set_event_loop(\_loop)

asyncioreactor.install(\_loop)
