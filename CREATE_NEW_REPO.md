# Migration Plan: price-feed-pipeline → clean repo

## Что где живёт (итог)

| Ветка         | Содержимое                                                                                                             |
| ------------- | ---------------------------------------------------------------------------------------------------------------------- |
| `main`        | код + все reference data (sku_map, image_cache, category, keywords, manufacturers, mapping rules, coefficients и т.д.) |
| `data-latest` | generated snapshot: `_old.csv`, `import_products.csv`, `*.xml`, `merged.csv` и т.д.                                    |

---

## Phase 1 — Остановить старый pipeline

**GitHub UI → старый repo:**

```
Settings → Actions → General → Disable actions
```

Делать первым — пока идёт миграция старый repo не должен ничего пушить в `data-latest`.

---

## Phase 2 — Переименовать старый repo

**GitHub UI:**

```
Settings → General → Repository name → price-feed-pipeline-archive → Rename
```

> ⚠️ После переименования локальная папка `PriceFeedPipeline` всё ещё имеет remote
> `https://github.com/oniks98/price-feed-pipeline.git` — но теперь это имя принадлежит
> новому repo. Из старой папки **больше не пушить** ничего.

---

## Phase 3 — Создать новый пустой repo

**GitHub UI → github.com/new:**

```
Name:       price-feed-pipeline
Public:     ✅  ← обязательно Public
README:     ❌
.gitignore: ❌
License:    ❌
```

> Repo должен быть **Public**. Фиды отдаются внешним сервисам через
> `raw.githubusercontent.com`. Private repo сломает все публичные raw-ссылки
> без авторизации.

---

## Phase 3.1 — Сразу отключить Actions в новом repo

Сделать до любого пуша кода, иначе расписание сработает
сразу после появления `.github/workflows/`.

**GitHub UI → новый repo:**

```
Settings → Actions → General → Disable actions
```

---

## Phase 4 — Подготовить чистый `main`

### 4.1 Скопировать проект без мусора

```bash
cd /c/FullStack

# Очистить целевые папки если остались с прошлого раза
rm -rf PriceFeedPipeline-clean
rm -rf data-latest-snapshot

rsync -av \
  --exclude='.git' \
  --exclude='.env' \
  --exclude='pw-profile' \
  --exclude='goog-profile' \
  --exclude='__pycache__' \
  --exclude='*.log' \
  --exclude='*.pyc' \
  --exclude='data/eserver/' \
  --exclude='data/output/' \
  --exclude='data/markets/*.xml' \
  --exclude='data/merged.csv' \
  --exclude='data/merged_prev.csv' \
  --exclude='data/prom_import_status.json' \
  --exclude='data/*/*_old.csv' \
  --exclude='data/*/import_products.csv' \
  PriceFeedPipeline/ PriceFeedPipeline-clean/

cd PriceFeedPipeline-clean
```

> `data/eserver/` исключается целиком — поставщик мёртвый, в `SUPPLIER_CONFIG`
> отсутствует. Страховка — старый archive repo лежит месяц.

### 4.2 Сканирование секретов перед пушем в Public repo

Repo будет Public. До первого `git push` обязательно:

```bash
rg -n "TOKEN|PASSWORD|SECRET|API_KEY|COOKIE|Bearer|ghp_|sk-" .
```

Если вывод не пустой — разобрать каждый результат. Убедиться что:

- `.env` файлы не попали (rsync уже исключил `suppliers/.env`)
- в коде нет hardcoded credentials
- в конфигах нет живых токенов

Только при пустом выводе (или после зачистки) — продолжать.

### 4.3 Проверить `.gitignore`

`.gitignore` в `main` должен исключать все генерируемые файлы:

```gitignore
# Secrets
.env
**/.env

# Generated data files — never commit to main
data/markets/*.xml
data/markets/*_prev.xml
data/**/import_products.csv
data/**/*_old.csv
data/merged.csv
data/merged_prev.csv
data/prom_import_status.json

# Dead suppliers
data/eserver/

# Logs and cache
*.log
__pycache__/
*.pyc

# Browser profiles
pw-profile/
goog-profile/
```

### 4.4 Убедиться что reference data присутствует

В `main` должны быть все рабочие справочники по каждому активному supplier
(`viatec`, `secur`, `lp`):

```bash
find data -name "sku_map.json" -o -name "image_cache.json"
```

Ожидаемый результат:

```
data/lp/sku_map.json
data/lp/image_cache.json
data/secur/sku_map.json
data/secur/image_cache.json
data/viatec/sku_map.json
data/viatec/image_cache.json
```

Дополнительно убедиться что на месте:

```bash
find data \
  -name "*_category.csv" \
  -o -name "*_keywords.csv" \
  -o -name "*_manufacturers.csv" \
  -o -name "*_mapping_rules.csv" \
  -o -path "*/dictionary_attribute/*" \
  -o -name "*mappings.xlsx" \
  -o -name "*_coefficients.csv" \
  -o -name "*_royalty.xlsx" \
  -o -name "merchant_rule.csv" \
  | sort
```

Если какого-то файла нет — скопировать вручную из старой папки:

```bash
# Пример
cp /c/FullStack/PriceFeedPipeline/data/lp/image_cache.json data/lp/
```

### 4.5 Инициализировать и запушить

```bash
git init
git branch -M main
git add .

# Проверка — что staged, в коротком формате
git status --short

# Финальная проверка — убедиться что generated файлы НЕ попали в индекс
git ls-files | rg "(_old\.csv|import_products\.csv|merged(_prev)?\.csv|prom_import_status\.json|data/markets/.*\.xml$)"
# Вывод должен быть пустым
```

> `git status --ignored` покажет ignored-файлы в отдельной секции — это нормально.
> Главное: в staged и untracked не должно быть generated файлов.
> Команда `git ls-files | rg ...` проверяет только то, что уже в индексе.

```bash
git commit -m "chore: initial clean snapshot"
git remote add origin https://github.com/oniks98/price-feed-pipeline.git
git push -u origin main
```

---

## Phase 5 — Подготовить чистый `data-latest`

Берём текущий снимок `data-latest` из archive repo и пушим как новую ветку без истории.

```bash
cd /c/FullStack

# Клонировать только ветку data-latest из архивного repo
git clone --single-branch --branch data-latest \
  https://github.com/oniks98/price-feed-pipeline-archive.git \
  data-latest-snapshot

cd data-latest-snapshot

# Убрать старую историю
rm -rf .git

# Инициализировать чистый repo с веткой data-latest
git init
git checkout -b data-latest

# Все файлы из скриншота уже здесь
git add .
git commit -m "chore(data): initial clean data snapshot"

git remote add origin https://github.com/oniks98/price-feed-pipeline.git
git push -u origin data-latest
```

> ⚠️ `.gitignore` в `data-latest` **не заменять** на `.gitignore` из `main`.
> В `data-latest` файлы `_old.csv`, `import_products.csv`, `*.xml`, `merged.csv`
> должны трекаться — это и есть смысл ветки. У `data-latest` свой `.gitignore`,
> который не игнорирует `data/`. Он остаётся как есть.

---

## Phase 6 — Настроить GitHub (ПЕРЕД запуском Actions)

> ⚠️ Secrets и permissions нужно настроить **до** любого ручного запуска.

### 6.1 Actions permissions

```
Settings → Actions → General
→ Workflow permissions: Read and write permissions ✅
```

### 6.2 Secrets

> ⚠️ GitHub показывает только **имена** секретов, не значения — значения скрыты навсегда.
> Значения брать из своего хранилища, локальных `.env`, или создавать новые токены.
>
> `GITHUB_TOKEN` — **не копировать**, он генерируется автоматически для каждого repo.
> PAT — если workflow его использует, нужно создать заново или взять значение
> из локального хранилища.

Посмотреть список имён секретов в старом repo:

```
price-feed-pipeline-archive → Settings → Secrets and variables → Actions
```

Добавить каждый в новый repo:

```
price-feed-pipeline → Settings → Secrets and variables → Actions → New repository secret
```

### 6.3 Branch protection (если был)

```
Settings → Branches → Add branch protection rule
```

Восстановить те же правила, что были в старом repo.

---

## Phase 7 — Включить Actions и тестовый запуск

Только после Phase 6.

> ⚠️ Как только Actions включены — `schedule` уже активен.
> Если время до ближайшего cron большое, можно просто включить и сразу запустить ручной тест.
> Если cron может сработать раньше чем успеешь проверить — временно закомментировать
> `schedule:` в workflow файле до первого успешного прогона, потом раскомментировать.

```
Settings → Actions → General → Allow all actions ✅
```

Сразу после включения — ручной запуск, не ждать расписания:

```
Actions → выбрать workflow → Run workflow
```

Проверить последовательно:

1. restore `*_old.csv` из `data-latest` ✅
2. генерация `import_products.csv` ✅
3. генерация XML-фидов ✅
4. push в `data-latest` через amend ✅
5. commit `sku_map.json` + `image_cache.json` в `main` ✅

После успешного прогона — раскомментировать `schedule:` если был закомментирован.

---

## Phase 8 — Обновить локальную рабочую папку

```bash
cd /c/FullStack

# Старая папка → в архив
mv PriceFeedPipeline PriceFeedPipeline-old-local

# Чистая папка → основная
mv PriceFeedPipeline-clean PriceFeedPipeline

# Временную папку со snapshot data-latest удалить
rm -rf data-latest-snapshot
```

Дальше работать только в `PriceFeedPipeline`.

---

## Phase 9 — Удалить archive (через месяц)

Когда новый repo стабильно работает:

```
github.com/oniks98/price-feed-pipeline-archive
→ Settings → General → Danger Zone → Delete this repository
```

---

## Итоговая структура нового repo

```
main
├── код проекта (.github/, suppliers/, scripts/ и т.д.)
├── data/
│   ├── lp/
│   │   ├── sku_map.json
│   │   ├── image_cache.json
│   │   ├── lp_category.csv
│   │   ├── lp_keywords.csv
│   │   ├── lp_manufacturers.csv
│   │   └── lp_mapping_rules.csv
│   ├── secur/
│   │   ├── sku_map.json
│   │   ├── image_cache.json
│   │   ├── secur_category.csv
│   │   ├── secur_keywords.csv
│   │   └── ...
│   ├── viatec/
│   │   ├── sku_map.json
│   │   ├── image_cache.json
│   │   ├── viatec_category.csv
│   │   └── ...
│   └── markets/
│       ├── *mappings.xlsx
│       └── (*.xml исключены .gitignore)
└── .gitignore  ← закрывает все generated файлы + data/eserver/ + .env

data-latest  (один коммит, без истории)
├── data/lp/import_products.csv
├── data/lp/lp_old.csv
├── data/markets/*.xml
├── data/markets/*_prev.xml
├── data/secur/import_products.csv
├── data/secur/secur_old.csv
├── data/viatec/import_products.csv
├── data/viatec/viatec_old.csv
├── data/merged.csv
├── data/merged_prev.csv
├── data/prom_import_status.json
└── .gitignore  ← свой, не трогать
```
