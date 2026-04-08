import json
import re
import os
from pathlib import Path
import requests
from bs4 import BeautifulSoup
from playwright.sync_api import sync_playwright

# ---------------------------
# Настройки URL
# ---------------------------

URLS = {
    "catalog": "https://secur.ua/ajax/startovye-komplekty",
    "product": "https://secur.ua/signalizatsii/gsm-signalizatsii/bezprovodnye-gsm-sygnalizatsii/komplekt-signalizaciyi-ajax-starterkit-bilii-ajax-1629"
}

# ---------------------------
# Базовые заранее определённые селекторы
# ---------------------------

BASE_SELECTORS = {
    "catalog": {
        "product_item": ".product-item",
        "title": ".product-title",
        "price": ".price",
        "availability": ".availability",
        "language_switch": ".lang-switch",
        "pagination_next": ".pagination-next",
        "image": ".product-image img",
    },
    "product": {
        "title": "h1.product-title",
        "price": ".price",
        "availability": ".availability",
        "description": ".product-description",
        "characteristics": ".product-characteristics, .specs-table",
        "images": ".product-gallery img",
        "language_switch": ".lang-switch",
    }
}

REPORT_PATH = Path(r"C:\FullStack\Scrapy\tests\report.json")
REPORT_PATH.parent.mkdir(parents=True, exist_ok=True)

# ---------------------------
# Вспомогательные функции
# ---------------------------

def clean_text(text):
    return re.sub(r'\s+', ' ', text.strip())

def extract_elements(html, selectors):
    soup = BeautifulSoup(html, "lxml")
    result = {}
    for key, sel in selectors.items():
        el = soup.select(sel)
        if not el:
            result[key] = None
        else:
            if len(el) == 1:
                if el[0].name == "img":
                    result[key] = el[0].get("src")
                else:
                    result[key] = clean_text(el[0].get_text())
            else:
                result[key] = [
                    clean_text(e.get_text() if e.name != "img" else e.get("src"))
                    for e in el
                ]
    return result

# ---------------------------
# Генератор CSS-селекторов через Playwright
# ---------------------------

def auto_selectors(page):
    auto = {}

    targets = {
        "title": ["h1", ".title", "[class*=title]"],
        "price": [".price", "[class*=price]"],
        "availability": [".availability", "[class*=stock]", "[class*=nal]"],
        "image": ["img"],
        "description": [".description", "[class*=desc]"],
        "characteristics": [".spec", ".char", "[class*=spec]", "[class*=char]"],
        "product_item": [".product", "[class*=item]"],
        "pagination_next": [".pagination-next", ".next", "[rel=next]"],
        "language_switch": [".lang", "[class*=lang]"],
    }

    for key, patterns in targets.items():
        for selector in patterns:
            element = page.query_selector(selector)
            if element:
                auto[key] = element.evaluate("el => window.CSS && CSS.escape(el.localName) ? el.tagName : el.outerHTML")
                auto[key] = page.locator(selector).first.evaluate("el => window.getComputedStyle(el) ? el.outerHTML : ''")
                xpath_selector = page.locator(selector).first.evaluate("el => window.generateCssSelector ? generateCssSelector(el) : ''")
                css = page.locator(selector).first.evaluate(
                    """el => {
                        const path = [];
                        while (el.parentElement) {
                            let selector = el.tagName.toLowerCase();
                            if (el.id) {
                                selector += "#" + el.id;
                                path.unshift(selector);
                                break;
                            } else {
                                let sib = el;
                                let nth = 1;
                                while ((sib = sib.previousElementSibling) != null) nth++;
                                selector += `:nth-child(${nth})`;
                            }
                            path.unshift(selector);
                            el = el.parentElement;
                        }
                        return path.join(" > ");
                    }"""
                )
                auto[key] = css
                break

    return auto

# ---------------------------
# Сравнение данных
# ---------------------------

def compare_static_dynamic(static_data, dynamic_data):
    for key in static_data:
        if static_data[key] != dynamic_data.get(key):
            return True
    return False

# ---------------------------
# Анализ страницы
# ---------------------------

def analyze_page(page_type, url, selectors):

    resp = requests.get(url, headers={"User-Agent": "Mozilla/5.0"})
    static_html = resp.text
    static_data = extract_elements(static_html, selectors)

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_page()
        page.goto(url, wait_until="networkidle")
        dynamic_html = page.content()
        dynamic_data = extract_elements(dynamic_html, selectors)

        auto = auto_selectors(page)

        browser.close()

    requires_playwright = compare_static_dynamic(static_data, dynamic_data)

    summary = {
        "requires_playwright": requires_playwright,
        "recommended_spider": "Scrapy + Playwright" if requires_playwright else "Scrapy",
        "note": "Данные подгружаются динамически → нужен Playwright" if requires_playwright else "Серверный HTML → Playwright не нужен"
    }

    return {
        "url": url,
        "manual_selectors": selectors,
        "auto_selectors": auto,
        "static_data_sample": static_data,
        "dynamic_data_sample": dynamic_data,
        "summary": summary
    }

# ---------------------------
# Генерация отчёта
# ---------------------------

def main():
    report = {}
    for page_type, url in URLS.items():
        print(f"Analyzing {page_type}...")
        report[page_type] = analyze_page(page_type, url, BASE_SELECTORS[page_type])

    with open(REPORT_PATH, "w", encoding="utf-8") as f:
        json.dump(report, f, ensure_ascii=False, indent=4)

    print(f"Report saved to {REPORT_PATH}")

if __name__ == "__main__":
    main()
