#!/usr/bin/env python3
"""
Сбор ссылок комплексов DomClick.

Логика:
- Открываем первую страницу поиска
- Определяем количество страниц по элементам data-e2e-id="paginate-item-*"
- Идем по оффсетам с шагом 20: 0, 20, 40, ... до (pagesCount-1)*20
- На каждой странице выполняем page.evaluate, чтобы собрать все href содержащие "/complexes/"
- Сохраняем уникальные ссылки в JSON файл
"""
import asyncio
import json
from typing import List, Set
from pathlib import Path

# Директория текущего скрипта
PROJECT_ROOT = Path(__file__).resolve().parent

from browser_manager import create_browser, create_browser_page


SEARCH_BASE = (
    "https://ufa.domclick.ru/search?deal_type=sale&category=living&offer_type=complex"
    "&aids=19186&sort=qi&sort_dir=desc&offset={offset}"
)


async def wait_offers_loaded(page) -> None:
    """Ожидаем появления карточек с предложениями на странице."""
    try:
        await page.waitForSelector('[data-e2e-id="offers-list__item"]', {"timeout": 60000})
    except Exception as e:
        # эмулируем сбой шага, чтобы сработали ретраи/перезапуск
        raise TimeoutError("Не найден список офферов [data-e2e-id='offers-list__item']") from e


async def get_pages_count(page) -> int:
    """Возвращает количество страниц пагинации на текущем поиске."""
    script = """
    () => {
      const nodes = Array.from(document.querySelectorAll('[data-e2e-id^="paginate-item-"]'));
      return nodes.length || 1;
    }
    """
    try:
        count = await page.evaluate(script)
        return int(count) if isinstance(count, (int, float)) else 1
    except Exception:
        return 1


async def collect_complex_links_on_page(page) -> List[str]:
    """Собирает все ссылки на комплексы на текущей странице (через page.evaluate)."""
    script = """
    () => {
      const items = document.querySelectorAll('[data-e2e-id="offers-list__item"]');
      const urls = Array.from(items)
        .flatMap(item => Array.from(item.querySelectorAll('a')))
        .map(a => a.getAttribute('href') || a.href)
        .filter(Boolean)
        .map(href => { try { return new URL(href, location.origin); } catch { return null; } })
        .filter(Boolean)
        .filter(u => u.href.includes('domclick.ru/search?address='))
        .filter(u => !u.href.includes('&rooms='))
        .map(u => u.href);
      return Array.from(new Set(urls));
    }
    """
    try:
        links = await page.evaluate(script)
        return list(links or [])
    except Exception:
        return []


async def run() -> None:
    browser, proxy_url = await create_browser(headless=False)
    page = await create_browser_page(browser)

    try:
        # Стартовая страница с offset=0
        start_url = SEARCH_BASE.format(offset=0)
        print(f"Открываю: {start_url}")
        attempts = 0
        while True:
            try:
                await page.goto(start_url,  timeout=60000)
                await wait_offers_loaded(page)
                break
            except Exception as e:
                attempts += 1
                print(f"Ошибка открытия стартовой страницы: {e} (попытка {attempts}/3)")
                if attempts >= 3:
                    from browser_manager import restart_browser
                    browser, page, proxy_url = await restart_browser(browser, headless=False)
                    attempts = 0
                else:
                    await asyncio.sleep(2)

        pages_count = await get_pages_count(page)
        print(f"Найдено страниц: {pages_count}")

        unique_links: Set[str] = set()

        # Сначала собираем ссылки на уже открытой странице (offset=0)
        first_page_links = await collect_complex_links_on_page(page)
        print(f"  Ссылок найдено на странице offset=0: {len(first_page_links)}")
        unique_links.update(first_page_links)

        # Затем переходим по оставшимся страницам: 20, 40, ..., (pages_count-1)*20
        from browser_manager import restart_browser
        for page_index in range(1, pages_count):
            offset = page_index * 20
            url = SEARCH_BASE.format(offset=offset)
            print(f"→ Перехожу на offset={offset}: {url}")
            attempts = 0
            while True:
                try:
                    await page.goto(url,  timeout=60000)
                    await wait_offers_loaded(page)
                    break
                except Exception as e:
                    attempts += 1
                    print(f"Ошибка перехода на offset={offset}: {e} (попытка {attempts}/3)")
                    if attempts >= 3:
                        browser, page, proxy_url = await restart_browser(browser, headless=False)
                        attempts = 0
                    else:
                        await asyncio.sleep(2)

            try:
                links = await collect_complex_links_on_page(page)
                if not links:
                    raise ValueError("Селекторы не найдены или ссылок нет")
            except Exception as e:
                # Если сбор не удался — применяем ту же стратегию ретраев
                retry = 0
                while True:
                    try:
                        await page.goto(url,  timeout=60000)
                        await wait_offers_loaded(page)
                        links = await collect_complex_links_on_page(page)
                        if not links:
                            raise ValueError("Селекторы не найдены или ссылок нет")
                        break
                    except Exception as ee:
                        retry += 1
                        print(f"Повторный сбор offset={offset} неудачен: {ee} (повтор {retry}/3)")
                        if retry >= 3:
                            browser, page, proxy_url = await restart_browser(browser, headless=False)
                            retry = 0
                        else:
                            await asyncio.sleep(2)
            print(f"  Ссылок найдено на странице offset={offset}: {len(links)}")
            unique_links.update(links)

        result_list = sorted(unique_links)
        output_path = PROJECT_ROOT / "complex_links.json"
        with open(output_path, "w", encoding="utf-8") as f:
            json.dump(result_list, f, ensure_ascii=False, indent=2)
        print(f"Сохранено {len(result_list)} уникальных ссылок в {output_path}")

    finally:
        await browser.close()


if __name__ == "__main__":
    asyncio.get_event_loop().run_until_complete(run())
