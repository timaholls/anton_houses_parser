import asyncio
from typing import Optional, Dict, List
import json
from pathlib import Path

# Директория текущего скрипта
PROJECT_ROOT = Path(__file__).resolve().parent

# Импорт функций работы с браузером
from browser_manager import create_browser, create_browser_page, restart_browser


async def check_ip_blocked(page) -> bool:
    """Проверяет, заблокирован ли доступ по IP"""
    try:
        page_content = await page.evaluate('''() => {
            return document.body.textContent || '';
        }''')

        # Проверяем наличие текста о блокировке IP
        blocked_texts = [
            "Доступ ограничен: проблема с IP",
            "доступ ограничен",
            "проблема с ip",
            "blocked",
            "forbidden",
            "access denied"
        ]

        page_content_lower = page_content.lower()
        for blocked_text in blocked_texts:
            if blocked_text.lower() in page_content_lower:
                print(f"🚫 Обнаружена блокировка IP: найдено '{blocked_text}'")
                return True

        return False

    except Exception as e:
        print(f"❌ Ошибка проверки блокировки IP: {e}")
        return False


async def extract_catalog_links(page) -> List[str]:
    """Извлекает ссылки с текущей страницы"""
    return await page.evaluate('''
        () => {
            const elements = document.querySelectorAll('[data-marker="developments-list"] a[href*="/catalog/novostroyki/ufa/"]');
            return Array.from(elements).map(el => el.href);
        }
    ''')


async def get_total_pages(page) -> int:
    """Определяет общее количество страниц из пагинации"""
    try:
        total_pages = await page.evaluate('''
            () => {
                // Ищем пагинацию
                const pagination = document.querySelector('nav[aria-label="Пагинация"]');
                if (!pagination) return 1;
                
                // Ищем все ссылки с data-value в пагинации
                const pageLinks = pagination.querySelectorAll('a[data-value]');
                if (pageLinks.length === 0) return 1;
                
                // Находим максимальный номер страницы
                let maxPage = 1;
                pageLinks.forEach(link => {
                    const pageNum = parseInt(link.getAttribute('data-value'));
                    if (pageNum > maxPage) maxPage = pageNum;
                });
                
                return maxPage;
            }
        ''')
        return total_pages
    except Exception as e:
        print(f"Ошибка при определении количества страниц: {e}")
        return 1


async def main() -> None:
    # Запускаем браузер со случайным прокси
    browser, proxy_url = await create_browser(headless=False)
    page = await create_browser_page(browser)
    print(f"✅ Браузер запущен с прокси: {proxy_url}")

    base_url = "https://www.avito.ru/ufa/kvartiry/prodam/vtorichka-ASgBAgICAkSSA8YQ5geMUg?context=H4sIAAAAAAAA_wEtANL_YToxOntzOjg6ImZyb21QYWdlIjtzOjE2OiJzZWFyY2hGb3JtV2lkZ2V0Ijt9F_yIfi0AAAA"
    all_catalog_links = []

    try:
        # Переходим на первую страницу для определения общего количества страниц
        max_retries = 3
        for attempt in range(max_retries):
            print(f"Загружаем первую страницу (попытка {attempt + 1}/{max_retries})...")
            await page.goto(base_url, waitUntil='domcontentloaded', timeout=120000)
            await asyncio.sleep(5)

            # Проверяем блокировку IP
            is_blocked = await check_ip_blocked(page)
            if is_blocked:
                if attempt < max_retries - 1:
                    print(f"🔄 IP заблокирован, перезапускаем браузер с новым прокси...")
                    browser, page, proxy_url = await restart_browser(browser, headless=False)
                    print(f"✅ Браузер перезапущен с прокси: {proxy_url}")
                    await asyncio.sleep(3)
                else:
                    print(f"❌ IP заблокирован после {max_retries} попыток. Завершение работы.")
                    return
            else:
                print("✅ Доступ разрешен, продолжаем работу")
                break

        # Определяем общее количество страниц
        total_pages = await get_total_pages(page)
        print(f"Найдено {total_pages} страниц для обработки")

        # Обрабатываем все страницы
        for page_num in range(1, total_pages + 1):
            try:
                if page_num > 1:
                    # Формируем URL с параметром page
                    url = f"{base_url}?page={page_num}"
                    print(f"Обрабатываем страницу {page_num}/{total_pages}: {url}")

                    # Пытаемся загрузить страницу с проверкой блокировки
                    for attempt in range(3):
                        await page.goto(url, waitUntil='domcontentloaded', timeout=120000)
                        await asyncio.sleep(3)

                        # Проверяем блокировку
                        is_blocked = await check_ip_blocked(page)
                        if is_blocked:
                            if attempt < 2:
                                print(f"  🔄 IP заблокирован на странице {page_num}, перезапускаем браузер...")
                                browser, page, proxy_url = await restart_browser(browser, headless=False)
                                print(f"  ✅ Браузер перезапущен с прокси: {proxy_url}")
                                await asyncio.sleep(3)
                            else:
                                print(f"  ❌ Пропускаем страницу {page_num} из-за блокировки")
                                break
                        else:
                            break

                # Извлекаем ссылки с текущей страницы
                page_links = await extract_catalog_links(page)
                print(f"  Найдено {len(page_links)} ссылок на странице {page_num}")

                all_catalog_links.extend(page_links)

                # Небольшая пауза между запросами
                await asyncio.sleep(2)

            except Exception as e:
                print(f"Ошибка при обработке страницы {page_num}: {e}")
                continue

        # Выводим статистику
        print(f"\n=== ИТОГО ===")
        print(f"Всего найдено {len(all_catalog_links)} ссылок с /catalog")
        print(f"Обработано страниц: {total_pages}")

        # Убираем дубликаты
        unique_links = list(set(all_catalog_links))

        print(f"Уникальных ссылок: {len(unique_links)}")

        # Сохраняем в JSON файл
        output_file = PROJECT_ROOT / 'catalog_links_all_pages.json'
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(unique_links, f, ensure_ascii=False, indent=2)

        print(f"\nВсе ссылки сохранены в файл '{output_file}'")

        # Выводим первые несколько ссылок для проверки
        print(f"\nПервые 5 ссылок:")
        for i, link in enumerate(unique_links[:5], 1):
            print(f"{i}. {link}")

    except Exception as e:
        print(f"Критическая ошибка: {e}")

    # Закрываем браузер
    await browser.close()


if __name__ == "__main__":
    asyncio.get_event_loop().run_until_complete(main())
