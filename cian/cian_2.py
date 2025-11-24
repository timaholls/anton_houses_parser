#!/usr/bin/env python3
"""
Сбор ссылок на квартиры с сайта CIAN.

Логика:
- Читает JSON файл из cian_1.py (cian_buildings.json)
- Для каждого ЖК открывает страницу с квартирами (поле link)
- Находит все элементы data-name="TitleComponent" и извлекает ссылки на квартиры
- Обрабатывает пагинацию на страницах с квартирами (data-name="Pagination")
- Дополняет JSON файл, добавляя к каждому ЖК поле apartments со списком ссылок
"""
import asyncio
import json
from typing import List, Dict, Any
from pathlib import Path
from urllib.parse import urlparse, parse_qs, urlencode, urlunparse
from dotenv import load_dotenv

# Директория текущего скрипта
PROJECT_ROOT = Path(__file__).resolve().parent

# Загружаем переменные окружения из .env файла (из корня проекта)
PROJECT_ROOT_PARENT = PROJECT_ROOT.parent
load_dotenv(dotenv_path=PROJECT_ROOT_PARENT / ".env")

from browser_manager import create_browser, create_browser_page, restart_browser

INPUT_FILE = PROJECT_ROOT / "cian_buildings.json"
OUTPUT_FILE = PROJECT_ROOT / "cian_buildings.json"
PROGRESS_FILE = PROJECT_ROOT / "progress_cian_2.json"


async def wait_apartments_loaded(page) -> None:
    """Ожидаем появления карточек квартир на странице."""
    try:
        # Пробуем найти элементы TitleComponent
        await page.waitForSelector('[data-name="TitleComponent"]', {"timeout": 60000})
    except Exception as e:
        raise TimeoutError("Не найдены карточки квартир [data-name='TitleComponent']") from e
        # Если не нашли TitleComponent, пробуем альтернативные селекторы
    try:
        await page.waitForSelector('a[href*="/sale/flat/"], a[href*="/flat/"]', {"timeout": 10000})
    except Exception as e:
        raise TimeoutError("Не найдены карточки квартир [data-name='TitleComponent']") from e


async def get_pages_count_apartments(page) -> int:
    """Определяет количество страниц из пагинатора на странице с квартирами."""
    script = """
    () => {
      // Находим пагинатор
      const pagination = document.querySelector('[data-name="Pagination"]');
      if (!pagination) {
        return 1;
      }
      
      const pageNumbers = [];
      
      // Ищем все ссылки в пагинаторе
      const links = pagination.querySelectorAll('a');
      links.forEach(link => {
        const href = link.getAttribute('href') || link.href;
        if (href) {
          // Извлекаем номер страницы из URL параметра p=
          try {
            const url = new URL(href, window.location.origin);
            const p = url.searchParams.get('p');
            if (p) {
              const pageNum = parseInt(p, 10);
              if (!isNaN(pageNum) && pageNum > 0) {
                pageNumbers.push(pageNum);
              }
            }
          } catch (e) {
            // Пытаемся извлечь из строки напрямую
            const match = href.match(/[&?]p=(\\d+)/);
            if (match) {
              const pageNum = parseInt(match[1], 10);
              if (!isNaN(pageNum) && pageNum > 0) {
                pageNumbers.push(pageNum);
              }
            }
          }
        }
      });
      
      // Также проверяем текст элементов пагинации (номера страниц)
      const allElements = pagination.querySelectorAll('a, button, span, li');
      allElements.forEach(el => {
        const text = el.textContent.trim();
        const pageNum = parseInt(text, 10);
        if (!isNaN(pageNum) && pageNum > 0 && pageNum <= 1000) {
          pageNumbers.push(pageNum);
        }
      });
      
      // Возвращаем максимальный номер страницы или 1, если не нашли
      if (pageNumbers.length === 0) {
        return 1;
      }
      return Math.max(...pageNumbers);
    }
    """
    try:
        count = await page.evaluate(script)
        result = int(count) if isinstance(count, (int, float)) and count > 0 else 1
        return result
    except Exception as e:
        print(f"Ошибка при определении количества страниц: {e}")
        return 1


def add_page_param(url: str, page_num: int) -> str:
    """Добавляет или обновляет параметр p= в URL."""
    parsed = urlparse(url)
    query_params = parse_qs(parsed.query)
    query_params['p'] = [str(page_num)]
    new_query = urlencode(query_params, doseq=True)
    new_parsed = parsed._replace(query=new_query)
    return urlunparse(new_parsed)


async def collect_apartment_links(page) -> List[str]:
    """Собирает ссылки на квартиры на текущей странице через JavaScript."""
    script = """
    () => {
      const titleComponents = Array.from(document.querySelectorAll('[data-name="TitleComponent"]'));
      const links = [];
      
      titleComponents.forEach(component => {
        let href = '';
        
        // Проверяем, является ли сам компонент тегом <a>
        if (component.tagName === 'A' || component.tagName === 'a') {
          href = component.getAttribute('href') || component.href || '';
        } else {
          // Если нет, ищем ссылку внутри компонента
          const linkElement = component.querySelector('a');
          if (linkElement) {
            href = linkElement.getAttribute('href') || linkElement.href || '';
          }
        }
        
        if (href) {
          // Фильтруем только ссылки на квартиры (исключаем служебные ссылки)
          if (href.includes('/sale/flat/') || href.includes('/flat/')) {
            // Преобразуем относительную ссылку в абсолютную
            try {
              let fullUrl;
              if (href.startsWith('http://') || href.startsWith('https://')) {
                fullUrl = href;
              } else if (href.startsWith('//')) {
                fullUrl = window.location.protocol + href;
              } else if (href.startsWith('/')) {
                fullUrl = window.location.origin + href;
              } else {
                const url = new URL(href, window.location.origin);
                fullUrl = url.href;
              }
              
              // Убираем параметры типа ?open_calculator=true
              try {
                const urlObj = new URL(fullUrl);
                urlObj.search = ''; // Удаляем все query параметры
                fullUrl = urlObj.href;
              } catch (e) {
                // Если не удалось распарсить, оставляем как есть
              }
              
              links.push(fullUrl);
            } catch (e) {
              // Если не удалось преобразовать, добавляем как есть (если это полный URL)
              if (href.startsWith('http')) {
                links.push(href);
              }
            }
          }
        }
      });
      
      // Удаляем дубликаты
      return Array.from(new Set(links));
    }
    """
    try:
        links = await page.evaluate(script)
        return list(links or [])
    except Exception as e:
        print(f"Ошибка при сборе ссылок на квартиры: {e}")
        return []


def load_buildings(path: str = str(INPUT_FILE)) -> List[Dict[str, Any]]:
    """Загружает список ЖК из JSON файла."""
    if not Path(path).exists():
        print(f"Файл не найден: {path}")
        return []

    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
        if isinstance(data, list):
            return data
        elif isinstance(data, dict) and "buildings" in data:
            return data["buildings"]
        else:
            return []
    except Exception as e:
        print(f"Ошибка при чтении файла {path}: {e}")
        return []


def save_buildings(buildings: List[Dict[str, Any]], path: str = str(OUTPUT_FILE)) -> None:
    """Сохраняет список ЖК в JSON файл."""
    try:
        # Сохраняем во временный файл, затем переименовываем (атомарная операция)
        tmp_path = str(path) + ".tmp"
        with open(tmp_path, "w", encoding="utf-8") as f:
            json.dump(buildings, f, ensure_ascii=False, indent=2)
        Path(tmp_path).replace(path)
    except Exception as e:
        print(f"Ошибка при сохранении файла {path}: {e}")


def load_progress(path: str = str(PROGRESS_FILE)) -> int:
    """Загружает прогресс из файла. Возвращает индекс последнего обработанного ЖК."""
    if not Path(path).exists():
        return 0

    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
        return int(data.get("building_index", 0))
    except Exception as e:
        print(f"Ошибка при чтении прогресса {path}: {e}")
        return 0


def save_progress(building_index: int, path: str = str(PROGRESS_FILE)) -> None:
    """Сохраняет прогресс в файл."""
    try:
        # Сохраняем во временный файл, затем переименовываем (атомарная операция)
        tmp_path = str(path) + ".tmp"
        with open(tmp_path, "w", encoding="utf-8") as f:
            json.dump({"building_index": building_index}, f, ensure_ascii=False, indent=2)
        Path(tmp_path).replace(path)
    except Exception as e:
        print(f"Ошибка при сохранении прогресса {path}: {e}")


async def collect_all_apartments_for_building(page, building_link: str, browser=None) -> List[str]:
    """Собирает все ссылки на квартиры для одного ЖК со всех страниц пагинации."""
    all_apartments: List[str] = []

    # Открываем первую страницу
    print(f"  Открываю: {building_link}")
    attempts = 0
    while True:
        try:
            await page.goto(building_link, timeout=60000)
            await wait_apartments_loaded(page)
            await asyncio.sleep(3)
            break
        except Exception as e:
            error_str = str(e)
            # Если ошибка прокси, сразу перезапускаем браузер с новым прокси
            if "ERR_PROXY_CONNECTION_FAILED" in error_str or "proxy" in error_str.lower():
                print(f"    Ошибка прокси, перезапускаю браузер с новым прокси...")
                try:
                    browser, page, proxy_url = await restart_browser(browser, headless=False,
                                                                     set_domclick_cookies=False)
                    print(f"    ✓ Браузер перезапущен с новым прокси: {proxy_url}")
                    await asyncio.sleep(2)
                    continue  # Пробуем снова с новым прокси
                except Exception as restart_error:
                    print(f"    ✗ Ошибка перезапуска браузера: {restart_error}, повторная попытка...")
                    await asyncio.sleep(3)
                    continue
            else:
                attempts += 1
                print(f"    Ошибка открытия страницы: {e} (попытка {attempts}/3)")
                if attempts >= 3:
                    raise
                await asyncio.sleep(2)

    # Определяем количество страниц на первой странице
    pages_count = await get_pages_count_apartments(page)
    print(f"  Найдено страниц на первой странице: {pages_count}")

    # Собираем ссылки с первой страницы
    page_links = await collect_apartment_links(page)
    print(f"  Страница 1: найдено {len(page_links)} квартир")
    all_apartments.extend(page_links)

    # Переходим по остальным страницам с динамической проверкой
    current_page = 2
    max_pages_found = pages_count

    while current_page <= max_pages_found:
        page_url = add_page_param(building_link, current_page)
        print(f"  Страница {current_page}/{max_pages_found}: {page_url}")

        attempts = 0
        while True:
            try:
                await page.goto(page_url, timeout=60000)
                await wait_apartments_loaded(page)
                await asyncio.sleep(3)
                break
            except Exception as e:
                error_str = str(e)
                # Если ошибка прокси, сразу перезапускаем браузер с новым прокси
                if "ERR_PROXY_CONNECTION_FAILED" in error_str or "proxy" in error_str.lower():
                    print(f"    Ошибка прокси, перезапускаю браузер с новым прокси...")
                    try:
                        browser, page, proxy_url = await restart_browser(browser, headless=False,
                                                                         set_domclick_cookies=False)
                        print(f"    ✓ Браузер перезапущен с новым прокси: {proxy_url}")
                        await asyncio.sleep(2)
                        continue  # Пробуем снова с новым прокси
                    except Exception as restart_error:
                        print(f"    ✗ Ошибка перезапуска браузера: {restart_error}, повторная попытка...")
                        await asyncio.sleep(3)
                        continue
                else:
                    attempts += 1
                    print(f"    Ошибка перехода на страницу {current_page}: {e} (попытка {attempts}/3)")
                    if attempts >= 3:
                        print(f"    Предупреждение: не удалось открыть страницу {current_page}")
                        break
                    await asyncio.sleep(2)

        if attempts < 3:
            # Собираем ссылки со страницы
            page_links = await collect_apartment_links(page)
            if page_links:
                print(f"    Найдено {len(page_links)} квартир")
                all_apartments.extend(page_links)
            else:
                print(f"    Предупреждение: не найдено квартир на странице {current_page}")

            # Когда дошли до последней найденной страницы, проверяем пагинатор еще раз
            if current_page >= max_pages_found:
                new_pages_count = await get_pages_count_apartments(page)
                if new_pages_count > max_pages_found:
                    print(f"    Обнаружено больше страниц: было {max_pages_found}, стало {new_pages_count}")
                    max_pages_found = new_pages_count
                else:
                    # Если новых страниц не появилось, завершаем
                    break

        current_page += 1

    # Удаляем дубликаты
    unique_apartments = list(dict.fromkeys(all_apartments))
    print(f"  Всего собрано уникальных квартир: {len(unique_apartments)}")

    return unique_apartments


async def run() -> None:
    # Загружаем список ЖК
    buildings = load_buildings()
    if not buildings:
        print("Файл со списком ЖК пуст или отсутствует")
        return

    print(f"Загружено ЖК: {len(buildings)}")

    # Загружаем прогресс
    start_index = load_progress()
    if start_index > 0:
        print(f"Продолжаю с ЖК #{start_index + 1} (индекс {start_index})")
    else:
        print(f"Начинаю с начала")

    # Создаем браузер
    browser, proxy_url = await create_browser(headless=False)
    page = await create_browser_page(browser, set_domclick_cookies=False)

    try:
        for i in range(start_index, len(buildings)):
            building = buildings[i]
            building_title = building.get('title', f'ЖК #{i + 1}')
            building_link = building.get('link', '')

            if not building_link:
                print(f"\n[{i + 1}/{len(buildings)}] {building_title}: нет ссылки, пропускаю")
                # Добавляем пустой список квартир, если его еще нет
                if 'apartments' not in building:
                    building['apartments'] = []
                # Сохраняем данные и прогресс даже при пропуске
                save_buildings(buildings)
                save_progress(i + 1)
                continue

            # Проверяем, есть ли уже собранные квартиры
            if 'apartments' in building and building['apartments']:
                print(
                    f"\n[{i + 1}/{len(buildings)}] {building_title}: квартиры уже собраны ({len(building['apartments'])} шт.)")
                # Сохраняем прогресс
                save_progress(i + 1)
                continue

            print(f"\n[{i + 1}/{len(buildings)}] {building_title}")

            try:
                # Собираем ссылки на квартиры
                apartments = await collect_all_apartments_for_building(page, building_link, browser)

                # Добавляем список квартир к записи ЖК
                building['apartments'] = apartments

                # Сохраняем данные и прогресс после каждого ЖК
                save_buildings(buildings)
                save_progress(i + 1)
                print(f"  ✓ Сохранено: {len(apartments)} квартир")

            except Exception as e:
                print(f"  ✗ Ошибка при сборе квартир: {e}")
                # При ошибке добавляем пустой список
                building['apartments'] = []
                # Сохраняем данные и прогресс даже при ошибке
                save_buildings(buildings)
                save_progress(i + 1)

                # Пытаемся перезапустить браузер
                try:
                    browser, page, proxy_url = await restart_browser(browser, headless=False, set_domclick_cookies=False)
                except Exception as restart_error:
                    print(f"  ✗ Ошибка перезапуска браузера: {restart_error}")
                    print(f"  Прогресс сохранен. Можно продолжить позже с индекса {i + 1}")
                    break

        print(f"\n✓ Обработка завершена")
        print(f"Итоговая статистика:")
        total_apartments = sum(len(b.get('apartments', [])) for b in buildings)
        buildings_with_apartments = sum(1 for b in buildings if b.get('apartments'))
        print(f"  ЖК с квартирами: {buildings_with_apartments}/{len(buildings)}")
        print(f"  Всего квартир: {total_apartments}")

        # Удаляем файл прогресса после успешного завершения
        if Path(PROGRESS_FILE).exists():
            try:
                Path(PROGRESS_FILE).unlink()
                print(f"  Файл прогресса удален")
            except Exception:
                pass

    finally:
        await browser.close()

if __name__ == "__main__":
    asyncio.get_event_loop().run_until_complete(run())
