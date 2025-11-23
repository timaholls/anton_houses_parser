#!/usr/bin/env python3
"""
Сбор данных о ЖК с сайта CIAN.

Логика:
- Открываем страницу https://ufa.cian.ru/novostroyki-bashkortostan/
- Определяем количество страниц из пагинатора
- Переходим по всем страницам (используя параметр &p=номер_страницы)
- На каждой странице находим все элементы с data-name="GKCardComponent"
- Внутри каждого элемента находим:
  - data-testid="newbuildingTitle" - название ЖК
  - data-mark="RoomCounts" - ссылку
- Сохраняем в JSON: название ЖК - ссылка
"""
import asyncio
import json
import logging
import hashlib
from typing import List, Dict, Any, Optional
from pathlib import Path
from urllib.parse import urlparse, parse_qs, urlencode, urlunparse
from io import BytesIO
import aiohttp
from dotenv import load_dotenv

# Директория текущего скрипта
PROJECT_ROOT = Path(__file__).resolve().parent

# Загружаем переменные окружения из .env файла (из корня проекта)
PROJECT_ROOT_PARENT = PROJECT_ROOT.parent
load_dotenv(dotenv_path=PROJECT_ROOT_PARENT / ".env")

from browser_manager import create_browser, create_browser_page
from resize_img import ImageProcessor
from s3_service import S3Service
from watermark_on_save import upload_with_watermark

# Настройка логгера
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
if not logger.handlers:
    handler = logging.StreamHandler()
    handler.setLevel(logging.INFO)
    formatter = logging.Formatter('%(message)s')
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    logger.propagate = False

# Инициализация обработчика изображений
image_processor = ImageProcessor(logger, max_size=(800, 600), max_kb=150)


TARGET_URL = "https://ufa.cian.ru/novostroyki-bashkortostan/"
OUTPUT_FILE = PROJECT_ROOT / "cian_buildings.json"
PROGRESS_FILE = PROJECT_ROOT / "progress_cian_1.json"


async def wait_cards_loaded(page) -> None:
    """Ожидаем появления карточек ЖК на странице."""
    try:
        await page.waitForSelector('[data-name="GKCardComponent"]', {"timeout": 60000})
    except Exception as e:
        raise TimeoutError("Не найдены карточки ЖК [data-name='GKCardComponent']") from e


async def get_pages_count(page) -> int:
    """Определяет количество страниц из пагинатора."""
    script = """
    () => {
      // Находим все элементы пагинации
      const paginationItems = Array.from(document.querySelectorAll('[data-name="PaginationItem"]'));
      const pageNumbers = [];
      
      paginationItems.forEach(item => {
        // Ищем ссылки внутри элементов пагинации
        const link = item.querySelector('a[data-name="PaginationLink"]');
        if (link) {
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
        }
        
        // Также проверяем текущую страницу
        const isCurrent = item.getAttribute('data-testid') === 'pagination-item-current' ||
                         item.querySelector('[data-testid="pagination-item-current"]');
        if (isCurrent) {
          const text = item.textContent.trim();
          const pageNum = parseInt(text, 10);
          if (!isNaN(pageNum) && pageNum > 0) {
            pageNumbers.push(pageNum);
          }
        }
        
        // Также проверяем текст элемента напрямую (может быть просто число)
        const text = item.textContent.trim();
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


def get_building_id(building: Dict[str, Any]) -> str:
    """Создает уникальный ID ЖК из ссылки или названия."""
    building_link = building.get('link', '')
    building_title = building.get('title', '')
    
    if building_link:
        # Пробуем извлечь ID из ссылки
        try:
            parsed = urlparse(building_link)
            # Ищем newobject в query параметрах
            query_params = parse_qs(parsed.query)
            if 'newobject' in query_params:
                building_id = query_params['newobject'][0]
                if building_id:
                    return building_id
            # Или из пути
            building_id = parsed.path.split('/')[-1].rstrip('/')
            if building_id and building_id != 'unknown' and building_id:
                return building_id
        except Exception:
            pass
    
    # Если не удалось извлечь из ссылки, используем хеш названия + ссылки для уникальности
    unique_string = f"{building_title}_{building_link}"
    return hashlib.md5(unique_string.encode()).hexdigest()[:16]


async def download_and_process_building_photo(session: aiohttp.ClientSession, image_url: str, building_id: str, photo_index: int) -> Optional[str]:
    """Скачивает фотографию ЖК, обрабатывает и загружает в S3."""
    try:
        async with session.get(image_url, timeout=aiohttp.ClientTimeout(total=10)) as response:
            if response.status != 200:
                return None
            image_bytes = await response.read()
    except Exception as e:
        logger.error(f"Ошибка скачивания фотографии ЖК {image_url}: {e}")
        return None
    
    try:
        # Обрабатываем изображение
        input_bytes = BytesIO(image_bytes)
        processed_bytes = image_processor.process(input_bytes)
        processed_bytes.seek(0)
        processed_data = processed_bytes.read()
        
        # Создаем уникальный ключ: добавляем хеш исходного URL фотографии
        image_url_hash = hashlib.md5(image_url.encode()).hexdigest()[:8]
        s3 = S3Service()
        key = f"cian/buildings/{building_id}/photo_{photo_index + 1}_{image_url_hash}.jpg"
        url_public = upload_with_watermark(s3, processed_data, key)
        return url_public
    except Exception as e:
        logger.error(f"Ошибка обработки фотографии ЖК {image_url}: {e}")
        return None


async def process_building_photos(building: Dict[str, Any]) -> List[str]:
    """Обрабатывает и загружает фотографии ЖК в S3."""
    photos_urls = building.get('photos', [])
    if not photos_urls:
        return []
    
    building_id = get_building_id(building)
    processed_photos = []
    
    async with aiohttp.ClientSession() as session:
        # Обрабатываем до 5 фотографий параллельно
        semaphore = asyncio.Semaphore(5)
        
        async def process_single_photo(url, index):
            async with semaphore:
                return await download_and_process_building_photo(session, url, building_id, index)
        
        tasks = [process_single_photo(url, i) for i, url in enumerate(photos_urls[:8])]  # Максимум 8 фото
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        for result in results:
            if isinstance(result, str) and result:
                processed_photos.append(result)
    
    return processed_photos


def load_progress() -> int:
    """Загружает прогресс из файла. Возвращает номер страницы, с которой нужно продолжить."""
    if not PROGRESS_FILE.exists():
        return 1
    
    try:
        with open(PROGRESS_FILE, 'r', encoding='utf-8') as f:
            progress = json.load(f)
            return progress.get('last_page', 1)
    except Exception as e:
        print(f"Ошибка загрузки прогресса: {e}")
        return 1


def save_progress(page_num: int) -> None:
    """Сохраняет прогресс в файл."""
    try:
        progress = {
            'last_page': page_num
        }
        with open(PROGRESS_FILE, 'w', encoding='utf-8') as f:
            json.dump(progress, f, ensure_ascii=False, indent=2)
    except Exception as e:
        print(f"Ошибка сохранения прогресса: {e}")


def save_data(buildings_data: List[Dict[str, Any]]) -> None:
    """Сохраняет данные в JSON файл."""
    try:
        with open(OUTPUT_FILE, 'w', encoding='utf-8') as f:
            json.dump(buildings_data, f, ensure_ascii=False, indent=2)
    except Exception as e:
        print(f"Ошибка сохранения данных: {e}")


def load_data() -> List[Dict[str, Any]]:
    """Загружает существующие данные из JSON файла."""
    if not OUTPUT_FILE.exists():
        return []
    
    try:
        with open(OUTPUT_FILE, 'r', encoding='utf-8') as f:
            return json.load(f)
    except Exception as e:
        print(f"Ошибка загрузки данных: {e}")
        return []


async def collect_buildings_data(page) -> List[Dict[str, any]]:
    """Собирает данные о ЖК на текущей странице через JavaScript."""
    script = """
    () => {
      const cards = Array.from(document.querySelectorAll('[data-name="GKCardComponent"]'));
      const results = [];
      
      cards.forEach(card => {
        // Находим название ЖК через data-testid="newbuildingTitle"
        const titleElement = card.querySelector('[data-testid="newbuildingTitle"]');
        let title = '';
        if (titleElement) {
          // Ищем текст внутри, может быть в span или другом элементе
          // Проверяем все возможные вложенные элементы с текстом
          const textElements = titleElement.querySelectorAll('span');
          if (textElements.length > 0) {
            // Берем последний span, который обычно содержит основной текст
            const lastSpan = Array.from(textElements).pop();
            title = lastSpan ? lastSpan.textContent.trim() : '';
          }
          // Если не нашли в span, берем текст самого элемента
          if (!title) {
            title = titleElement.textContent.trim();
          }
        }
        
        // Находим ссылку с data-mark="RoomCounts"
        const linkElement = card.querySelector('[data-mark="RoomCounts"]');
        let link = '';
        let isFromAgents = false;
        if (linkElement) {
          // Проверяем текст элемента на наличие "от агентов"
          const linkText = linkElement.textContent || '';
          const linkTextLower = linkText.toLowerCase();
          if (linkTextLower.includes('от агентов') || linkTextLower.includes('от агента')) {
            isFromAgents = true;
          } else {
            // Получаем href атрибут только если не от агентов
            const href = linkElement.getAttribute('href') || linkElement.href || '';
            if (href) {
              // Преобразуем относительную ссылку в абсолютную
              try {
                if (href.startsWith('http://') || href.startsWith('https://')) {
                  link = href;
                } else if (href.startsWith('//')) {
                  link = window.location.protocol + href;
                } else if (href.startsWith('/')) {
                  link = window.location.origin + href;
                } else {
                  const url = new URL(href, window.location.origin);
                  link = url.href;
                }
              } catch (e) {
                link = href;
              }
            }
          }
        }
        
        // Собираем фотографии ЖК из галереи (CarouselBlock)
        const photos = [];
        const carousel = card.querySelector('[data-name="CarouselBlock"]') || card.querySelector('[data-name="Carousel"]');
        if (carousel) {
          // Ищем все изображения в карусели
          const images = carousel.querySelectorAll('img');
          images.forEach(img => {
            const src = img.getAttribute('src') || img.getAttribute('data-src') || img.src;
            if (src) {
              try {
                const url = new URL(src, window.location.origin).href;
                // Фильтруем только реальные изображения (не placeholder)
                if (url.includes('images.cdn-cian.ru') || url.includes('cdn-cian.ru')) {
                  photos.push(url);
                }
              } catch (e) {
                if (src.startsWith('http') && (src.includes('images.cdn-cian.ru') || src.includes('cdn-cian.ru'))) {
                  photos.push(src);
                }
              }
            }
          });
        }
        
        // Удаляем дубликаты фотографий
        const uniquePhotos = Array.from(new Set(photos));
        
        // Добавляем если есть название (ссылка может быть пустой, если от агентов)
        if (title) {
          results.push({
            title: title,
            link: link,  // Может быть пустой строкой, если от агентов
            photos: uniquePhotos,
            isFromAgents: isFromAgents  // Флаг, что квартиры от агентов
          });
        }
      });
      
      return results;
    }
    """
    try:
        data = await page.evaluate(script)
        return list(data or [])
    except Exception as e:
        print(f"Ошибка при сборе данных: {e}")
        return []


async def run() -> None:
    # Загружаем прогресс и существующие данные
    start_page = load_progress()
    all_buildings_data = load_data()
    
    if start_page > 1:
        print(f"Продолжаем с страницы {start_page}")
        print(f"Уже собрано ЖК: {len(all_buildings_data)}")
    
    browser, proxy_url = await create_browser(headless=False)
    page = await create_browser_page(browser, set_domclick_cookies=False)

    try:
        # Открываем первую страницу (или страницу, с которой продолжаем)
        if start_page == 1:
            print(f"Открываю: {TARGET_URL}")
            attempts = 0
            while True:
                try:
                    await page.goto(TARGET_URL, timeout=60000)
                    await wait_cards_loaded(page)
                    # Даем время на полную загрузку динамического контента
                    await asyncio.sleep(3)
                    break
                except Exception as e:
                    error_str = str(e)
                    # Если ошибка прокси, сразу перезапускаем браузер с новым прокси
                    if "ERR_PROXY_CONNECTION_FAILED" in error_str or "proxy" in error_str.lower():
                        print(f"  Ошибка прокси, перезапускаю браузер с новым прокси...")
                        try:
                            from browser_manager import restart_browser
                            browser, page, proxy_url = await restart_browser(browser, headless=False, set_domclick_cookies=False)
                            print(f"  ✓ Браузер перезапущен с новым прокси: {proxy_url}")
                            await asyncio.sleep(2)
                            continue  # Пробуем снова с новым прокси
                        except Exception as restart_error:
                            print(f"  ✗ Ошибка перезапуска браузера: {restart_error}, повторная попытка...")
                            await asyncio.sleep(3)
                            continue
                    else:
                        attempts += 1
                        print(f"Ошибка открытия страницы: {e} (попытка {attempts}/3)")
                        if attempts >= 3:
                            from browser_manager import restart_browser
                            browser, page, proxy_url = await restart_browser(browser, headless=False, set_domclick_cookies=False)
                            attempts = 0
                        else:
                            await asyncio.sleep(2)
        else:
            # Открываем страницу, с которой продолжаем
            page_url = add_page_param(TARGET_URL, start_page)
            print(f"Открываю: {page_url}")
            attempts = 0
            while True:
                try:
                    await page.goto(page_url, timeout=60000)
                    await wait_cards_loaded(page)
                    await asyncio.sleep(3)
                    break
                except Exception as e:
                    error_str = str(e)
                    # Если ошибка прокси, сразу перезапускаем браузер с новым прокси
                    if "ERR_PROXY_CONNECTION_FAILED" in error_str or "proxy" in error_str.lower():
                        print(f"  Ошибка прокси, перезапускаю браузер с новым прокси...")
                        try:
                            from browser_manager import restart_browser
                            browser, page, proxy_url = await restart_browser(browser, headless=False, set_domclick_cookies=False)
                            print(f"  ✓ Браузер перезапущен с новым прокси: {proxy_url}")
                            await asyncio.sleep(2)
                            continue  # Пробуем снова с новым прокси
                        except Exception as restart_error:
                            print(f"  ✗ Ошибка перезапуска браузера: {restart_error}, повторная попытка...")
                            await asyncio.sleep(3)
                            continue
                    else:
                        attempts += 1
                        print(f"Ошибка открытия страницы: {e} (попытка {attempts}/3)")
                        if attempts >= 3:
                            from browser_manager import restart_browser
                            browser, page, proxy_url = await restart_browser(browser, headless=False, set_domclick_cookies=False)
                            attempts = 0
                        else:
                            await asyncio.sleep(2)

        # Определяем количество страниц
        pages_count = await get_pages_count(page)
        print(f"Найдено страниц: {pages_count}")

        # Обрабатываем страницы, начиная с start_page
        from browser_manager import restart_browser
        for page_num in range(start_page, pages_count + 1):
            if page_num > start_page:
                # Переходим на следующую страницу
                page_url = add_page_param(TARGET_URL, page_num)
                print(f"\nСтраница {page_num}: {page_url}")
                
                attempts = 0
                while True:
                    try:
                        await page.goto(page_url, timeout=60000)
                        await wait_cards_loaded(page)
                        await asyncio.sleep(3)
                        break
                    except Exception as e:
                        error_str = str(e)
                        # Если ошибка прокси, сразу перезапускаем браузер с новым прокси
                        if "ERR_PROXY_CONNECTION_FAILED" in error_str or "proxy" in error_str.lower():
                            print(f"  Ошибка прокси, перезапускаю браузер с новым прокси...")
                            try:
                                browser, page, proxy_url = await restart_browser(browser, headless=False, set_domclick_cookies=False)
                                print(f"  ✓ Браузер перезапущен с новым прокси: {proxy_url}")
                                await asyncio.sleep(2)
                                continue  # Пробуем снова с новым прокси
                            except Exception as restart_error:
                                print(f"  ✗ Ошибка перезапуска браузера: {restart_error}, повторная попытка...")
                                await asyncio.sleep(3)
                                continue
                        else:
                            attempts += 1
                            print(f"  Ошибка перехода на страницу {page_num}: {e} (попытка {attempts}/3)")
                            if attempts >= 3:
                                browser, page, proxy_url = await restart_browser(browser, headless=False, set_domclick_cookies=False)
                                attempts = 0
                            else:
                                await asyncio.sleep(2)
            else:
                print(f"\nСтраница {page_num}:")

            # Собираем данные со страницы
            page_data = await collect_buildings_data(page)
            if not page_data:
                print("  Данные не найдены, повторная попытка...")
                await asyncio.sleep(5)
                page_data = await collect_buildings_data(page)
            
            if not page_data:
                print(f"  Предупреждение: не удалось собрать данные со страницы {page_num}")
                # Сохраняем прогресс даже если не нашли данные
                save_progress(page_num)
            else:
                print(f"  Найдено ЖК: {len(page_data)}")
                
                # Обрабатываем фотографии ЖК для этой страницы и загружаем в S3
                print(f"  Обработка фотографий ЖК...")
                for building in page_data:
                    building_title = building.get('title', 'ЖК')
                    building_link = building.get('link', '')
                    is_from_agents = building.get('isFromAgents', False)
                    
                    photos_urls = building.get('photos', [])
                    if photos_urls:
                        processed_photos = await process_building_photos(building)
                        building['photos'] = processed_photos
                        # Выводим сообщение в зависимости от наличия ссылки и агентов
                        if not building_link:
                            if is_from_agents:
                                print(f"    ✓ {building_title}: квартир нет (от агентов), фото собраны ({len(processed_photos)} шт.)")
                            else:
                                print(f"    ✓ {building_title}: квартир нет, фото собраны ({len(processed_photos)} шт.)")
                        else:
                            print(f"    ✓ {building_title}: {len(processed_photos)} фотографий")
                    else:
                        building['photos'] = []
                        # Выводим сообщение даже если нет фото
                        if not building_link:
                            if is_from_agents:
                                print(f"    ✓ {building_title}: квартир нет (от агентов), фото нет")
                            else:
                                print(f"    ✓ {building_title}: квартир нет, фото нет")
                    
                    # Удаляем флаг isFromAgents из результата (он был только для логирования)
                    if 'isFromAgents' in building:
                        del building['isFromAgents']
                
                # Добавляем данные со страницы
                all_buildings_data.extend(page_data)
                
                # Сохраняем данные и прогресс после каждой страницы
                save_data(all_buildings_data)
                save_progress(page_num)
                print(f"  ✓ Сохранено в файл. Всего ЖК: {len(all_buildings_data)}")
            
            # Проверяем, не появились ли новые страницы (динамическая пагинация)
            if page_num == pages_count:
                new_pages_count = await get_pages_count(page)
                if new_pages_count > pages_count:
                    print(f"  Обнаружены новые страницы: {new_pages_count} (было {pages_count})")
                    pages_count = new_pages_count

        print(f"\n✓ Обработка завершена")
        print(f"Всего собрано ЖК: {len(all_buildings_data)}")
        
        # Удаляем файл прогресса после успешного завершения
        if PROGRESS_FILE.exists():
            try:
                PROGRESS_FILE.unlink()
                print(f"Файл прогресса удален")
            except Exception:
                pass
        
        # Выводим первые несколько записей для проверки
        if all_buildings_data:
            print("\nПримеры собранных данных:")
            for i, item in enumerate(all_buildings_data[:3], 1):
                photos_count = len(item.get('photos', []))
                print(f"  {i}. {item['title']} -> {item['link']} ({photos_count} фото)")

    finally:
        await browser.close()


if __name__ == "__main__":
    asyncio.get_event_loop().run_until_complete(run())
