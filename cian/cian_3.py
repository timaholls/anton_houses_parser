#!/usr/bin/env python3
"""
Сбор детальной информации по квартирам с сайта CIAN.

Логика:
- Читает JSON файл из cian_2.py (cian_buildings.json) с собранными ссылками на квартиры
- Для каждой квартиры собирает:
  - data-name="OfferTitleNew" - название
  - data-name="OfferGallery" - первую фотографию (обрабатывает через resize_img.py и загружает в S3)
  - data-name="ObjectFactoidsItem" - параметры (все элементы)
  - data-name="OfferSummaryInfoGroup" - дополнительные данные
  - data-name="NewbuildingCurrentDecoration" - отделка квартиры (описание и фото)
- Сохраняет в отдельный JSON файл со структурой: название ЖК, ссылка на ЖК, список квартир
"""
import asyncio
import json
import logging
import hashlib
from typing import List, Dict, Any, Optional
from pathlib import Path
from io import BytesIO
import aiohttp
from urllib.parse import urlparse
from dotenv import load_dotenv

# Директория текущего скрипта
PROJECT_ROOT = Path(__file__).resolve().parent

# Загружаем переменные окружения из .env файла (из корня проекта)
PROJECT_ROOT_PARENT = PROJECT_ROOT.parent
load_dotenv(dotenv_path=PROJECT_ROOT_PARENT / ".env")

from browser_manager import create_browser, create_browser_page, restart_browser
from resize_img import ImageProcessor
from s3_service import S3Service
from watermark_on_save import upload_with_watermark

INPUT_FILE = PROJECT_ROOT / "cian_buildings.json"
OUTPUT_FILE = PROJECT_ROOT / "cian_apartments_data.json"
PROGRESS_FILE = PROJECT_ROOT / "progress_cian_3.json"

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


def save_data(data: List[Dict[str, Any]], path: str = str(OUTPUT_FILE)) -> None:
    """Сохраняет данные в JSON файл."""
    try:
        tmp_path = str(path) + ".tmp"
        with open(tmp_path, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
        Path(tmp_path).replace(path)
    except Exception as e:
        print(f"Ошибка при сохранении файла {path}: {e}")


def load_progress(path: str = str(PROGRESS_FILE)) -> Dict[str, int]:
    """Загружает прогресс из файла."""
    if not Path(path).exists():
        return {"building_index": 0, "apartment_index": 0}
    
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
        return {
            "building_index": int(data.get("building_index", 0)),
            "apartment_index": int(data.get("apartment_index", 0))
        }
    except Exception as e:
        print(f"Ошибка при чтении прогресса {path}: {e}")
        return {"building_index": 0, "apartment_index": 0}


def save_progress(building_index: int, apartment_index: int, path: str = str(PROGRESS_FILE)) -> None:
    """Сохраняет прогресс в файл."""
    try:
        tmp_path = str(path) + ".tmp"
        with open(tmp_path, "w", encoding="utf-8") as f:
            json.dump({"building_index": building_index, "apartment_index": apartment_index}, f, ensure_ascii=False, indent=2)
        Path(tmp_path).replace(path)
    except Exception as e:
        print(f"Ошибка при сохранении прогресса {path}: {e}")


async def download_and_process_image(session: aiohttp.ClientSession, image_url: str, apartment_id: str, image_type: str = "main") -> Optional[str]:
    """Скачивает изображение, обрабатывает и загружает в S3."""
    try:
        async with session.get(image_url, timeout=aiohttp.ClientTimeout(total=10)) as response:
            if response.status != 200:
                return None
            image_bytes = await response.read()
    except Exception as e:
        logger.error(f"Ошибка скачивания изображения {image_url}: {e}")
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
        key = f"cian/apartments/{apartment_id}/{image_type}_{image_url_hash}.jpg"
        url_public = upload_with_watermark(s3, processed_data, key)
        return url_public
    except Exception as e:
        logger.error(f"Ошибка обработки изображения {image_url}: {e}")
        return None


async def parse_apartment_page(page, apartment_url: str) -> Dict[str, Any]:
    """Парсит страницу квартиры и собирает всю информацию."""
    apartment_data = {
        "url": apartment_url,
        "title": "",
        "main_photo": None,
        "price": "",
        "price_per_square": "",
        "factoids": [],
        "summary_info": [],
        "decoration": {
            "description": "",
            "photos": []
        }
    }
    
    try:
        # Проверяем, что страница не закрыта
        try:
            if page.isClosed():
                raise Exception("Страница закрыта, требуется перезапуск браузера")
        except Exception as check_error:
            # Если проверка не удалась, значит страница закрыта или недоступна
            error_str = str(check_error).lower()
            if 'session closed' in error_str or 'target closed' in error_str or 'page closed' in error_str:
                raise Exception("Страница закрыта, требуется перезапуск браузера")
            raise
        
        # Переходим на страницу, ждем только загрузки DOM (не всех ресурсов)
        await page.goto(apartment_url, timeout=60000, waitUntil='domcontentloaded')
        
        # Ждем появления ключевых элементов вместо полной загрузки страницы
        # Ждем появления хотя бы одного из основных элементов
        try:
            # Пробуем дождаться названия или цены (эти элементы появляются первыми)
            await page.waitForSelector(
                '[data-name="OfferTitleNew"], [data-name="NewbuildingPriceInfo"]',
                {'timeout': 15000}
            )
        except Exception:
            # Если не появились, пробуем дождаться галереи
            try:
                await page.waitForSelector('[data-name="OfferGallery"]', {'timeout': 10000})
            except Exception:
                # Если ничего не появилось, продолжаем - возможно элементы уже есть
                pass
        
        # Парсим данные через JavaScript
        script = """
        () => {
          const result = {
            title: "",
            main_photo: null,
            price: "",
            price_per_square: "",
            factoids: [],
            summary_info: [],
            decoration: {
              description: "",
              photos: []
            }
          };
          
          // 1. Название квартиры
          const titleElement = document.querySelector('[data-name="OfferTitleNew"]');
          if (titleElement) {
            result.title = titleElement.textContent.trim();
          }
          
          // 2. Первая фотография из галереи
          const gallery = document.querySelector('[data-name="OfferGallery"]');
          if (gallery) {
            const firstImg = gallery.querySelector('img');
            if (firstImg) {
              const src = firstImg.getAttribute('src') || firstImg.getAttribute('data-src') || firstImg.src;
              if (src) {
                try {
                  result.main_photo = new URL(src, window.location.origin).href;
                } catch (e) {
                  result.main_photo = src.startsWith('http') ? src : window.location.origin + src;
                }
              }
            }
          }
          
          // 2.5. Цена (NewbuildingPriceInfo)
          const priceInfo = document.querySelector('[data-name="NewbuildingPriceInfo"]');
          if (priceInfo) {
            // Основная цена
            const priceAmount = priceInfo.querySelector('[data-testid="price-amount"]');
            if (priceAmount) {
              result.price = priceAmount.textContent.trim();
            } else {
              // Пробуем найти цену другим способом
              const priceSpans = priceInfo.querySelectorAll('span');
              priceSpans.forEach(span => {
                const text = span.textContent.trim();
                if (text.match(/[\\d\\s]+[РрP]/)) {
                  if (!result.price) {
                    result.price = text;
                  }
                }
              });
            }
            
            // Цена за квадратный метр
            const factItems = priceInfo.querySelectorAll('[data-name="OfferFactItem"]');
            factItems.forEach(item => {
              const spans = item.querySelectorAll('span');
              if (spans.length >= 2) {
                const label = spans[0].textContent.trim();
                const value = spans[1].textContent.trim();
                if (label.includes('метр') || label.includes('м²') || label.includes('квадрат')) {
                  result.price_per_square = value;
                }
              }
            });
            
            // Если не нашли в OfferFactItem, пробуем найти в тексте
            if (!result.price_per_square) {
              const allText = priceInfo.textContent;
              const pricePerSquareMatch = allText.match(/([\\d\\s]+[РрP]\/м²)/);
              if (pricePerSquareMatch) {
                result.price_per_square = pricePerSquareMatch[1];
              }
            }
          } else {
            // Альтернативный поиск цены
            const altPrice = document.querySelector('[data-testid="price-amount"]');
            if (altPrice) {
              result.price = altPrice.textContent.trim();
            }
          }
          
          // 3. Параметры (ObjectFactoidsItem)
          const factoidItems = document.querySelectorAll('[data-name="ObjectFactoidsItem"]');
          factoidItems.forEach(item => {
            const textDiv = item.querySelector('div[class*="--text"]');
            if (textDiv) {
              const spans = textDiv.querySelectorAll('span');
              if (spans.length >= 2) {
                const label = spans[0].textContent.trim();
                const value = spans[1].textContent.trim();
                result.factoids.push({ label: label, value: value });
              }
            }
          });
          
          // 4. Дополнительные данные (OfferSummaryInfoGroup)
          const summaryGroups = document.querySelectorAll('[data-name="OfferSummaryInfoGroup"]');
          summaryGroups.forEach(group => {
            const items = group.querySelectorAll('[data-name="OfferSummaryInfoItem"]');
            items.forEach(item => {
              const paragraphs = item.querySelectorAll('p');
              if (paragraphs.length >= 2) {
                const label = paragraphs[0].textContent.trim();
                const value = paragraphs[1].textContent.trim();
                result.summary_info.push({ label: label, value: value });
              }
            });
          });
          
          // 5. Отделка квартиры (NewbuildingCurrentDecoration)
          const decoration = document.querySelector('[data-name="NewbuildingCurrentDecoration"]');
          if (decoration) {
            // Описание
            const subtitle = decoration.querySelector('div[class*="--subtitle"]');
            if (subtitle) {
              result.decoration.description = subtitle.textContent.trim();
            }
            
            // Фотографии отделки
            const gallery = decoration.querySelector('div[class*="--gallery"]');
            if (gallery) {
              const images = gallery.querySelectorAll('img');
              images.forEach(img => {
                const src = img.getAttribute('src') || img.getAttribute('data-src') || img.src;
                if (src) {
                  try {
                    const url = new URL(src, window.location.origin).href;
                    result.decoration.photos.push(url);
                  } catch (e) {
                    if (src.startsWith('http')) {
                      result.decoration.photos.push(src);
                    }
                  }
                }
              });
            }
          }
          
          return result;
        }
        """
        
        parsed_data = await page.evaluate(script)
        
        apartment_data.update(parsed_data)
        
        # Обрабатываем title: убираем HTML пробелы и слово "Продается"
        if apartment_data.get("title"):
            title = apartment_data["title"]
            # Заменяем HTML пробелы (NBSP, \u00A0, &nbsp;) на обычные пробелы
            title = title.replace('\u00A0', ' ')  # NBSP
            title = title.replace('&nbsp;', ' ')
            title = title.replace('\u2009', ' ')  # Thin space
            title = title.replace('\u2007', ' ')  # Figure space
            title = title.replace('\u202F', ' ')  # Narrow no-break space
            # Убираем множественные пробелы
            title = ' '.join(title.split())
            # Убираем слово "Продается" в начале (с учетом регистра)
            if title.startswith("Продается "):
                title = title[10:]  # Убираем "Продается " (10 символов)
            elif title.startswith("продается "):
                title = title[10:]
            elif title.startswith("ПРОДАЕТСЯ "):
                title = title[10:]
            apartment_data["title"] = title
        
        # Обрабатываем description отделки: убираем HTML пробелы
        if apartment_data.get("decoration", {}).get("description"):
            desc = apartment_data["decoration"]["description"]
            desc = desc.replace('\u00A0', ' ')  # NBSP
            desc = desc.replace('&nbsp;', ' ')
            desc = desc.replace('\u2009', ' ')
            desc = desc.replace('\u2007', ' ')
            desc = desc.replace('\u202F', ' ')
            desc = ' '.join(desc.split())
            apartment_data["decoration"]["description"] = desc
        
        # Обрабатываем и загружаем фотографии
        # Создаем уникальный ID квартиры: извлекаем ID из пути и добавляем короткий хеш URL для уникальности
        parsed_url = urlparse(apartment_url)
        apartment_id_from_path = parsed_url.path.split('/')[-1].rstrip('/')
        url_hash = hashlib.md5(apartment_url.encode()).hexdigest()[:8]
        
        if apartment_id_from_path and apartment_id_from_path != 'unknown':
            # Используем ID из пути + короткий хеш URL для уникальности
            apartment_id = f"{apartment_id_from_path}_{url_hash}"
        else:
            # Если не удалось извлечь из пути, используем только хеш всего URL
            apartment_id = hashlib.md5(apartment_url.encode()).hexdigest()[:16]
        
        async with aiohttp.ClientSession() as session:
            # Главная фотография
            if apartment_data.get("main_photo"):
                main_photo_url = await download_and_process_image(session, apartment_data["main_photo"], apartment_id, "main")
                apartment_data["main_photo"] = main_photo_url
            
            # Фотографии отделки
            if apartment_data.get("decoration", {}).get("photos"):
                decoration_photos = []
                for idx, photo_url in enumerate(apartment_data["decoration"]["photos"][:5]):  # Максимум 5 фото
                    # Используем индекс для уникальности позиции, хеш URL добавится в функции download_and_process_image
                    image_type = f"decoration_{idx+1}"
                    processed_url = await download_and_process_image(session, photo_url, apartment_id, image_type)
                    if processed_url:
                        decoration_photos.append(processed_url)
                apartment_data["decoration"]["photos"] = decoration_photos
        
    except Exception as e:
        error_str = str(e)
        # Если это ошибка прокси/подключения, пробрасываем исключение наверх для перезапуска браузера
        is_proxy_error = "ERR_PROXY_CONNECTION_FAILED" in error_str or "proxy" in error_str.lower()
        is_connection_error = "connection" in error_str.lower() or "timeout" in error_str.lower() or "network" in error_str.lower()
        is_session_error = "сессия закрыта" in error_str.lower() or "session closed" in error_str.lower() or "Target closed" in error_str or "page closed" in error_str.lower()
        
        if is_proxy_error or is_connection_error or is_session_error:
            # Пробрасываем ошибку наверх для обработки в основном цикле
            logger.error(f"Ошибка подключения/прокси при парсинге квартиры {apartment_url}: {e}")
            raise  # Пробрасываем исключение
        
        # Для других ошибок просто логируем и возвращаем пустые данные
        logger.error(f"Ошибка при парсинге квартиры {apartment_url}: {e}")
    
    return apartment_data


async def run() -> None:
    # Загружаем список ЖК
    buildings = load_buildings()
    if not buildings:
        print("Файл со списком ЖК пуст или отсутствует")
        return
    
    print(f"Загружено ЖК: {len(buildings)}")
    
    # Загружаем прогресс
    progress = load_progress()
    start_building_index = progress["building_index"]
    start_apartment_index = progress["apartment_index"]
    
    if start_building_index > 0 or start_apartment_index > 0:
        print(f"Продолжаю с ЖК #{start_building_index + 1}, квартира #{start_apartment_index + 1}")
    else:
        print(f"Начинаю с начала")
    
    # Загружаем существующие данные или создаем новый список
    result_data = []
    if Path(OUTPUT_FILE).exists():
        try:
            with open(OUTPUT_FILE, "r", encoding="utf-8") as f:
                result_data = json.load(f)
        except Exception:
            result_data = []
    
    # Создаем браузер
    browser, proxy_url = await create_browser(headless=False)
    page = await create_browser_page(browser, set_domclick_cookies=False)
    
    try:
        for building_idx in range(start_building_index, len(buildings)):
            building = buildings[building_idx]
            building_title = building.get('title', f'ЖК #{building_idx+1}')
            building_link = building.get('link', '')
            apartments_links = building.get('apartments', [])
            
            if not apartments_links:
                print(f"\n[{building_idx+1}/{len(buildings)}] {building_title}: нет ссылок на квартиры")
                continue
            
            print(f"\n[{building_idx+1}/{len(buildings)}] {building_title}: {len(apartments_links)} квартир")
            
            # Ищем или создаем запись для этого ЖК
            building_data = None
            for bd in result_data:
                if bd.get("building_link") == building_link:
                    building_data = bd
                    break
            
            if not building_data:
                # Получаем фотографии ЖК из исходных данных
                building_photos = building.get('photos', [])
                building_data = {
                    "building_title": building_title,
                    "building_link": building_link,
                    "building_photos": building_photos,  # Фотографии ЖК
                    "apartments": []
                }
                result_data.append(building_data)
            
            # Парсим квартиры
            start_idx = start_apartment_index if building_idx == start_building_index else 0
            for apt_idx in range(start_idx, len(apartments_links)):
                apartment_url = apartments_links[apt_idx]
                print(f"  Квартира {apt_idx+1}/{len(apartments_links)}: {apartment_url}")
                
                max_retries = 3
                retry_count = 0
                success = False
                
                while retry_count < max_retries and not success:
                    try:
                        # Проверяем, что страница не закрыта перед использованием
                        page_closed = False
                        try:
                            if page.isClosed():
                                page_closed = True
                        except Exception:
                            # Если проверка не удалась, считаем страницу закрытой
                            page_closed = True
                        
                        if page_closed:
                            print(f"    Страница закрыта, перезапускаю браузер...")
                            browser, page, proxy_url = await restart_browser(browser, headless=False, set_domclick_cookies=False)
                            await asyncio.sleep(3)  # Пауза после перезапуска
                        
                        apartment_data = await parse_apartment_page(page, apartment_url)
                        
                        # Проверяем, что данные действительно собраны (есть хотя бы title или другие поля)
                        if apartment_data.get("title") or apartment_data.get("price") or apartment_data.get("factoids"):
                            building_data["apartments"].append(apartment_data)
                            # Сохраняем данные и прогресс после каждой квартиры
                            save_progress(building_idx, apt_idx + 1)
                            save_data(result_data)
                            print(f"    ✓ Сохранено: {apartment_data.get('title', 'Без названия')}")
                            success = True
                        else:
                            # Если данных нет, считаем это ошибкой и пробуем снова
                            raise Exception("Не удалось собрать данные с страницы (пустой результат)")
                        
                    except Exception as e:
                        error_str = str(e)
                        print(f"    ✗ Ошибка при парсинге квартиры: {e}")
                        
                        # Проверяем тип ошибки
                        is_proxy_error = "ERR_PROXY_CONNECTION_FAILED" in error_str or "proxy" in error_str.lower()
                        # Проверяем, закрыта ли страница
                        page_closed_check = False
                        try:
                            page_closed_check = page.isClosed()
                        except Exception:
                            page_closed_check = True
                        is_session_error = "сессия закрыта" in error_str.lower() or "session closed" in error_str.lower() or "Target closed" in error_str or "page closed" in error_str.lower() or page_closed_check
                        is_connection_error = "connection" in error_str.lower() or "timeout" in error_str.lower() or "network" in error_str.lower()
                        
                        # Если ошибка прокси или сессии, СРАЗУ перезапускаем браузер и пробуем снова (не считаем как попытку)
                        if is_proxy_error or is_session_error or is_connection_error:
                            print(f"    Ошибка подключения/прокси, перезапускаю браузер с новым прокси...")
                            try:
                                await asyncio.sleep(3)  # Пауза перед перезапуском
                                browser, page, proxy_url = await restart_browser(browser, headless=False, set_domclick_cookies=False)
                                print(f"    ✓ Браузер перезапущен с новым прокси: {proxy_url}")
                                await asyncio.sleep(3)  # Пауза после перезапуска
                                continue  # Пробуем снова БЕЗ увеличения счетчика попыток
                            except Exception as restart_error:
                                print(f"    ✗ Ошибка перезапуска браузера: {restart_error}")
                                retry_count += 1
                                if retry_count < max_retries:
                                    await asyncio.sleep(5)  # Пауза перед следующей попыткой перезапуска
                                    continue
                                else:
                                    # Если не удалось перезапустить после нескольких попыток
                                    building_data["apartments"].append({
                                        "url": apartment_url,
                                        "error": f"Не удалось перезапустить браузер: {restart_error}"
                                    })
                                    save_progress(building_idx, apt_idx + 1)
                                    save_data(result_data)
                                    print(f"    ✗ Не удалось перезапустить браузер, пропускаю эту квартиру")
                                    await asyncio.sleep(2)
                                    break
                        
                        # Для других ошибок увеличиваем счетчик попыток
                        retry_count += 1
                        print(f"    Попытка {retry_count}/{max_retries}")
                        
                        # Если все попытки исчерпаны
                        if retry_count >= max_retries:
                            # Добавляем пустую запись с ошибкой
                            building_data["apartments"].append({
                                "url": apartment_url,
                                "error": str(e)
                            })
                            save_progress(building_idx, apt_idx + 1)
                            save_data(result_data)
                            print(f"    ✗ Все попытки исчерпаны, пропускаю эту квартиру")
                            await asyncio.sleep(2)  # Пауза перед следующей квартирой
                        else:
                            # Пауза перед следующей попыткой
                            wait_time = min(5 * retry_count, 15)  # Увеличиваем паузу с каждой попыткой (до 15 сек)
                            print(f"    Пауза {wait_time} секунд перед следующей попыткой...")
                            await asyncio.sleep(wait_time)
                
                # Пауза между квартирами для снижения нагрузки
                await asyncio.sleep(1)
            
            # Сбрасываем индекс квартиры для следующего ЖК
            start_apartment_index = 0
            save_progress(building_idx + 1, 0)
        
        print(f"\n✓ Обработка завершена")
        print(f"Итоговая статистика:")
        total_apartments = sum(len(bd.get("apartments", [])) for bd in result_data)
        print(f"  ЖК обработано: {len(result_data)}")
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
