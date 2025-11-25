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
import os
import re
from datetime import datetime, timezone
from typing import List, Dict, Any, Optional
from pathlib import Path
from io import BytesIO
import aiohttp
from urllib.parse import urlparse
from dotenv import load_dotenv
from pymongo import MongoClient

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
PROGRESS_FILE = PROJECT_ROOT / "progress_cian_3.json"
REPROCESS_FILE = PROJECT_ROOT / "buildings_to_reprocess.json"
MONGO_COLLECTION_NAME = "unified_houses_2"

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


def load_buildings_to_reprocess(path: str = str(REPROCESS_FILE)) -> Optional[List[Dict[str, str]]]:
    """Загружает список ЖК для повторной обработки."""
    if not Path(path).exists():
        return None

    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
        if isinstance(data, list):
            return data
        return None
    except Exception as e:
        print(f"Ошибка при чтении файла {path}: {e}")
        return None


def filter_buildings_by_reprocess_list(
    buildings: List[Dict[str, Any]], 
    reprocess_list: List[Dict[str, str]]
) -> List[Dict[str, Any]]:
    """Фильтрует список ЖК, оставляя только те, что есть в списке для повторной обработки."""
    if not reprocess_list:
        return buildings
    
    # Создаем множество названий и ссылок для быстрого поиска
    reprocess_titles = {item.get("title") for item in reprocess_list}
    reprocess_links = {item.get("link") for item in reprocess_list}
    
    filtered = []
    for building in buildings:
        building_title = building.get('title', '')
        building_link = building.get('link', '')
        
        # Проверяем по названию или ссылке
        if building_title in reprocess_titles or building_link in reprocess_links:
            filtered.append(building)
    
    return filtered


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
            json.dump({"building_index": building_index, "apartment_index": apartment_index}, f, ensure_ascii=False,
                      indent=2)
        Path(tmp_path).replace(path)
    except Exception as e:
        print(f"Ошибка при сохранении прогресса {path}: {e}")


def ensure_factoid(factoids: List[Dict[str, Any]], label: str, value: Optional[str]) -> None:
    """Добавляет или обновляет значение в массиве factoids."""
    if not value:
        return
    for item in factoids:
        if item.get("label") == label:
            item["value"] = value
            return
    factoids.append({"label": label, "value": value})


def create_mongo_connection():
    """Создает подключение к MongoDB и возвращает клиент и коллекцию."""
    mongo_uri = os.getenv("MONGO_URI", "mongodb://root:Kfleirb_17@176.98.177.188:27017/admin")
    db_name = os.getenv("DB_NAME", "houses")
    client = MongoClient(mongo_uri)
    collection = client[db_name][MONGO_COLLECTION_NAME]
    return client, collection


def upsert_building_data(collection, building_data: Dict[str, Any]) -> None:
    """Сохраняет данные ЖК в коллекцию MongoDB."""
    if collection is None or not building_data:
        return

    building_link = building_data.get("building_link")
    building_title = building_data.get("building_title")

    if not building_link and not building_title:
        logger.warning("Пропускаем апдейт MongoDB — отсутствуют building_link и building_title")
        return

    document = dict(building_data)
    document["source"] = "cian"
    document["updatedAt"] = datetime.now(timezone.utc).isoformat()

    query = {"building_link": building_link} if building_link else {"building_title": building_title}

    try:
        collection.replace_one(query, document, upsert=True)
    except Exception as e:
        logger.error(f"Ошибка при сохранении ЖК в MongoDB: {e}")


def load_building_record(collection, building_title: str, building_link: str, building_photos: List[str]) -> Dict[str, Any]:
    """Загружает существующую запись ЖК из MongoDB или создает новую структуру."""
    base_data = {
        "building_title": building_title,
        "building_link": building_link,
        "building_photos": building_photos,
        "apartments": []
    }

    if collection is None:
        return base_data

    try:
        query = {"building_link": building_link} if building_link else {"building_title": building_title}
        existing = collection.find_one(query, projection={"_id": 0})
        if existing:
            existing.setdefault("building_title", building_title)
            existing.setdefault("building_link", building_link)
            existing.setdefault("building_photos", building_photos)
            existing.setdefault("apartments", [])
            return existing
    except Exception as e:
        logger.error(f"Ошибка чтения ЖК из MongoDB: {e}")

    return base_data


def save_building_state(building_data: Dict[str, Any], collection) -> None:
    """Сохраняет текущее состояние ЖК в MongoDB."""
    if building_data:
        upsert_building_data(collection, building_data)


def upsert_apartment_entry(building_data: Dict[str, Any], apartment_entry: Dict[str, Any]) -> bool:
    """
    Добавляет или обновляет запись квартиры в текущем объекте ЖК.
    Возвращает True, если добавлена новая запись (для статистики).
    """
    if not building_data:
        return False

    apartments = building_data.setdefault("apartments", [])
    apartment_url = apartment_entry.get("url")

    if apartment_url:
        for idx, existing in enumerate(apartments):
            if existing.get("url") == apartment_url:
                apartments[idx] = apartment_entry
                return False

    apartments.append(apartment_entry)
    return True


async def mark_apartment_processed(building_idx: int, apartment_index: int, progress_state: Dict[str, Any],
                                   progress_lock: asyncio.Lock) -> None:
    """
    Отмечает квартиру как обработанную и обновляет файл прогресса,
    когда сформирована непрерывная последовательность обработанных квартир.
    """
    async with progress_lock:
        progress_state["processed_flags"][apartment_index] = True
        current_pointer = progress_state["next_index"]

        while progress_state["processed_flags"].get(current_pointer, False):
            current_pointer += 1

        if current_pointer != progress_state["next_index"]:
            progress_state["next_index"] = current_pointer
            save_progress(building_idx, current_pointer)


async def increment_stat(stats: Dict[str, int], stats_lock: asyncio.Lock, field: str, delta: int = 1) -> None:
    """Потокобезопасно увеличивает счетчик статистики."""
    async with stats_lock:
        stats[field] = stats.get(field, 0) + delta


async def apartment_worker(
    worker_id: int,
    apartment_queue: asyncio.Queue,
    building_idx: int,
    building_title: str,
    building_data: Dict[str, Any],
    mongo_collection,
    progress_state: Dict[str, Any],
    progress_lock: asyncio.Lock,
    building_lock: asyncio.Lock,
    stats: Dict[str, int],
    stats_lock: asyncio.Lock,
    headless: bool = False,
) -> None:
    """Воркера, который обрабатывает квартиры из очереди с собственным браузером."""

    browser, proxy_url = await create_browser(headless=headless)
    page = await create_browser_page(browser, set_domclick_cookies=False)

    async def store_entry(entry: Dict[str, Any]) -> None:
        async with building_lock:
            is_new = upsert_apartment_entry(building_data, entry)
            save_building_state(building_data, mongo_collection)
        if is_new:
            await increment_stat(stats, stats_lock, "new_entries", 1)

    try:
        while True:
            task = await apartment_queue.get()
            if task is None:
                apartment_queue.task_done()
                print(f"    [Browser {worker_id}] Завершил работу (нет задач)")
                break

            apartment_index, apartment_url = task
            print(
                f"    [Browser {worker_id}] → обрабатываю квартиру {apartment_index + 1}: {apartment_url}"
            )
            retry_count = 0
            max_retries = 3
            success = False
            last_error: Optional[Exception] = None

            try:
                while retry_count < max_retries and not success:
                    try:
                        page_closed = False
                        try:
                            if page.isClosed():
                                page_closed = True
                        except Exception:
                            page_closed = True

                        if page_closed:
                            print(f"    [Browser {worker_id}] Страница закрыта, перезапускаю браузер...")
                            browser, page, proxy_url = await restart_browser(
                                browser, headless=headless, set_domclick_cookies=False
                            )
                            await asyncio.sleep(3)

                        apartment_data = await parse_apartment_page(page, apartment_url)

                        if apartment_data.get("title") or apartment_data.get("price") or apartment_data.get("factoids"):
                            await store_entry(apartment_data)
                            await mark_apartment_processed(
                                building_idx, apartment_index, progress_state, progress_lock
                            )
                            print(
                                f"    [Browser {worker_id}] ✓ Сохранено: "
                                f"{apartment_data.get('title', 'Без названия')}"
                            )
                            success = True
                        else:
                            raise Exception("Не удалось собрать данные с страницы (пустой результат)")

                    except Exception as e:
                        last_error = e
                        error_str = str(e)
                        print(f"    [Browser {worker_id}] ✗ Ошибка при парсинге квартиры: {e}")

                        is_proxy_error = "ERR_PROXY_CONNECTION_FAILED" in error_str or "proxy" in error_str.lower()
                        page_closed_check = False
                        try:
                            page_closed_check = page.isClosed()
                        except Exception:
                            page_closed_check = True
                        is_session_error = (
                            "сессия закрыта" in error_str.lower()
                            or "session closed" in error_str.lower()
                            or "target closed" in error_str.lower()
                            or "page closed" in error_str.lower()
                            or page_closed_check
                        )
                        is_connection_error = (
                            "connection" in error_str.lower()
                            or "timeout" in error_str.lower()
                            or "network" in error_str.lower()
                        )

                        if is_proxy_error or is_session_error or is_connection_error:
                            print(
                                f"    [Browser {worker_id}] Ошибка подключения/прокси, перезапускаю браузер "
                                f"с новым прокси..."
                            )
                            try:
                                await asyncio.sleep(3)
                                browser, page, proxy_url = await restart_browser(
                                    browser, headless=headless, set_domclick_cookies=False
                                )
                                print(
                                    f"    [Browser {worker_id}] ✓ Браузер перезапущен с новым прокси: {proxy_url}"
                                )
                                await asyncio.sleep(3)
                                continue
                            except Exception as restart_error:
                                print(
                                    f"    [Browser {worker_id}] ✗ Ошибка перезапуска браузера: {restart_error}"
                                )
                                last_error = restart_error
                                retry_count += 1
                                if retry_count < max_retries:
                                    await asyncio.sleep(5)
                                    continue
                                else:
                                    break

                        retry_count += 1
                        if retry_count < max_retries:
                            wait_time = min(5 * retry_count, 15)
                            print(
                                f"    [Browser {worker_id}] Попытка {retry_count}/{max_retries}. "
                                f"Пауза {wait_time} секунд..."
                            )
                            await asyncio.sleep(wait_time)

                if not success:
                    error_entry = {
                        "url": apartment_url,
                        "error": str(last_error) if last_error else "Неизвестная ошибка",
                    }
                    await store_entry(error_entry)
                    await mark_apartment_processed(building_idx, apartment_index, progress_state, progress_lock)
                    print(
                        f"    [Browser {worker_id}] ✗ Все попытки исчерпаны, пропускаю квартиру: {apartment_url}"
                    )

            finally:
                apartment_queue.task_done()
                await asyncio.sleep(1)  # Небольшая пауза между квартирами

    finally:
        await browser.close()

async def download_and_process_image(session: aiohttp.ClientSession, image_url: str, apartment_id: str,
                                     image_type: str = "main") -> Optional[str]:
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
        "description": "",
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

        # Используем 'domcontentloaded' вместо полной загрузки страницы
        await page.goto(apartment_url, waitUntil='domcontentloaded', timeout=60000)
        print(f"        → loaded domcontent {apartment_url}")
        
        # Ждем появления ключевых элементов вместо полной загрузки страницы
        # Пробуем дождаться хотя бы одного из основных элементов
        try:
            # Ждем появления названия или галереи (эти элементы появляются первыми)
            await page.waitForSelector(
                '[data-name="OfferTitleNew"], [data-name="OfferGallery"], [data-name="NewbuildingPriceInfo"]',
                timeout=10000
            )
        except Exception:
            # Если не дождались, пробуем продолжить - возможно элементы уже есть
            pass
        
        # Небольшая пауза для подгрузки динамического контента
        await asyncio.sleep(1)

        # Парсим данные через JavaScript
        script = r"""
        () => {
          const result = {
            title: "",
            main_photo: null,
            price: "",
            price_per_square: "",
            factoids: [],
            summary_info: [],
            description: "",
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

          // 2.7. Описание из блока Description
          const descriptionBlock = document.querySelector('[data-name="Description"] [data-id="content"], [data-id="content"]');
          if (descriptionBlock) {
            result.description = descriptionBlock.textContent.trim();
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

        # Описание квартиры (из блока Description)
        if apartment_data.get("description"):
            description_text = apartment_data["description"]
            description_text = description_text.replace('\u00A0', ' ')
            description_text = description_text.replace('&nbsp;', ' ')
            description_text = description_text.replace('\u2009', ' ')
            description_text = description_text.replace('\u2007', ' ')
            description_text = description_text.replace('\u202F', ' ')
            description_text = ' '.join(description_text.split())
            apartment_data["description"] = description_text

            # Пытаемся извлечь срок сдачи из описания
            match = re.search(r'срок\s+сдачи[:\s]*([^.,\\n]+)', description_text, re.IGNORECASE)
            if match:
                completion_value = match.group(1).strip()
                completion_value = completion_value.rstrip(' ,.;')
                ensure_factoid(apartment_data["factoids"], "Срок сдачи", completion_value)

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
                main_photo_url = await download_and_process_image(session, apartment_data["main_photo"], apartment_id,
                                                                  "main")
                apartment_data["main_photo"] = main_photo_url

            # Фотографии отделки
            if apartment_data.get("decoration", {}).get("photos"):
                decoration_photos = []
                for idx, photo_url in enumerate(apartment_data["decoration"]["photos"][:5]):  # Максимум 5 фото
                    # Используем индекс для уникальности позиции, хеш URL добавится в функции download_and_process_image
                    image_type = f"decoration_{idx + 1}"
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

    # Проверяем, есть ли файл со списком для повторной обработки
    reprocess_list = load_buildings_to_reprocess()
    if reprocess_list:
        print(f"Найден файл со списком ЖК для повторной обработки: {REPROCESS_FILE}")
        print(f"ЖК в списке для повторной обработки: {len(reprocess_list)}")
        buildings = filter_buildings_by_reprocess_list(buildings, reprocess_list)
        print(f"После фильтрации осталось ЖК для обработки: {len(buildings)}")
        if not buildings:
            print("Нет ЖК для обработки после фильтрации. Удалите файл buildings_to_reprocess.json для обработки всех ЖК.")
            return
        # Сбрасываем прогресс при работе со списком повторной обработки
        print("Сбрасываю прогресс для обработки списка повторной обработки")
        start_building_index = 0
        start_apartment_index = 0
    else:
        # Загружаем прогресс только если нет списка для повторной обработки
        progress = load_progress()
        start_building_index = progress["building_index"]
        start_apartment_index = progress["apartment_index"]

    print(f"Загружено ЖК: {len(buildings)}")

    if not reprocess_list:
        if start_building_index > 0 or start_apartment_index > 0:
            print(f"Продолжаю с ЖК #{start_building_index + 1}, квартира #{start_apartment_index + 1}")
        else:
            print(f"Начинаю с начала")
    else:
        print(f"Начинаю обработку списка повторной обработки с начала")

    # Создаем подключения
    mongo_client = None
    mongo_collection = None
    try:
        mongo_client, mongo_collection = create_mongo_connection()
    except Exception as e:
        print(f"⚠️ Не удалось подключиться к MongoDB: {e}")
        mongo_client = None
        mongo_collection = None

    if mongo_collection is None:
        print("Нет подключения к MongoDB. Останавливаю работу.")
        return

    try:
        processed_buildings = 0
        total_new_apartments = 0

        for building_idx in range(start_building_index, len(buildings)):
            building = buildings[building_idx]
            building_title = building.get('title', f'ЖК #{building_idx + 1}')
            building_link = building.get('link', '')
            apartments_links = building.get('apartments', [])

            if not apartments_links:
                print(f"\n[{building_idx + 1}/{len(buildings)}] {building_title}: нет ссылок на квартиры")
                save_progress(building_idx + 1, 0)
                processed_buildings += 1
                continue

            print(f"\n[{building_idx + 1}/{len(buildings)}] {building_title}: {len(apartments_links)} квартир")

            building_photos = building.get('photos', [])
            building_data = load_building_record(mongo_collection, building_title, building_link, building_photos)

            start_idx = start_apartment_index if building_idx == start_building_index else 0

            if start_idx >= len(apartments_links):
                print("  Все квартиры уже обработаны ранее для этого ЖК.")
                save_progress(building_idx + 1, 0)
                processed_buildings += 1
                start_apartment_index = 0
                continue

            apartment_queue: asyncio.Queue = asyncio.Queue()
            for apt_idx in range(start_idx, len(apartments_links)):
                await apartment_queue.put((apt_idx, apartments_links[apt_idx]))

            worker_count = min(5, apartment_queue.qsize())
            if worker_count == 0:
                print(f"  Нет квартир для обработки после индекса {start_idx}.")
                save_progress(building_idx + 1, 0)
                processed_buildings += 1
                start_apartment_index = 0
                continue

            print(
                f"  Запускаю {worker_count} браузеров для обработки {apartment_queue.qsize()} квартир "
                f"(начиная с #{start_idx + 1})"
            )

            progress_state = {
                "processed_flags": {idx: True for idx in range(start_idx)},
                "next_index": start_idx
            }
            building_lock = asyncio.Lock()
            progress_lock = asyncio.Lock()
            stats_lock = asyncio.Lock()
            building_stats = {"new_entries": 0}

            for _ in range(worker_count):
                await apartment_queue.put(None)

            workers = [
                asyncio.create_task(
                    apartment_worker(
                        worker_id=i + 1,
                        apartment_queue=apartment_queue,
                        building_idx=building_idx,
                        building_title=building_title,
                        building_data=building_data,
                        mongo_collection=mongo_collection,
                        progress_state=progress_state,
                        progress_lock=progress_lock,
                        building_lock=building_lock,
                        stats=building_stats,
                        stats_lock=stats_lock,
                        headless=False,
                    )
                )
                for i in range(worker_count)
            ]

            await asyncio.gather(*workers)

            save_progress(building_idx + 1, 0)
            processed_buildings += 1
            total_new_apartments += building_stats.get("new_entries", 0)
            start_apartment_index = 0

        print(f"\n✓ Обработка завершена")
        print(f"Итоговая статистика:")
        print(f"  ЖК обработано (за текущий запуск): {processed_buildings}")
        print(f"  Новых записей квартир (включая обновления): {total_new_apartments}")

        if Path(PROGRESS_FILE).exists():
            try:
                Path(PROGRESS_FILE).unlink()
                print(f"  Файл прогресса удален")
            except Exception:
                pass

    finally:
        if mongo_client:
            mongo_client.close()


if __name__ == "__main__":
    asyncio.run(run())
