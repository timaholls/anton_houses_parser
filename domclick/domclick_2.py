#!/usr/bin/env python3
"""
Проход по списку URL и сбор данных с карточек на страницах (только 1-й элемент).

Логика:
- Читает JSON со ссылками (complex_links.json по умолчанию)
- Для каждого URL определяет количество страниц по селектору пагинации
- Переходит по страницам с параметром offset (шаг 20): 0, 20, 40, ...
- На каждой странице ждёт [data-e2e-id="offers-list__item"], берёт первый элемент и
  собирает: изображения из галереи, параметры квартиры, адрес, название/ссылку ЖК
- На каждой странице поиска извлекает фотографии ЖК из галереи
  (элементы с data-e2e-id="complex-header-gallery-image__X")
- Скачивает ВСЕ фотографии (ЖК + квартир), обрабатывает через resize_img.py (сжатие, очистка метаданных)
- Сохраняет изображения локально в папке uploads/ и сохраняет пути в MongoDB:
  - development.photos - пути к фотографиям ЖК
  - apartment_types.*.apartments.*.photos - пути к фотографиям квартир
- Результаты пишет в MongoDB и offers_data.json (массив объектов)
- Прогресс хранит в progress_domclick_2.json: {"url_index": i, "offset": n}
- При ошибках делает до 3 попыток; после 3-й — перезапускает браузер (новый прокси)
  и продолжает с того же места
"""
import asyncio
import json
import os
import base64
import logging
from typing import List, Dict, Any, Tuple
from pathlib import Path
import aiohttp
from io import BytesIO
import sys
import shutil
from urllib.parse import urlparse

# Директория текущего скрипта
PROJECT_ROOT = Path(__file__).resolve().parent

# Папка для сохранения изображений
UPLOADS_DIR = PROJECT_ROOT / "uploads"

from browser_manager import create_browser, create_browser_page, restart_browser
from db_manager import save_to_mongodb
from resize_img import ImageProcessor

LINKS_FILE = PROJECT_ROOT / "complex_links.json"
PROGRESS_FILE = PROJECT_ROOT / "progress_domclick_2.json"
OUTPUT_FILE = PROJECT_ROOT / "offers_data.json"  # больше не используется как основной, оставим для отладки
START_PAUSE_SECONDS = 5  # пауза после открытия URL
STEP_PAUSE_SECONDS = 5  # пауза между страницами/шагами

# Настройка логгера для ImageProcessor
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# Инициализация обработчика изображений
image_processor = ImageProcessor(logger, max_size=(800, 600), max_kb=150)


def create_complex_directory(complex_id: str) -> Path:
    """
    Создает структуру папок для комплекса.
    """
    complex_dir = UPLOADS_DIR / "complexes" / complex_id
    complex_photos_dir = complex_dir / "complex_photos"
    apartments_dir = complex_dir / "apartments"

    # Создаем все необходимые папки
    complex_photos_dir.mkdir(parents=True, exist_ok=True)
    apartments_dir.mkdir(parents=True, exist_ok=True)

    return complex_dir


def get_complex_id_from_url(url: str) -> str:
    """
    Извлекает ID комплекса из URL.
    """
    try:
        # Пример: https://domclick.ru/complexes/zhk-8-marta__109690
        parsed = urlparse(url)
        path_parts = parsed.path.split('/')
        if 'complexes' in path_parts:
            complex_index = path_parts.index('complexes')
            if complex_index + 1 < len(path_parts):
                return path_parts[complex_index + 1]
    except Exception:
        pass

    # Fallback - используем хеш URL
    import hashlib
    return hashlib.md5(url.encode()).hexdigest()[:10]


def save_processed_image(image_data: bytes, file_path: Path) -> bool:
    """
    Сохраняет обработанное изображение в файл.
    """
    try:
        file_path.parent.mkdir(parents=True, exist_ok=True)
        with open(file_path, 'wb') as f:
            f.write(image_data)
        return True
    except Exception as e:
        logger.error(f"Ошибка сохранения файла {file_path}: {e}")
        return False


def load_links(path: str = str(LINKS_FILE)) -> List[str]:
    with open(path, "r", encoding="utf-8") as f:
        data = json.load(f)
    # допускаем как список, так и словарь с ключом links
    if isinstance(data, dict) and "links" in data:
        return list(data.get("links") or [])
    return list(data or [])


def load_progress(path: str = str(PROGRESS_FILE)) -> Tuple[int, int]:
    if not os.path.exists(path):
        return 0, 0
    try:
        with open(path, "r", encoding="utf-8") as f:
            obj = json.load(f)
        return int(obj.get("url_index", 0)), int(obj.get("offset", 0))
    except Exception:
        return 0, 0


def save_progress(url_index: int, offset: int, path: str = str(PROGRESS_FILE)) -> None:
    tmp_path = path + ".tmp"
    with open(tmp_path, "w", encoding="utf-8") as f:
        json.dump({"url_index": url_index, "offset": offset}, f, ensure_ascii=False)
    os.replace(tmp_path, path)


def set_offset_param(url: str, offset: int) -> str:
    try:
        from urllib.parse import urlparse, parse_qsl, urlencode, urlunparse
        parts = urlparse(url)
        q = dict(parse_qsl(parts.query, keep_blank_values=True))
        q["offset"] = str(offset)
        new_query = urlencode(q, doseq=True)
        return urlunparse((parts.scheme, parts.netloc, parts.path, parts.params, new_query, parts.fragment))
    except Exception:
        sep = '&' if ('?' in url) else '?'
        return f"{url}{sep}offset={offset}"


async def download_and_process_image(session: aiohttp.ClientSession, image_url: str, file_path: Path) -> str:
    """
    Скачивает изображение по URL, обрабатывает его через resize_img.py и сохраняет локально.
    Возвращает относительный путь к файлу.
    """
    try:
        async with session.get(image_url, timeout=aiohttp.ClientTimeout(total=30)) as response:
            if response.status == 200:
                image_bytes = await response.read()

                # Обрабатываем изображение через resize_img.py
                input_bytes = BytesIO(image_bytes)
                try:
                    processed_bytes = image_processor.process(input_bytes)
                except Exception as process_error:
                    logger.error(f"Ошибка resize_img.py: {process_error}")
                    return None

                # Сохраняем обработанное изображение в файл
                processed_bytes.seek(0)
                image_data = processed_bytes.read()

                if save_processed_image(image_data, file_path):
                    # Возвращаем относительный путь от uploads
                    relative_path = file_path.relative_to(UPLOADS_DIR)
                    return str(relative_path).replace('\\', '/')  # Универсальные разделители
                else:
                    return None
            else:
                logger.warning(f"HTTP {response.status} для {image_url}")
                return None
    except Exception as e:
        logger.error(f"Ошибка скачивания {image_url}: {e}")
        return None


async def process_complex_photos(photo_urls: List[str], complex_dir: Path) -> List[str]:
    """
    Обрабатывает список URL фотографий ЖК и сохраняет их локально.
    Возвращает список путей к файлам.
    """
    if not photo_urls:
        return []

    processed_photos = []
    complex_photos_dir = complex_dir / "complex_photos"

    async with aiohttp.ClientSession() as session:
        # Обрабатываем до 5 фотографий параллельно
        semaphore = asyncio.Semaphore(5)

        async def process_single_photo(url, index):
            async with semaphore:
                file_path = complex_photos_dir / f"photo_{index + 1}.jpg"
                return await download_and_process_image(session, url, file_path)

        tasks = [process_single_photo(url, i) for i, url in enumerate(photo_urls[:8])]  # максимум 8 фото ЖК
        results = await asyncio.gather(*tasks, return_exceptions=True)

        for result in results:
            if isinstance(result, str) and result:
                processed_photos.append(result)

    logger.info(f"Обработано {len(processed_photos)} из {len(photo_urls)} фотографий ЖК")
    return processed_photos


async def process_apartment_photos(apartment_data: Dict[str, Any], apartment_dir: Path) -> Dict[str, Any]:
    """
    Обрабатывает фотографии для одной квартиры и сохраняет их локально.
    Возвращает данные с путями к файлам.
    """
    if not apartment_data.get("images"):
        return {
            "offer": apartment_data.get("offer"),
            "photos": []
        }

    image_urls = apartment_data["images"]
    if not image_urls:
        return {
            "offer": apartment_data.get("offer"),
            "photos": []
        }

    processed_images = []

    async with aiohttp.ClientSession() as session:
        # Обрабатываем до 3 фотографий параллельно для квартир
        semaphore = asyncio.Semaphore(3)

        async def process_single_photo(url, index):
            async with semaphore:
                file_path = apartment_dir / f"photo_{index + 1}.jpg"
                return await download_and_process_image(session, url, file_path)

        tasks = [process_single_photo(url, i) for i, url in enumerate(image_urls[:3])]  # максимум 3 фото на квартиру
        results = await asyncio.gather(*tasks, return_exceptions=True)

        for i, result in enumerate(results):
            if isinstance(result, str) and result:
                processed_images.append(result)
            elif isinstance(result, Exception):
                logger.error(f"Ошибка обработки фото {i + 1}: {result}")
            else:
                logger.warning(f"Фото {i + 1} не обработано: {type(result)}")

    # Возвращаем данные квартиры с путями к файлам
    result = {
        "offer": apartment_data.get("offer"),
        "photos": processed_images
    }
    return result


async def process_all_apartment_types(apartment_types: Dict[str, Any], complex_dir: Path) -> Dict[str, Any]:
    """
    Обрабатывает все фотографии во всех типах квартир и сохраняет их локально.
    """
    if not apartment_types:
        return apartment_types

    processed_types = {}
    apartments_dir = complex_dir / "apartments"

    for apartment_type, type_data in apartment_types.items():
        # Обрабатываем разные структуры данных
        if isinstance(type_data, list):
            # Если type_data - это список квартир напрямую
            apartments = type_data
        elif isinstance(type_data, dict) and "apartments" in type_data:
            # Если type_data - это словарь с ключом "apartments"
            apartments = type_data.get("apartments", [])
        else:
            # Если неизвестная структура, пропускаем
            processed_types[apartment_type] = type_data
            continue

        processed_apartments = []
        apartment_type_dir = apartments_dir / apartment_type.replace('-', '_').replace('комн', 'komn')

        for i, apartment in enumerate(apartments):
            if isinstance(apartment, dict):
                apartment_dir = apartment_type_dir / f"apartment_{i + 1}"
                processed_apartment = await process_apartment_photos(apartment, apartment_dir)
                processed_apartments.append(processed_apartment)
            else:
                processed_apartments.append(apartment)

        # Правильно формируем результат в зависимости от исходной структуры
        if isinstance(type_data, list):
            # Если исходные данные были списком, возвращаем список
            processed_types[apartment_type] = processed_apartments
        else:
            # Если исходные данные были словарем, возвращаем словарь
            processed_types[apartment_type] = {
                **type_data,
                "apartments": processed_apartments
            }

    return processed_types


class SkipUrlException(Exception):
    pass


async def wait_offers(page) -> None:
    try:
        await page.waitForSelector('[data-e2e-id="offers-list__item"]', {"timeout": 60000})
    except Exception as e:
        # Проверяем страницу с ошибкой "Что-то пошло не так..."
        try:
            error_page = await page.evaluate("""
            () => {
              const t = (document.body && document.body.innerText) || '';
              return /Что-то пошло не так/i.test(t) || /В работе приложения произошла неизвестная ошибка/i.test(t);
            }
            """)
        except Exception:
            error_page = False

        if error_page:
            raise Exception("Обнаружена страница с ошибкой Domclick - требуется перезапуск браузера")

        # Проверяем экран "Поиск не дал результатов"
        try:
            no_results = await page.evaluate("""
            () => {
              const t = (document.body && document.body.innerText) || '';
              return /Поиск не дал результатов/i.test(t);
            }
            """)
        except Exception:
            no_results = False
        if no_results:
            raise SkipUrlException("Поиск не дал результатов")
        # Иначе триггерим ретраи/перезапуск
        raise TimeoutError("Не найден список офферов [data-e2e-id='offers-list__item']") from e


async def get_pages_count(page) -> int:
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


async def collect_page_items_grouped(page) -> Dict[str, Any]:
    script = """
    () => {
      const toAbs = (u) => { try { return new URL(u, location.origin).href; } catch { return u || null; } };
      const isPngJpg = (u) => /\.(png|jpe?g)(?:$|\?|#)/i.test(String(u || ''));
      const pickFromSrcset = (srcset) => {
        if (!srcset) return null;
        const first = String(srcset).split(',')[0].trim().split(' ')[0];
        return first || null;
      };
      const extractImagesFromGallery = (root) => {
        if (!root) return [];
        const urls = new Set();
        root.querySelectorAll('img').forEach(img => {
          [img.getAttribute('src'), img.getAttribute('data-src'), img.getAttribute('data-lazy'), img.getAttribute('data-original')]
            .filter(Boolean).map(toAbs).filter(isPngJpg).forEach(u => urls.add(u));
          const picked = pickFromSrcset(img.getAttribute('srcset'));
          if (isPngJpg(picked)) urls.add(toAbs(picked));
        });
        root.querySelectorAll('source[srcset]').forEach(s => {
          const picked = pickFromSrcset(s.getAttribute('srcset'));
          if (isPngJpg(picked)) urls.add(toAbs(picked));
        });
        root.querySelectorAll("[style*='background']").forEach(el => {
          const st = el.getAttribute('style') || '';
          const m = st.match(/url\((['\"]?)(.*?)\1\)/i);
          if (m && isPngJpg(m[2])) urls.add(toAbs(m[2]));
        });
        return [...urls];
      };

      const normalizeRoom = (txt) => {
        if (!txt) return 'Другое';
        const s = txt.toLowerCase();
        if (s.includes('студ')) return 'Студия';
        const m = s.match(/(^|\s)([1-9]+)\s*[-–—]?\s*комн/i);
        if (m) return `${m[2]}-комн`;
        const m2 = s.match(/^([1-9]+)\s*[-–—]?\s*комм?/);
        if (m2) return `${m2[1]}-комн`;
        return 'Другое';
      };

      const items = Array.from(document.querySelectorAll('[data-e2e-id="offers-list__item"]'));
      if (!items.length) return null;

      const first = items[0];
      const addressEl = first.querySelector('[data-e2e-id="product-snippet-address"], [data-e2-id="product-snippet-address"]');
      const addressText = addressEl ? addressEl.textContent.replace(/\s+/g, ' ').trim() : null;
      const complexLinkEl = first.querySelector('a[href^="/complexes/"], a[href*="//domclick.ru/complexes/"], a[href*="//ufa.domclick.ru/complexes/"]');
      const complexName = complexLinkEl ? complexLinkEl.textContent.replace(/\s+/g, ' ').trim() : null;
      const complexHref = complexLinkEl ? toAbs(complexLinkEl.getAttribute('href') || complexLinkEl.href) : null;

      const offers = {};
      for (const item of items) {
        const offerEl = item.querySelector('[data-test="product-snippet-property-offer"]');
        const offerText = offerEl ? offerEl.innerText.replace(/\s+/g, ' ').trim() : null;
        const roomKey = normalizeRoom(offerText);
        const galleryRoot = item.querySelector('[data-e2e-id="product-snippet-gallery"]') || item.querySelector('[data-e2e-id^="product-snippet-gallery"]') || item;
        const images = extractImagesFromGallery(galleryRoot);
        const card = { offer: offerText, images };
        if (!offers[roomKey]) offers[roomKey] = [];
        offers[roomKey].push(card);
      }

      // извлекаем фотографии ЖК из галереи на странице поиска
      const complexPhotos = [];
      const galleryContainer = document.querySelector('[data-e2e-id="complex-header-gallery"]');
      
      if (galleryContainer) {
        const imageElements = galleryContainer.querySelectorAll('[data-e2e-id^="complex-header-gallery-image__"]');
        imageElements.forEach(element => {
          const img = element.querySelector('img.picture-image-object-fit--cover-820-5-0-5.picture-imageFillingContainer-4a2-5-0-5');
          if (img && img.src) {
            try {
              const absoluteUrl = new URL(img.src, location.origin).href;
              complexPhotos.push(absoluteUrl);
            } catch (e) {
              if (img.src.startsWith('http')) {
                complexPhotos.push(img.src);
              }
            }
          }
        });
      }

      return { address: addressText, complexName, complexHref, offers, complexPhotos };
    }
    """
    try:
        return await page.evaluate(script)
    except Exception:
        return None


async def run() -> None:
    urls = load_links(str(LINKS_FILE))
    if not urls:
        print("Файл со ссылками пуст или отсутствует:", LINKS_FILE)
        return

    url_index, offset = load_progress(str(PROGRESS_FILE))
    url_index = max(0, min(url_index, len(urls)))
    print(f"Старт: url_index={url_index}, offset={offset}, всего URL: {len(urls)}")

    results: List[Dict[str, Any]] = []
    if os.path.exists(str(OUTPUT_FILE)):
        try:
            with open(str(OUTPUT_FILE), 'r', encoding='utf-8') as f:
                old = json.load(f)
                if isinstance(old, list):
                    results = old
        except Exception:
            pass

    browser, _ = await create_browser(headless=False)
    page = await create_browser_page(browser)

    try:
        while url_index < len(urls):
            base_url = urls[url_index]
            print(f"→ URL [{url_index + 1}/{len(urls)}]: {base_url}")

            if offset % 20 != 0:
                offset = (offset // 20) * 20

            attempts = 0
            while True:
                try:
                    await page.goto(set_offset_param(base_url, 0), timeout=120000)
                    # Пауза для стабильной загрузки динамики
                    await asyncio.sleep(START_PAUSE_SECONDS)
                    await wait_offers(page)
                    pages_count = await get_pages_count(page)
                    break
                except SkipUrlException as e:
                    print(f"URL без результатов: {e}. Пропускаю {base_url}")
                    pages_count = 0
                    break
                except Exception as e:
                    # Проверяем, является ли это ошибкой страницы Domclick
                    if "страница с ошибкой Domclick" in str(e):
                        print(f"Обнаружена страница с ошибкой Domclick: {e}")
                        try:
                            browser, page, _ = await restart_browser(browser, headless=False)
                            attempts = 0  # Сбрасываем счетчик попыток после перезапуска
                            continue  # Повторяем попытку с новым браузером
                        except Exception as restart_error:
                            print(f"  Ошибка при перезапуске браузера: {restart_error}")
                            # Пропускаем этот URL и переходим к следующему
                            break

                    attempts += 1
                    print(f"Ошибка при открытии базовой страницы: {e} (попытка {attempts}/3)")
                    if attempts >= 3:
                        try:
                            browser, page, _ = await restart_browser(browser, headless=False)
                            attempts = 0
                        except Exception as restart_error:
                            print(f"  Ошибка при перезапуске браузера: {restart_error}")
                            # Пропускаем этот URL и переходим к следующему
                            break
                    else:
                        await asyncio.sleep(2)

            total_pages = max(1, pages_count)

            # агрегируем данные по всем оффсетам для текущего URL
            aggregated_address: str = None
            aggregated_complex_name: str = None
            aggregated_complex_href: str = None
            aggregated_offers: Dict[str, List[Dict[str, Any]]] = {}
            complex_gallery_images: List[str] = []  # фотографии ЖК (извлекаем один раз)

            while True:
                if offset >= total_pages * 20:
                    break
                current_url = set_offset_param(base_url, offset)
                print(f"  Переход: offset={offset} → {current_url}")

                attempts = 0
                while True:
                    try:
                        await page.goto(current_url, timeout=120000)
                        # Пауза между шагами для докрутки ленивых элементов
                        await asyncio.sleep(STEP_PAUSE_SECONDS)
                        await wait_offers(page)
                        data = await collect_page_items_grouped(page)
                        # Если не нашли нужные элементы — считаем за ошибку шага
                        if not data or not (data.get("offers") or {}):
                            raise ValueError("Селекторы не найдены или offers пустой")

                        if aggregated_address is None:
                            aggregated_address = data.get("address")
                            aggregated_complex_name = data.get("complexName")
                            aggregated_complex_href = data.get("complexHref")

                            # извлекаем фотографии ЖК только при первом сборе данных
                            if not complex_gallery_images:
                                complex_gallery_images = data.get("complexPhotos") or []

                        # объединяем группы офферов
                        offers = data.get("offers") or {}
                        for group, cards in offers.items():
                            if group not in aggregated_offers:
                                aggregated_offers[group] = []
                            aggregated_offers[group].extend(cards)

                        offset += 20
                        # Небольшая дополнительная пауза перед следующим оффсетом
                        await asyncio.sleep(1)
                        save_progress(url_index, offset, str(PROGRESS_FILE))
                        break
                    except SkipUrlException as e:
                        print(f"Offset без результатов: {e}. Завершаю обработку URL: {base_url}")
                        offset = total_pages * 20
                        break
                    except Exception as e:
                        # Проверяем, является ли это ошибкой страницы Domclick
                        if "страница с ошибкой Domclick" in str(e):
                            print(f"  Обнаружена страница с ошибкой Domclick на offset={offset}: {e}")
                            try:
                                browser, page, _ = await restart_browser(browser, headless=False)
                                attempts = 0  # Сбрасываем счетчик попыток после перезапуска
                                # Повторяем попытку с новым браузером
                                continue
                            except Exception as restart_error:
                                print(f"  Ошибка при перезапуске браузера: {restart_error}")
                                # Пропускаем этот offset и переходим к следующему
                                offset += 20
                                break

                        attempts += 1
                        print(f"  Ошибка на странице offset={offset}: {e} (попытка {attempts}/3)")
                        if attempts >= 3:
                            try:
                                browser, page, _ = await restart_browser(browser, headless=False)
                                attempts = 0
                            except Exception as restart_error:
                                print(f"  Ошибка при перезапуске браузера: {restart_error}")
                                # Пропускаем этот offset и переходим к следующему
                                offset += 20
                                break
                        else:
                            await asyncio.sleep(2)

            # формируем запись под Mongo-схему
            def to_db_item(complex_photos_paths: List[str] = None, processed_apartment_types: Dict[str, Any] = None) -> \
                    Dict[str, Any]:
                # Используем обработанные данные квартир, если они есть
                apartment_types_data = processed_apartment_types or aggregated_offers

                apartment_types: Dict[str, Any] = {}
                for group, cards in (apartment_types_data or {}).items():
                    # cards может быть как списком, так и словарем с ключом "apartments"
                    if isinstance(cards, list):
                        # Если cards - это список квартир напрямую (уже обработанных)
                        apartment_types[group] = {
                            "apartments": [
                                {
                                    "title": c.get("offer"),
                                    "photos": c.get("photos") or [],  # пути к файлам
                                }
                                for c in cards
                            ]
                        }
                    elif isinstance(cards, dict) and "apartments" in cards:
                        # Если cards - это словарь с ключом "apartments"
                        apartment_list = cards["apartments"]
                        apartment_types[group] = {
                            "apartments": [
                                {
                                    "title": c.get("offer"),
                                    "photos": c.get("photos") or [],  # пути к файлам
                                }
                                for c in apartment_list
                            ]
                        }
                    else:
                        # Если неизвестная структура, пропускаем
                        apartment_types[group] = cards
                        continue
                return {
                    "url": aggregated_complex_href or base_url,
                    "development": {
                        "complex_name": aggregated_complex_name,
                        "address": aggregated_address,
                        "source_url": base_url,
                        "photos": complex_photos_paths or [],  # пути к фотографиям ЖК
                    },
                    "apartment_types": apartment_types,
                }

            # Создаем структуру папок для комплекса
            complex_id = get_complex_id_from_url(aggregated_complex_href or base_url)
            complex_dir = create_complex_directory(complex_id)

            # Обрабатываем фотографии ЖК и сохраняем локально
            complex_photos_paths = []
            if complex_gallery_images:
                try:
                    complex_photos_paths = await process_complex_photos(complex_gallery_images, complex_dir)
                except Exception as e:
                    logger.error(f"Ошибка при обработке фотографий ЖК: {e}")
                    complex_photos_paths = []

            # Обрабатываем фотографии всех квартир и сохраняем локально
            processed_apartment_types = aggregated_offers
            if aggregated_offers:
                try:
                    processed_apartment_types = await process_all_apartment_types(aggregated_offers, complex_dir)
                except Exception as e:
                    logger.error(f"Ошибка при обработке фотографий квартир: {e}")
                    processed_apartment_types = aggregated_offers

            db_item = to_db_item(complex_photos_paths, processed_apartment_types)

            try:
                save_to_mongodb([db_item])


            except Exception as e:
                print(f"Ошибка записи в MongoDB: {e}. Сохраню в {str(OUTPUT_FILE)} для отладки.")
                results.append({"sourceUrl": base_url,
                                "data": {"address": aggregated_address, "complexName": aggregated_complex_name,
                                         "complexHref": aggregated_complex_href, "offers": processed_apartment_types,
                                         "complexPhotosPaths": complex_photos_paths}})
                with open(str(OUTPUT_FILE), 'w', encoding='utf-8') as f:
                    json.dump(results, f, ensure_ascii=False, indent=2)

            url_index += 1
            offset = 0
            save_progress(url_index, offset, str(PROGRESS_FILE))
    finally:
        try:
            await browser.close()
        except Exception as e:
            print(f"Ошибка при закрытии браузера: {e}")
            # Игнорируем ошибки закрытия браузера


if __name__ == "__main__":
    asyncio.get_event_loop().run_until_complete(run())
