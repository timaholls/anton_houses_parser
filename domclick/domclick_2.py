#!/usr/bin/env python3
"""
Проход по списку URL и сбор данных через API Domclick.

Логика:
- Читает JSON со ссылками (complex_links.json по умолчанию)
- Для каждого URL извлекает параметры и делает fetch запросы к API bff-search-web.domclick.ru/api/offers/v1
- Запрашивает данные с параметром offset (шаг 20): 0, 20, 40, ...
- Обрабатывает ответы API: извлекает фотографии квартир, адрес, название/ссылку ЖК
- Скачивает ВСЕ фотографии (ЖК + квартир), обрабатывает через resize_img.py (сжатие, очистка метаданных)
- Загружает изображения в S3 и сохраняет пути в MongoDB:
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
from typing import List, Dict, Any, Tuple, Optional
from pathlib import Path
import aiohttp
from io import BytesIO
import sys
import shutil
from urllib.parse import urlparse, parse_qs, urlencode

# Директория текущего скрипта
PROJECT_ROOT = Path(__file__).resolve().parent

# Папка для сохранения изображений
UPLOADS_DIR = PROJECT_ROOT / "uploads"

from browser_manager import create_browser, create_browser_page, restart_browser
from db_manager import save_to_mongodb
from resize_img import ImageProcessor
from s3_service import S3Service
from watermark_on_save import upload_with_watermark

LINKS_FILE = PROJECT_ROOT / "complex_links.json"
PROGRESS_FILE = PROJECT_ROOT / "progress_domclick_2.json"
OUTPUT_FILE = PROJECT_ROOT / "offers_data.json"  # больше не используется как основной, оставим для отладки
START_PAUSE_SECONDS = 5  # пауза после открытия URL
STEP_PAUSE_SECONDS = 5  # пауза между страницами/шагами

# Настройка логгера для ImageProcessor
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# Добавляем обработчик для вывода в консоль, если его еще нет
if not logger.handlers:
    handler = logging.StreamHandler()
    handler.setLevel(logging.INFO)
    formatter = logging.Formatter('%(message)s')
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    # Предотвращаем дублирование логов через родительские логгеры
    logger.propagate = False

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


def normalize_complex_url(url: str) -> str:
    """
    Нормализует URL комплекса, приводя к единому формату.
    Всегда использует ufa.domclick.ru для единообразия.
    """
    if not url:
        return url
    
    try:
        parsed = urlparse(url)
        path_parts = parsed.path.split('/')
        if 'complexes' in path_parts:
            complex_index = path_parts.index('complexes')
            if complex_index + 1 < len(path_parts):
                slug = path_parts[complex_index + 1]
                # Всегда используем ufa.domclick.ru
                return f"https://ufa.domclick.ru/complexes/{slug}"
    except Exception:
        pass
    
    return url


async def extract_construction_from_domclick(page, hod_url: str) -> Dict[str, Any]:
    """Переходит на страницу хода строительства Domclick и извлекает даты и ссылки на фото со всех страниц пагинации.
    Возвращает { construction_stages: [{stage_number, date, photos: [urls<=5]}] }.
    """
    try:
        await page.goto(hod_url, timeout=120000, waitUntil='networkidle0')
        await asyncio.sleep(3)

        # Клик по бейджу и по чекбоксу "2025" в ОДНОМ evaluate (с задержками)
        try:
            clicked_2025 = await page.evaluate(r"""
            async () => {
              const sleep = (ms) => new Promise(r => setTimeout(r, ms));
              // 1) Клик по бейджу
              const badge = document.querySelector('[data-badge="true"]');
              if (badge) { badge.click(); await sleep(300); }

              // 2) Находим опцию 2025
              const normalize = (s) => String(s || '').replace(/\s+/g, ' ').trim();
              const options = Array.from(document.querySelectorAll('[role="option"], [aria-selected]'));
              const opt2025 = options.find(el => /\b2025\b/.test(normalize(el.textContent)));
              if (!opt2025) return false;

              // 3) Ищем кликабельный элемент
              const checkbox = opt2025.querySelector('input[type="checkbox"]');
              const target = checkbox || opt2025.querySelector('label, [role="checkbox"], .checkbox-root, .list-cell-root, span[tabindex], div[tabindex]') || opt2025;

              // 4) Эмуляция клика
              const fire = (type, el) => el && el.dispatchEvent(new MouseEvent(type, { bubbles: true, cancelable: true, view: window }));
              await sleep(150); fire('pointerover', target);
              await sleep(150); fire('mouseover',  target);
              await sleep(180); fire('pointerdown', target);
              await sleep(150); fire('mousedown',   target);
              await sleep(220); fire('pointerup',   target);
              await sleep(180); fire('mouseup',     target);
              await sleep(220);
              return target.dispatchEvent(new MouseEvent('click', { bubbles: true, cancelable: true, view: window }));
            }
            """)
            if clicked_2025:
                await asyncio.sleep(1200/1000)
        except Exception:
            pass

        try:
            await page.waitForSelector('[data-testid="construction-progress-pagination"]', {"timeout": 4000})
        except Exception:
            pass
    except Exception:
        return {"construction_stages": []}

    eval_script = r"""
    () => {
      const toAbs = (u) => { try { return new URL(u, location.origin).href; } catch { return u || null; } };
      const isImg = (u) => /\.(png|jpe?g|webp)(?:$|\?|#)/i.test(String(u || ''));
      const pickFromSrcset = (srcset) => {
        if (!srcset) return null;
        const first = String(srcset).split(',')[0].trim().split(' ')[0];
        return first || null;
      };
      const headerLike = (txt) => {
        if (!txt) return false;
        const s = txt.replace(/\s+/g, ' ').trim();
        if (s.length < 5 || s.length > 160) return false;
        const hasMarkers = /(квартал|кв\.|литер|обновлен|обновлено|год|месяц)/i.test(s);
        const hasYear = /\b20\d{2}\b/.test(s);
        const hasMonthYear = /[А-ЯЁ][а-яё]+,?\s*\d{4}/.test(s);
        return hasMarkers || hasYear || hasMonthYear;
      };
      const collectImages = (root) => {
        const urls = new Set();
        root.querySelectorAll('img').forEach(img => {
          const s1 = img.getAttribute('src');
          const s2 = img.getAttribute('data-src') || img.getAttribute('data-lazy') || img.getAttribute('data-original');
          const s3 = pickFromSrcset(img.getAttribute('srcset'));
          [s1, s2, s3].filter(Boolean).map(toAbs).filter(isImg).forEach(u => urls.add(u));
        });
        root.querySelectorAll('source[srcset]').forEach(s => {
          const picked = pickFromSrcset(s.getAttribute('srcset'));
          if (picked && isImg(picked)) urls.add(toAbs(picked));
        });
        root.querySelectorAll('[style*=\"background\"]').forEach(el => {
          const st = String(el.getAttribute('style') || '');
          const m = st.match(/url\((['\"]?)(.*?)\1\)/i);
          if (m && isImg(m[2])) urls.add(toAbs(m[2]));
        });
        return [...urls];
      };

      const pagination = document.querySelector('[data-testid=\"construction-progress-pagination\"]');
      const container = pagination ? pagination.parentElement : null;
      let upperBlocks = [];
      if (container && pagination) {
        let el = pagination.previousElementSibling;
        while (el) { upperBlocks.push(el); el = el.previousElementSibling; }
        upperBlocks.reverse();
      }
      if (!upperBlocks.length) {
        const candidate = document.querySelector('[role=\"list\"] [role=\"listitem\"]')
          ? document.querySelector('[role=\"list\"]').parentElement
          : document.body;
        upperBlocks = [candidate];
      }

      const seen = new Set();
      const stages = [];

      const extractStageFromBlock = (block) => {
        const headerEl = Array.from(block.querySelectorAll('div,span,p,h1,h2,h3,h4'))
          .find(x => headerLike((x.innerText || '').replace(/\s+/g, ' ').trim()));
        const title = headerEl ? (headerEl.innerText || '').replace(/\s+/g, ' ').trim() : null;
        const photos = collectImages(block).slice(0, 5);
        if (!(title || photos.length)) return;
        const key = `${title || ''}::${photos[0] || ''}`;
        if (!seen.has(key)) {
          stages.push({ title: title || 'Этап', photos });
          seen.add(key);
        }
      };

      upperBlocks.forEach(extractStageFromBlock);
      const filtered = stages.filter(s => s.photos && s.photos.length);
      return filtered;
    }
    """
    # Сбор со всех страниц пагинации
    stages_merged: List[Dict[str, Any]] = []
    used_keys = set()

    def merge_pages(stages_page: List[Dict[str, Any]]):
        for s in stages_page or []:
            title = s.get('title') or s.get('date') or ''
            photos = list(s.get('photos') or [])[:5]  # ограничиваем первыми 5
            key = f"{title}::{photos[0] if photos else ''}"
            if key in used_keys:
                continue
            used_keys.add(key)
            stages_merged.append({
                'stage_number': len(stages_merged) + 1,
                'date': title,
                'photos': photos
            })

    try:
        # Определяем количество страниц
        pages_count = await page.evaluate("""
        () => {
          const pag = document.querySelector('[data-testid="construction-progress-pagination"]');
          if (!pag) return 1;
          const nums = Array.from(pag.querySelectorAll('button, a'))
            .map(el => parseInt((el.textContent || '').trim(), 10))
            .filter(n => Number.isFinite(n));
          return Math.max(1, ...(nums.length ? nums : [1]));
        }
        """)
        if not isinstance(pages_count, (int, float)) or pages_count < 1:
            pages_count = 1

        for page_index in range(1, int(pages_count) + 1):
            try:
                data = await page.evaluate(eval_script)
                
                if isinstance(data, list):
                    merge_pages(data)
                elif isinstance(data, dict):
                    stages_list = data.get('stages') or data.get('construction_stages') or []
                    merge_pages(stages_list)
            except Exception:
                pass

            # Кликаем следующую страницу, если есть
            if page_index < pages_count:
                try:
                    clicked = await page.evaluate("""
                    (n) => {
                      const pag = document.querySelector('[data-testid="construction-progress-pagination"]');
                      if (!pag) return false;
                      const btn = Array.from(pag.querySelectorAll('button, a'))
                        .find(el => (el.textContent || '').trim() === String(n));
                      if (btn) { btn.click(); return true; }
                      return false;
                    }
                    """, page_index + 1)
                    if clicked:
                        await asyncio.sleep(2)
                except Exception:
                    pass

        return {"construction_stages": stages_merged}
    except Exception:
        return {"construction_stages": []}


async def process_construction_stages_domclick(stages: List[Dict[str, Any]], complex_id: str) -> Dict[str, Any]:
    """Скачивает фото по этапам и загружает в S3, возвращает структуру construction_progress с URL."""
    if not stages:
        return {"construction_stages": []}
    try:
        s3 = S3Service()
    except Exception as s3_error:
        logger.error(f"Ошибка инициализации S3Service для хода строительства: {s3_error}")
        import traceback
        logger.error(f"Полный traceback:\n{traceback.format_exc()}")
        return {"construction_stages": []}
    result_stages = []
    async with aiohttp.ClientSession() as session:
        for s in stages:
            stage_num = s.get("stage_number") or (len(result_stages) + 1)
            urls = (s.get("photos") or [])[:5]  # скачиваем не более 5 фото на этап
            saved = []
            sem = asyncio.Semaphore(5)
            async def work(u, idx):
                async with sem:
                    try:
                        async with session.get(u, timeout=aiohttp.ClientTimeout(total=30)) as response:
                            if response.status != 200:
                                return None
                            raw = await response.read()
                    except Exception:
                        return None
                    input_bytes = BytesIO(raw)
                    try:
                        processed = image_processor.process(input_bytes)
                    except Exception:
                        return None
                    processed.seek(0)
                    data = processed.read()
                    key = f"complexes/{complex_id}/construction/stage_{stage_num}/photo_{idx + 1}.jpg"
                    try:
                        url_public = upload_with_watermark(s3, data, key)
                        return url_public
                    except Exception:
                        return None
            tasks = [work(u, i) for i, u in enumerate(urls)]
            results = await asyncio.gather(*tasks, return_exceptions=True)
            for p in results:
                if isinstance(p, str) and p:
                    saved.append(p)
            result_stages.append({
                "stage_number": stage_num,
                "date": s.get("date") or "",
                "photos": saved
            })
    return {"construction_stages": result_stages}


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


def extract_url_params(url: str) -> Dict[str, Any]:
    """
    Извлекает параметры из URL поиска Domclick для формирования API запроса.
    """
    try:
        parsed = urlparse(url)
        params = parse_qs(parsed.query, keep_blank_values=True)
        # Преобразуем списки в строки (берем первое значение)
        result = {}
        for key, value_list in params.items():
            if value_list:
                result[key] = value_list[0] if len(value_list) == 1 else value_list
        return result
    except Exception as e:
        logger.error(f"Ошибка извлечения параметров из URL {url}: {e}")
        return {}


async def fetch_offers_api(page, api_params: Dict[str, Any], offset: int, max_retries: int = 3) -> Dict[str, Any]:
    """
    Выполняет fetch запрос к API Domclick через page.evaluate().
    Возвращает ответ API или None при ошибке.
    Повторяет запрос до max_retries раз при ошибках.
    """
    # Формируем параметры для API запроса
    api_params_copy = api_params.copy()
    api_params_copy['offset'] = str(offset)
    api_params_copy['limit'] = api_params_copy.get('limit', '20')
    api_params_copy.setdefault('sort', 'price')
    api_params_copy.setdefault('sort_dir', 'desc')
    api_params_copy.setdefault('deal_type', 'sale')
    api_params_copy.setdefault('category', 'living')
    api_params_copy.setdefault('offer_type', 'layout')
    api_params_copy.setdefault('from_developer', '1')
    api_params_copy.setdefault('disable_payment', 'true')
    api_params_copy.setdefault('enable_mixed_ranking', '1')
    
    # Формируем query string с помощью urlencode
    query_string = urlencode(api_params_copy, doseq=True)
    api_url = f"https://bff-search-web.domclick.ru/api/offers/v1?{query_string}"

    script = """
    async (url) => {
      try {
        const response = await fetch(url, {
          headers: {
            'accept': 'application/json, text/plain, */*',
            'accept-language': 'ru,en;q=0.9',
            'sec-ch-ua': '"Not A(Brand";v="8", "Chromium";v="132", "YaBrowser";v="25.2", "Yowser";v="2.5"',
            'sec-ch-ua-mobile': '?0',
            'sec-ch-ua-platform': '"Linux"',
            'sec-fetch-dest': 'empty',
            'sec-fetch-mode': 'cors',
            'sec-fetch-site': 'same-site'
          },
          referrer: 'https://ufa.domclick.ru/',
          referrerPolicy: 'strict-origin-when-cross-origin',
          method: 'GET',
          mode: 'cors',
          credentials: 'include'
        });
        
        if (!response.ok) {
          return { error: 'HTTP ' + response.status + ': ' + response.statusText };
        }
        
        const data = await response.json();
        return data;
      } catch (error) {
        return { error: error.toString() };
      }
    }
    """
    
    for attempt in range(1, max_retries + 1):
        try:
            result = await page.evaluate(script, api_url)
            if isinstance(result, dict):
                if 'error' in result:
                    if attempt < max_retries:
                        await asyncio.sleep(2 * attempt)
                        continue
                    return None
                
                # Проверяем, есть ли обертка 'result'
                if 'result' in result and isinstance(result['result'], dict):
                    actual_data = result['result']
                    if 'total' in result:
                        actual_data['total'] = result['total']
                    return actual_data
                
                return result
            else:
                if attempt < max_retries:
                    await asyncio.sleep(2 * attempt)
                    continue
                return None
        except Exception as e:
            if attempt < max_retries:
                await asyncio.sleep(2 * attempt)
                continue
            return None
    
    return None


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


async def process_complex_photos(photo_urls: List[str], complex_id: str) -> List[str]:
    """
    Обрабатывает список URL фотографий ЖК и загружает их в S3.
    Возвращает список публичных URL.
    """
    if not photo_urls:
        return []

    processed_photos = []
    try:
        s3 = S3Service()
    except Exception as s3_error:
        logger.error(f"Ошибка инициализации S3Service для фотографий ЖК: {s3_error}")
        import traceback
        logger.error(f"Полный traceback:\n{traceback.format_exc()}")
        return []

    async with aiohttp.ClientSession() as session:
        # Обрабатываем до 5 фотографий параллельно
        semaphore = asyncio.Semaphore(5)

        async def process_single_photo(url, index):
            async with semaphore:
                # Скачиваем исходник
                try:
                    async with session.get(url, timeout=aiohttp.ClientTimeout(total=30)) as response:
                        if response.status != 200:
                            return None
                        raw = await response.read()
                except Exception:
                    return None

                # Обрабатываем через resize
                input_bytes = BytesIO(raw)
                try:
                    processed = image_processor.process(input_bytes)
                except Exception:
                    return None
                processed.seek(0)
                data = processed.read()

                # Загружаем в S3
                key = f"complexes/{complex_id}/complex_photos/photo_{index + 1}.jpg"
                try:
                    url_public = upload_with_watermark(s3, data, key)
                    return url_public
                except Exception:
                    return None

        tasks = [process_single_photo(url, i) for i, url in enumerate(photo_urls[:8])]  # максимум 8 фото ЖК
        results = await asyncio.gather(*tasks, return_exceptions=True)

        for result in results:
            if isinstance(result, str) and result:
                processed_photos.append(result)

    return processed_photos


async def process_apartment_photos(apartment_data: Dict[str, Any], complex_id: str, apartment_path: str) -> Dict[str, Any]:
    """
    Обрабатывает фотографии для одной квартиры и загружает в S3.
    Возвращает данные с URL к файлам.
    """
    image_urls = apartment_data.get("images")
    if not image_urls:
        image_urls = apartment_data.get("photos")
    
    if not image_urls:
        return {
            "offer": apartment_data.get("offer"),
            "photos": [],
            "area": apartment_data.get("area", ""),
            "totalArea": apartment_data.get("totalArea"),
            "price": apartment_data.get("price", ""),
            "pricePerSquare": apartment_data.get("pricePerSquare", ""),
            "completionDate": apartment_data.get("completionDate", ""),
            "url": apartment_data.get("url", "")
        }

    processed_images = []
    try:
        s3 = S3Service()
    except Exception as s3_error:
        logger.error(f"Ошибка инициализации S3Service для фотографий квартир: {s3_error}")
        import traceback
        logger.error(f"Полный traceback:\n{traceback.format_exc()}")
        return {
            "offer": apartment_data.get("offer"),
            "photos": [],
            "area": apartment_data.get("area", ""),
            "totalArea": apartment_data.get("totalArea"),
            "price": apartment_data.get("price", ""),
            "pricePerSquare": apartment_data.get("pricePerSquare", ""),
            "completionDate": apartment_data.get("completionDate", ""),
            "url": apartment_data.get("url", "")
        }

    async with aiohttp.ClientSession() as session:
        # Обрабатываем до 3 фотографий параллельно для квартир
        semaphore = asyncio.Semaphore(3)

        async def process_single_photo(url, index):
            async with semaphore:
                try:
                    async with session.get(url, timeout=aiohttp.ClientTimeout(total=30)) as response:
                        if response.status != 200:
                            return None
                        raw = await response.read()
                except Exception:
                    return None

                input_bytes = BytesIO(raw)
                try:
                    processed = image_processor.process(input_bytes)
                except Exception:
                    return None
                processed.seek(0)
                data = processed.read()

                key = f"complexes/{complex_id}/apartments/{apartment_path}/photo_{index + 1}.jpg"
                try:
                    url_public = upload_with_watermark(s3, data, key)
                    return url_public
                except Exception:
                    return None

        tasks = [process_single_photo(url, i) for i, url in enumerate(image_urls[:3])]  # максимум 3 фото на квартиру
        results = await asyncio.gather(*tasks, return_exceptions=True)

        for result in results:
            if isinstance(result, str) and result:
                processed_images.append(result)

    # Возвращаем данные квартиры с URL к файлам
    result = {
        "offer": apartment_data.get("offer"),
        "photos": processed_images,
        "area": apartment_data.get("area", ""),
        "totalArea": apartment_data.get("totalArea"),
        "price": apartment_data.get("price", ""),
        "pricePerSquare": apartment_data.get("pricePerSquare", ""),
        "completionDate": apartment_data.get("completionDate", ""),
        "url": apartment_data.get("url", "")
    }
    return result


async def process_all_apartment_types(apartment_types: Dict[str, Any], complex_id: str) -> Dict[str, Any]:
    """
    Обрабатывает все фотографии во всех типах квартир и загружает в S3.
    """
    if not apartment_types:
        return apartment_types

    processed_types = {}

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
        apartment_type_normalized = apartment_type.replace('-', '_').replace('комн', 'komn')

        for i, apartment in enumerate(apartments):
            if isinstance(apartment, dict):
                apartment_path = f"{apartment_type_normalized}/apartment_{i + 1}"
                processed_apartment = await process_apartment_photos(apartment, complex_id, apartment_path)
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


def normalize_room_from_api(rooms: int) -> str:
    """
    Преобразует количество комнат из API в строку для группировки.
    """
    if rooms == 0:
        return 'Студия'
    return f'{rooms}-комн'


def process_api_response(api_data: Dict[str, Any]) -> Dict[str, Any]:
    """
    Обрабатывает ответ API и преобразует в нужный формат.
    Возвращает словарь с offers (группированные по комнатам), address, complexName, complexHref.
    """
    if not api_data or 'items' not in api_data:
        return {
            'offers': {},
            'address': None,
            'complexName': None,
            'complexHref': None
        }
    
    items = api_data.get('items', [])
    
    if not items:
        return {
            'offers': {},
            'address': None,
            'complexName': None,
            'complexHref': None
        }
    
    # Извлекаем данные из первого элемента
    first_item = items[0]
    
    address = first_item.get('address', {}).get('displayName')
    location_data = first_item.get('location', {}) or {}
    latitude = location_data.get('lat')
    longitude = location_data.get('lon')
    
    complex_data = first_item.get('complex', {})
    complex_name = complex_data.get('name')
    complex_slug = complex_data.get('slug')
    complex_id = complex_data.get('id')
    
    # Формируем ссылку на комплекс
    complex_href = None
    if complex_slug:
        complex_href = f"https://ufa.domclick.ru/complexes/{complex_slug}"
    elif complex_id:
        complex_href = f"https://ufa.domclick.ru/complexes/{complex_id}"
    
    # Группируем квартиры по количеству комнат
    offers = {}
    skipped_count = 0
    total_items = len(items)
    
    for idx, item in enumerate(items):
        if not isinstance(item, dict):
            skipped_count += 1
            continue
            
        general_info = item.get('generalInfo', {})
        if not general_info:
            skipped_count += 1
            continue
            
        rooms = general_info.get('rooms', 0)
        room_key = normalize_room_from_api(rooms)
        
        # Формируем название квартиры
        area = general_info.get('area')
        min_floor = general_info.get('minFloor')
        max_floor = general_info.get('maxFloor')
        
        title_parts = []
        if rooms == 0:
            title_parts.append('Студия')
        else:
            title_parts.append(f'{rooms}-комн')
        if area:
            title_parts.append(f'{area} м²')
        if min_floor is not None and max_floor is not None:
            if min_floor == max_floor:
                title_parts.append(f'{min_floor} этаж')
            else:
                title_parts.append(f'{min_floor}-{max_floor} этаж')
        
        title = ', '.join(title_parts) if title_parts else 'Квартира'
        
        # Извлекаем фотографии
        photos = item.get('photos', [])
        image_urls = []
        
        for photo_idx, photo in enumerate(photos):
            if not isinstance(photo, dict):
                continue
            photo_url = photo.get('url', '')
            if photo_url:
                # Формируем полный URL: https://img.dmclk.ru/ + путь
                if photo_url.startswith('/'):
                    full_url = f"https://img.dmclk.ru{photo_url}"
                elif photo_url.startswith('http'):
                    full_url = photo_url
                else:
                    full_url = f"https://img.dmclk.ru/{photo_url}"
                image_urls.append(full_url)
        
        # Извлекаем дополнительные поля из API
        # Цена
        price_info = item.get('price', {})
        if isinstance(price_info, dict):
            price = price_info.get('value') or price_info.get('text') or price_info.get('formatted')
        elif price_info:
            price = price_info
        else:
            price = None
        price_str = str(price) if price else ''
        
        # Цена за м² - вычисляем из price / area
        price_per_square = None
        if price and area:
            try:
                price_num = float(price) if isinstance(price, (int, float, str)) else None
                area_num = float(area) if isinstance(area, (int, float, str)) else None
                if price_num and area_num and area_num > 0:
                    price_per_square = round(price_num / area_num, 2)
            except (ValueError, TypeError):
                pass
        
        # Если не удалось вычислить, пробуем найти в API
        if not price_per_square:
            price_per_square_info = item.get('pricePerSquare', {})
            if isinstance(price_per_square_info, dict):
                price_per_square = price_per_square_info.get('value') or price_per_square_info.get('text') or price_per_square_info.get('formatted')
            elif price_per_square_info:
                price_per_square = price_per_square_info
        
        price_per_square_str = str(price_per_square) if price_per_square else ''
        
        # Дата сдачи - формируем из complex.building.endBuildQuarter и endBuildYear
        completion_date_str = ''
        complex_data = item.get('complex', {})
        building_data = complex_data.get('building', {}) if isinstance(complex_data, dict) else {}
        
        if building_data:
            end_build_quarter = building_data.get('endBuildQuarter')
            end_build_year = building_data.get('endBuildYear')
            
            if end_build_quarter and end_build_year:
                completion_date_str = f"{end_build_quarter} квартал {end_build_year}"
            elif end_build_year:
                completion_date_str = str(end_build_year)
        
        # Если не удалось сформировать из building, пробуем найти в API
        if not completion_date_str:
            completion_date = item.get('completionDate', '')
            if isinstance(completion_date, dict):
                completion_date = completion_date.get('value', '') or completion_date.get('text', '') or completion_date.get('formatted', '')
            completion_date_str = str(completion_date) if completion_date else ''
        
        # URL объявления - используем path из API
        apartment_url = item.get('path', '') or item.get('url', '') or item.get('urlPath', '') or item.get('href', '')
        if apartment_url and not apartment_url.startswith('http'):
            apartment_url = f"https://ufa.domclick.ru{apartment_url}" if apartment_url.startswith('/') else f"https://ufa.domclick.ru/{apartment_url}"
        apartment_url_str = apartment_url if apartment_url else ''
        
        # Площадь как строка и число
        area_str = str(area) if area else ''
        total_area = float(area) if area else None
        
        card = {
            'offer': title,
            'photos': image_urls,  # Используем 'photos' для совместимости с MongoDB схемой
            'area': area_str,  # Площадь как строка
            'totalArea': total_area,  # Площадь как число
            'price': price_str,  # Цена
            'pricePerSquare': price_per_square_str,  # Цена за м²
            'completionDate': completion_date_str,  # Дата сдачи
            'url': apartment_url_str  # URL объявления
        }
        
        if room_key not in offers:
            offers[room_key] = []
        offers[room_key].append(card)
    
    processed_count = sum(len(cards) for cards in offers.values())
    if skipped_count > 0:
        logger.warning(f"  Пропущено {skipped_count} из {total_items} элементов")
    
    return {
        'offers': offers,
        'address': address,
        'complexName': complex_name,
        'complexHref': complex_href,
        'latitude': latitude,
        'longitude': longitude,
    }


def log_apartment_photo_parsing(offers: Dict[str, List[Dict[str, Any]]], *, base_url: str, offset: int) -> None:
    """
    Логирует краткую информацию о собранных квартирах (только важные данные).
    """
    if not offers:
        return
    
    total_apartments = sum(len(cards) if isinstance(cards, list) else 0 for cards in offers.values())
    total_photos = 0
    for cards in offers.values():
        if isinstance(cards, list):
            for card in cards:
                if isinstance(card, dict):
                    images = card.get("photos") or card.get("images") or []
                    total_photos += len(images)
    
    logger.info(f"  Собрано квартир: {total_apartments}, групп: {len(offers)}, фото: {total_photos}")


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

    # Создаем браузер с повторными попытками в случае ошибки прокси
    browser = None
    page = None
    max_init_attempts = 5

    for init_attempt in range(max_init_attempts):
        try:
            browser, proxy_url = await create_browser(headless=False)
            print(f"Попытка {init_attempt + 1}/{max_init_attempts}: Создан браузер с прокси {proxy_url}")
            page = await create_browser_page(browser)
            print("✓ Браузер и страница успешно инициализированы")
            break
        except Exception as init_error:
            print(f"✗ Ошибка инициализации браузера (попытка {init_attempt + 1}/{max_init_attempts}): {init_error}")
            if browser:
                try:
                    await browser.close()
                except:
                    pass
            if init_attempt < max_init_attempts - 1:
                await asyncio.sleep(2)
            else:
                print("Не удалось создать браузер после всех попыток. Завершение работы.")
                return

    try:
        while url_index < len(urls):
            try:
                base_url = urls[url_index]
                print(f"→ URL [{url_index + 1}/{len(urls)}]: {base_url}")

                if offset % 20 != 0:
                    offset = (offset // 20) * 20

                # Извлекаем параметры из URL
                api_params = extract_url_params(base_url)
                if not api_params:
                    print(f"Не удалось извлечь параметры из URL: {base_url}. Пропускаю.")
                    url_index += 1
                    offset = 0
                    save_progress(url_index, offset, str(PROGRESS_FILE))
                    continue

                # Открываем страницу из файла для установки cookies и контекста браузера
                try:
                    await page.goto(base_url, timeout=120000, waitUntil='networkidle0')
                    await page.waitForFunction(
                        "() => document.readyState === 'complete'",
                        {"timeout": 30000}
                    )
                    await asyncio.sleep(3)
                except Exception:
                    pass  # Пробуем продолжить без открытия страницы

                # Делаем первый запрос для определения общего количества результатов
                attempts = 0
                browser_restart_count = 0
                max_browser_restarts = 2  # Максимум 2 перезапуска браузера на URL
                first_api_response = None

                while attempts < 3 and browser_restart_count < max_browser_restarts:
                    try:
                        # Проверяем, что браузер и страница еще живы
                        page_closed = False
                        try:
                            if page and not page.isClosed():
                                ready_state = await page.evaluate("() => document.readyState")
                                if ready_state != 'complete':
                                    await page.waitForFunction(
                                        "() => document.readyState === 'complete'",
                                        {"timeout": 30000}
                                    )
                                    await asyncio.sleep(2)
                            else:
                                page_closed = True
                        except Exception as check_error:
                            error_str = str(check_error).lower()
                            if 'session closed' in error_str or 'target closed' in error_str or 'page closed' in error_str:
                                page_closed = True
                            else:
                                try:
                                    await page.goto(base_url, timeout=120000, waitUntil='networkidle0')
                                    await page.waitForFunction(
                                        "() => document.readyState === 'complete'",
                                        {"timeout": 30000}
                                    )
                                    await asyncio.sleep(3)
                                except Exception:
                                    page_closed = True

                        # Если страница закрыта, перезапускаем браузер
                        if page_closed:
                            if browser_restart_count >= max_browser_restarts:
                                print(f"  ✗ Достигнут лимит перезапусков браузера, пропускаю URL")
                                try:
                                    if browser:
                                        await browser.close()
                                except Exception:
                                    pass
                                first_api_response = None
                                break

                            browser_restart_count += 1
                            try:
                                browser, page, _ = await restart_browser(browser, headless=False)
                                await page.goto(base_url, timeout=120000, waitUntil='networkidle0')
                                await page.waitForFunction(
                                    "() => document.readyState === 'complete'",
                                    {"timeout": 30000}
                                )
                                await asyncio.sleep(3)
                                attempts = 0
                            except Exception as restart_error:
                                if browser_restart_count >= max_browser_restarts:
                                    print(f"  ✗ Достигнут лимит перезапусков браузера, пропускаю URL")
                                    try:
                                        if browser:
                                            await browser.close()
                                    except Exception:
                                        pass
                                    first_api_response = None
                                    break
                                await asyncio.sleep(5)
                                continue

                        first_api_response = await fetch_offers_api(page, api_params, 0, max_retries=3)
                        if first_api_response and 'items' in first_api_response:
                            break
                        attempts += 1
                        if attempts < 3:
                            await asyncio.sleep(2)
                    except Exception as e:
                        error_str = str(e).lower()
                        attempts += 1

                        # Если ошибка связана с закрытой сессией, перезапускаем браузер
                        if 'session closed' in error_str or 'target closed' in error_str or 'page closed' in error_str:
                            if browser_restart_count >= max_browser_restarts:
                                print(f"  ✗ Достигнут лимит перезапусков браузера, пропускаю URL")
                                try:
                                    if browser:
                                        await browser.close()
                                except Exception:
                                    pass
                                first_api_response = None
                                break

                            browser_restart_count += 1
                            try:
                                browser, page, _ = await restart_browser(browser, headless=False)
                                await page.goto(base_url, timeout=120000, waitUntil='networkidle0')
                                await page.waitForFunction(
                                    "() => document.readyState === 'complete'",
                                    {"timeout": 30000}
                                )
                                await asyncio.sleep(3)
                                attempts = 0
                            except Exception:
                                if browser_restart_count >= max_browser_restarts:
                                    print(f"  ✗ Достигнут лимит перезапусков браузера, пропускаю URL")
                                    try:
                                        if browser:
                                            await browser.close()
                                    except Exception:
                                        pass
                                    first_api_response = None
                                    break
                                await asyncio.sleep(5)
                                continue
                        elif attempts >= 3:
                            if browser_restart_count < max_browser_restarts:
                                browser_restart_count += 1
                                try:
                                    browser, page, _ = await restart_browser(browser, headless=False)
                                    attempts = 0
                                except Exception:
                                    try:
                                        if browser:
                                            await browser.close()
                                    except Exception:
                                        pass
                                    first_api_response = None
                                    break
                            else:
                                try:
                                    if browser:
                                        await browser.close()
                                except Exception:
                                    pass
                                first_api_response = None
                                break
                        else:
                            await asyncio.sleep(2)

                if not first_api_response:
                    print(f"  ✗ Не удалось получить данные из API, пропускаю URL")
                    url_index += 1
                    offset = 0
                    save_progress(url_index, offset, str(PROGRESS_FILE))
                    continue

                # Определяем общее количество результатов и страниц
                total = first_api_response.get('total', 0)
                items_count = len(first_api_response.get('items', []))
                limit = int(api_params.get('limit', 20))

                # Если total=0, но есть items, используем количество items как индикатор
                if total == 0 and items_count > 0:
                    total = items_count + 1  # Чтобы цикл выполнился хотя бы один раз

                total_pages = max(1, (total + limit - 1) // limit) if total > 0 else 1
                print(f"  Всего результатов: {total}, страниц: {total_pages}")

                # Обрабатываем первый ответ
                first_data = process_api_response(first_api_response)
                aggregated_address = first_data.get('address')
                aggregated_complex_name = first_data.get('complexName')
                aggregated_complex_href = first_data.get('complexHref')
                aggregated_latitude = first_data.get('latitude')
                aggregated_longitude = first_data.get('longitude')
                aggregated_offers = first_data.get('offers', {})

                # Обрабатываем остальные страницы
                current_offset = limit
                # Если total был установлен искусственно (из-за total=0), используем другой подход
                if total == items_count + 1:
                    # Запрашиваем пока есть данные
                    while True:
                        api_response = await fetch_offers_api(page, api_params, current_offset, max_retries=3)

                        if api_response and 'items' in api_response:
                            response_items = api_response.get('items', [])
                            if not response_items:
                                break

                            data = process_api_response(api_response)
                            offers = data.get('offers', {})

                            # Объединяем группы офферов
                            for group, cards in offers.items():
                                if group not in aggregated_offers:
                                    aggregated_offers[group] = []
                                aggregated_offers[group].extend(cards)

                            offset = current_offset + limit
                            save_progress(url_index, offset, str(PROGRESS_FILE))

                            # Если получили меньше limit элементов, значит это последняя страница
                            if len(response_items) < limit:
                                break
                        else:
                            break

                        await asyncio.sleep(3)
                        current_offset += limit
                else:
                    # Обычный случай: total известен
                    while current_offset < total:
                        api_response = await fetch_offers_api(page, api_params, current_offset, max_retries=3)

                        if api_response and 'items' in api_response:
                            data = process_api_response(api_response)
                            offers = data.get('offers', {})

                            # Объединяем группы офферов
                            for group, cards in offers.items():
                                if group not in aggregated_offers:
                                    aggregated_offers[group] = []
                                aggregated_offers[group].extend(cards)

                            offset = current_offset + limit
                            save_progress(url_index, offset, str(PROGRESS_FILE))

                        if current_offset + limit < total:
                            await asyncio.sleep(3)

                        current_offset += limit

                # Для получения фотографий ЖК и ссылки на ход строительства нужно открыть страницу комплекса
                complex_gallery_images: List[str] = []
                aggregated_hod_url: str = None
                construction_progress_data: Dict[str, Any] = None

                if aggregated_complex_href:
                    try:
                        await page.goto(aggregated_complex_href, timeout=120000)
                        await asyncio.sleep(3)

                        # Извлекаем фотографии ЖК из галереи
                        try:
                            complex_photos_data = await page.evaluate("""
                            () => {
                              const complexPhotos = [];

                              // Пробуем разные селекторы для галереи
                              let galleryContainer = document.querySelector('[data-e2e-id="complex-header-gallery"]');
                              if (!galleryContainer) {
                                galleryContainer = document.querySelector('[data-e2e-id*="gallery"]');
                              }
                              if (!galleryContainer) {
                                galleryContainer = document.querySelector('.gallery, [class*="gallery"], [class*="Gallery"]');
                              }

                              if (galleryContainer) {
                                // Пробуем разные селекторы для изображений
                                let imageElements = galleryContainer.querySelectorAll('[data-e2e-id^="complex-header-gallery-image__"]');
                                if (imageElements.length === 0) {
                                  imageElements = galleryContainer.querySelectorAll('img');
                                }

                                imageElements.forEach((element, idx) => {
                                  // Пробуем разные способы получения изображения
                                  let img = element;
                                  if (element.tagName !== 'IMG') {
                                    img = element.querySelector('img');
                                  }

                                  if (!img) {
                                    // Пробуем найти img внутри элемента
                                    img = element.querySelector('img.picture-image-object-fit--cover-820-5-0-5.picture-imageFillingContainer-4a2-5-0-5');
                                  }
                                  if (!img) {
                                    // Пробуем любой img
                                    img = element.querySelector('img');
                                  }

                                  if (img) {
                                    // Пробуем разные атрибуты для получения URL
                                    let imgUrl = img.src || img.getAttribute('src') || img.getAttribute('data-src') ||
                                               img.getAttribute('data-lazy') || img.getAttribute('data-original');

                                    if (imgUrl) {
                                      try {
                                        const absoluteUrl = new URL(imgUrl, location.origin).href;
                                        // Фильтруем только реальные изображения
                                        if (/\.(jpg|jpeg|png|webp)/i.test(absoluteUrl) || absoluteUrl.includes('img.dmclk.ru') || absoluteUrl.includes('vitrina')) {
                                          complexPhotos.push(absoluteUrl);
                                        }
                                      } catch (e) {
                                        if (imgUrl.startsWith('http')) {
                                          complexPhotos.push(imgUrl);
                                        }
                                      }
                                    }
                                  }
                                });
                              }

                              return complexPhotos;
                            }
                            """)
                            complex_gallery_images = complex_photos_data or []
                        except Exception:
                            pass

                        # Сохраняем ссылку на страницу "О ЖК" для хода строительства
                        try:
                            about_href = await page.evaluate("""
                            () => {
                              let a = document.querySelector('[data-e2e-id="complex-header-about"]');

                              if (!a) {
                                const links = Array.from(document.querySelectorAll('a'));
                                a = links.find(link => {
                                  const text = (link.textContent || '').toLowerCase().trim();
                                  return text.includes('о жк') || text.includes('о комплексе') || text.includes('подробнее');
                                });
                              }
                              if (!a) {
                                const links = Array.from(document.querySelectorAll('a[href*="about"], a[href*="o-zhk"]'));
                                if (links.length > 0) {
                                  a = links[0];
                                }
                              }
                              if (!a) {
                                const currentPath = location.pathname;
                                const basePath = currentPath.split('/').slice(0, -1).join('/');
                                const links = Array.from(document.querySelectorAll(`a[href*="${basePath}/about"], a[href*="${basePath}/o-zhk"]`));
                                if (links.length > 0) {
                                  a = links[0];
                                }
                              }
                              if (a) {
                                const href = a.getAttribute('href') || a.href || null;
                                if (href) {
                                  try {
                                    return new URL(href, location.origin).href;
                                  } catch {
                                    return href.startsWith('http') ? href : location.origin + (href.startsWith('/') ? href : '/' + href);
                                  }
                                }
                              }
                              return null;
                            }
                            """)
                            if about_href:
                                if '/hod-stroitelstva' in about_href:
                                    aggregated_hod_url = about_href
                                elif about_href.endswith('/'):
                                    aggregated_hod_url = about_href + 'hod-stroitelstva'
                                else:
                                    aggregated_hod_url = about_href + '/hod-stroitelstva'
                            else:
                                if aggregated_complex_href:
                                    if '/hod-stroitelstva' in aggregated_complex_href:
                                        aggregated_hod_url = aggregated_complex_href
                                    elif aggregated_complex_href.endswith('/'):
                                        aggregated_hod_url = aggregated_complex_href + 'hod-stroitelstva'
                                    else:
                                        aggregated_hod_url = aggregated_complex_href + '/hod-stroitelstva'
                        except Exception:
                            if aggregated_complex_href:
                                if '/hod-stroitelstva' in aggregated_complex_href:
                                    aggregated_hod_url = aggregated_complex_href
                                elif aggregated_complex_href.endswith('/'):
                                    aggregated_hod_url = aggregated_complex_href + 'hod-stroitelstva'
                                else:
                                    aggregated_hod_url = aggregated_complex_href + '/hod-stroitelstva'
                    except Exception:
                        pass

                # Получаем ID комплекса для формирования ключей S3
                complex_id = get_complex_id_from_url(aggregated_complex_href or base_url)

                # Обрабатываем фотографии ЖК и загружаем в S3
                complex_photos_urls = []
                if complex_gallery_images:
                    try:
                        complex_photos_urls = await process_complex_photos(complex_gallery_images, complex_id)
                    except Exception as e:
                        logger.error(f"Ошибка при обработке фотографий ЖК: {e}")
                        complex_photos_urls = []

                # Обрабатываем фотографии всех квартир и загружаем в S3
                processed_apartment_types = aggregated_offers or {}
                if aggregated_offers:
                    try:
                        processed_apartment_types = await process_all_apartment_types(aggregated_offers, complex_id)
                    except Exception as e:
                        logger.error(f"Ошибка при обработке фотографий квартир: {e}")
                        import traceback
                        logger.error(f"Полный traceback:\n{traceback.format_exc()}")
                        processed_apartment_types = aggregated_offers

                # После сбора всех офферов: если есть hod_url — переходим и собираем ход строительства.
                # При ошибках (прокси/соединение) — перезапускаем браузер и пробуем ещё раз.
                if aggregated_hod_url:
                    complex_id = get_complex_id_from_url(aggregated_complex_href or base_url)
                    max_attempts_hod = 3
                    attempt_hod = 0
                    while attempt_hod < max_attempts_hod and not construction_progress_data:
                        attempt_hod += 1
                        try:
                            stages_data = await extract_construction_from_domclick(page, aggregated_hod_url)
                            if stages_data and stages_data.get('construction_stages'):
                                construction_progress_data = await process_construction_stages_domclick(stages_data['construction_stages'], complex_id)
                                break
                            else:
                                if attempt_hod < max_attempts_hod:
                                    try:
                                        browser, page, _ = await restart_browser(browser, headless=False)
                                    except Exception:
                                        try:
                                            if browser:
                                                await browser.close()
                                        except Exception:
                                            pass
                                        browser = None
                                        page = None
                        except Exception:
                            if attempt_hod < max_attempts_hod:
                                try:
                                    browser, page, _ = await restart_browser(browser, headless=False)
                                except Exception:
                                    try:
                                        if browser:
                                            await browser.close()
                                    except Exception:
                                        pass
                                    browser = None
                                    page = None

                # формируем запись под Mongo-схему после обработки фото
                apartment_types_data = processed_apartment_types or aggregated_offers or {}

                apartment_types: Dict[str, Any] = {}
                for group, cards in apartment_types_data.items():
                    if isinstance(cards, list):
                        apartment_types[group] = {
                            "apartments": [
                                {
                                    "title": c.get("offer"),
                                    "photos": c.get("photos") or [],
                                    "area": c.get("area", ""),
                                    "totalArea": c.get("totalArea"),
                                    "price": c.get("price", ""),
                                    "pricePerSquare": c.get("pricePerSquare", ""),
                                    "completionDate": c.get("completionDate", ""),
                                    "url": c.get("url", "")
                                }
                                for c in cards
                            ]
                        }
                    elif isinstance(cards, dict) and "apartments" in cards:
                        apartment_list = cards["apartments"]
                        apartment_types[group] = {
                            "apartments": [
                                {
                                    "title": c.get("offer"),
                                    "photos": c.get("photos") or [],
                                    "area": c.get("area", ""),
                                    "totalArea": c.get("totalArea"),
                                    "price": c.get("price", ""),
                                    "pricePerSquare": c.get("pricePerSquare", ""),
                                    "completionDate": c.get("completionDate", ""),
                                    "url": c.get("url", "")
                                }
                                for c in apartment_list
                            ]
                        }
                    else:
                        apartment_types[group] = cards

                complex_url = normalize_complex_url(aggregated_complex_href) if aggregated_complex_href else None
                if not complex_url:
                    complex_url = base_url

                db_item = {
                    "latitude": aggregated_latitude,
                    "longitude": aggregated_longitude,
                    "url": complex_url,
                    "development": {
                        "complex_name": aggregated_complex_name,
                        "address": aggregated_address,
                        "source_url": base_url,
                        "photos": complex_photos_urls or [],
                    },
                    "apartment_types": apartment_types,
                }

                if construction_progress_data:
                    db_item.setdefault('development', {})['construction_progress'] = construction_progress_data

                try:
                    save_to_mongodb([db_item])
                except Exception as e:
                    print(f"Ошибка записи в MongoDB: {e}. Сохраню в {str(OUTPUT_FILE)} для отладки.")
                    results.append({
                        "sourceUrl": base_url,
                        "data": {
                            "address": aggregated_address,
                            "complexName": aggregated_complex_name,
                            "complexHref": aggregated_complex_href,
                            "offers": processed_apartment_types,
                            "complexPhotosUrls": complex_photos_urls
                        }
                    })
                    with open(str(OUTPUT_FILE), 'w', encoding='utf-8') as f:
                        json.dump(results, f, ensure_ascii=False, indent=2)

                url_index += 1
                offset = 0
                save_progress(url_index, offset, str(PROGRESS_FILE))

            except Exception as url_error:
                print(f"  ✗ Критическая ошибка при обработке URL: {url_error}")
                # Закрываем браузер при критической ошибке
                try:
                    if browser:
                        await browser.close()
                except Exception:
                    pass
                # Пытаемся создать новый браузер для следующего URL
                try:
                    browser, proxy_url = await create_browser(headless=False)
                    page = await create_browser_page(browser)
                except Exception:
                    print("  ✗ Не удалось создать новый браузер, завершаю работу")
                    break
                url_index += 1
                offset = 0
                save_progress(url_index, offset, str(PROGRESS_FILE))

    finally:
        try:
            await browser.close()
        except Exception:
            pass  # Игнорируем ошибки закрытия браузера

if __name__ == "__main__":
    asyncio.run(run())
