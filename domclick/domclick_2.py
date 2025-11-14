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
from typing import List, Dict, Any, Tuple
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
    print(f"    🔍 Начинаю извлечение хода строительства с URL: {hod_url}")
    script = """
    async (targetUrl) => {
      try {
        // Навигация на страницу "Ход строительства"
        if (location.href !== targetUrl) {
          history.scrollRestoration = 'manual';
        }
      } catch (e) {}
      return null;
    }
    """
    try:
        # Переходим на страницу хода строительства
        print(f"    📍 Переход на страницу: {hod_url}")
        await page.goto(hod_url, timeout=120000, waitUntil='networkidle0')
        await asyncio.sleep(3)
        
        # Проверяем, что страница загрузилась
        page_title = await page.evaluate("() => document.title")
        page_url = await page.evaluate("() => location.href")
        print(f"    📄 Заголовок страницы: {page_title}")
        print(f"    🔗 Текущий URL: {page_url}")
        
        # Проверяем наличие элементов на странице
        page_info = await page.evaluate("""
        () => {
          const pagination = document.querySelector('[data-testid="construction-progress-pagination"]');
          const images = document.querySelectorAll('img');
          const stages = document.querySelectorAll('[role="listitem"], .stage, [class*="stage"]');
          return {
            hasPagination: !!pagination,
            imagesCount: images.length,
            stagesCount: stages.length,
            bodyText: document.body ? document.body.innerText.substring(0, 200) : ''
          };
        }
        """)
        print(f"    🔍 Информация о странице: пагинация={page_info.get('hasPagination')}, изображений={page_info.get('imagesCount')}, этапов={page_info.get('stagesCount')}")
        print(f"    📝 Начало текста страницы: {page_info.get('bodyText', '')[:100]}...")

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

        print(f"    📊 Количество страниц пагинации: {pages_count}")
        
        for page_index in range(1, int(pages_count) + 1):
            try:
                print(f"    📄 Обработка страницы {page_index}/{pages_count}...")
                data = await page.evaluate(eval_script)
                print(f"    📦 Данные со страницы {page_index}: тип={type(data)}, длина={len(data) if isinstance(data, (list, dict)) else 'N/A'}")
                
                if isinstance(data, list):
                    print(f"    ✅ Найдено этапов на странице {page_index}: {len(data)}")
                    merge_pages(data)
                elif isinstance(data, dict):
                    stages_list = data.get('stages') or data.get('construction_stages') or []
                    print(f"    ✅ Найдено этапов на странице {page_index}: {len(stages_list)}")
                    merge_pages(stages_list)
                else:
                    print(f"    ⚠️ Неожиданный формат данных на странице {page_index}: {type(data)}")
            except Exception as e:
                print(f"    ❌ Ошибка при обработке страницы {page_index}: {e}")
                import traceback
                traceback.print_exc()

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

        print(f"    ✅ Всего собрано этапов: {len(stages_merged)}")
        if stages_merged:
            print(f"    📸 Примеры фото из этапов:")
            for idx, stage in enumerate(stages_merged[:3], 1):
                photos_count = len(stage.get('photos', []))
                print(f"      Этап {idx}: дата={stage.get('date', 'N/A')}, фото={photos_count}")
        
        return {"construction_stages": stages_merged}
    except Exception as e:
        print(f"    ❌ Ошибка при извлечении хода строительства: {e}")
        import traceback
        traceback.print_exc()
        return {"construction_stages": []}


async def process_construction_stages_domclick(stages: List[Dict[str, Any]], complex_id: str) -> Dict[str, Any]:
    """Скачивает фото по этапам и загружает в S3, возвращает структуру construction_progress с URL."""
    if not stages:
        return {"construction_stages": []}
    s3 = S3Service()
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
                        return upload_with_watermark(s3, data, key)
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
            print(api_url)

            result = await page.evaluate(script, api_url)
            if isinstance(result, dict):
                if 'error' in result:
                    logger.warning(f"API запрос offset={offset} вернул ошибку (попытка {attempt}/{max_retries}): {result['error']}")
                    if attempt < max_retries:
                        await asyncio.sleep(2 * attempt)  # Экспоненциальная задержка: 2, 4, 6 секунд
                        continue
                    return None
                
                # Логируем структуру ответа для отладки
                logger.info(f"API ответ offset={offset}: ключи верхнего уровня: {list(result.keys())}")
                if 'result' in result:
                    logger.info(f"  Найден ключ 'result', его ключи: {list(result['result'].keys()) if isinstance(result['result'], dict) else 'не словарь'}")
                
                # Проверяем, есть ли обертка 'result'
                if 'result' in result and isinstance(result['result'], dict):
                    # Данные обернуты в 'result'
                    actual_data = result['result']
                    logger.info(f"  Используем данные из result, ключи: {list(actual_data.keys())}")
                    # Сохраняем также total из верхнего уровня, если он там есть
                    if 'total' in result:
                        actual_data['total'] = result['total']
                        logger.info(f"  Найден total в верхнем уровне: {result['total']}")
                    return actual_data
                
                # Успешный ответ без обертки
                logger.info(f"  Используем данные напрямую, ключи: {list(result.keys())}")
                if 'total' in result:
                    logger.info(f"  Найден total: {result['total']}")
                return result
            else:
                logger.warning(f"Неожиданный формат ответа API offset={offset} (попытка {attempt}/{max_retries})")
                if attempt < max_retries:
                    await asyncio.sleep(2 * attempt)
                    continue
                return None
        except Exception as e:
            logger.warning(f"Ошибка выполнения fetch запроса offset={offset} (попытка {attempt}/{max_retries}): {e}")
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
    s3 = S3Service()

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

    logger.info(f"Обработано {len(processed_photos)} из {len(photo_urls)} фотографий ЖК")
    return processed_photos


async def process_apartment_photos(apartment_data: Dict[str, Any], complex_id: str, apartment_path: str) -> Dict[str, Any]:
    """
    Обрабатывает фотографии для одной квартиры и загружает в S3.
    Возвращает данные с URL к файлам.
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
    s3 = S3Service()

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

        for i, result in enumerate(results):
            if isinstance(result, str) and result:
                processed_images.append(result)
            elif isinstance(result, Exception):
                logger.error(f"Ошибка обработки фото {i + 1}: {result}")
            else:
                logger.warning(f"Фото {i + 1} не обработано: {type(result)}")

    # Возвращаем данные квартиры с URL к файлам
    result = {
        "offer": apartment_data.get("offer"),
        "photos": processed_images
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
    logger.info(f"Обработка ответа API: ключи верхнего уровня: {list(api_data.keys()) if api_data else 'None'}")
    
    if not api_data:
        logger.warning("  api_data пустой или None")
        return {
            'offers': {},
            'address': None,
            'complexName': None,
            'complexHref': None
        }
    
    if 'items' not in api_data:
        logger.warning(f"  Ключ 'items' не найден в api_data. Доступные ключи: {list(api_data.keys())}")
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
            logger.warning(f"  ⚠️ Пропущен элемент {idx+1}/{total_items}: не является словарем (тип: {type(item).__name__})")
            skipped_count += 1
            continue
            
        general_info = item.get('generalInfo', {})
        if not general_info:
            logger.warning(f"  ⚠️ Пропущен элемент {idx+1}/{total_items}: отсутствует generalInfo. Ключи элемента: {list(item.keys())[:10]}")
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
        
        card = {
            'offer': title,
            'photos': image_urls  # Используем 'photos' для совместимости с MongoDB схемой
        }
        
        if room_key not in offers:
            offers[room_key] = []
        offers[room_key].append(card)
    
    processed_count = sum(len(cards) for cards in offers.values())
    logger.info(f"  Итого: получено={total_items}, обработано={processed_count}, пропущено={skipped_count}, групп={len(offers)}")
    
    if skipped_count > 0:
        logger.warning(f"  ⚠️ ВНИМАНИЕ: Пропущено {skipped_count} из {total_items} элементов!")
    
    return {
        'offers': offers,
        'address': address,
        'complexName': complex_name,
        'complexHref': complex_href
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
            # Используем waitUntil: 'networkidle0' чтобы дождаться полной загрузки
            try:
                print(f"  Открываю страницу для инициализации контекста: {base_url}")
                await page.goto(base_url, timeout=120000, waitUntil='networkidle0')
                
                # Дополнительно ждем, пока страница полностью загрузится
                await page.waitForFunction(
                    "() => document.readyState === 'complete'",
                    {"timeout": 30000}
                )
                
                # Ждем еще немного, чтобы все скрипты выполнились
                await asyncio.sleep(3)
                print(f"  Страница загружена, контекст готов")
            except Exception as e:
                print(f"  Предупреждение: не удалось открыть страницу: {e}")
                # Пробуем продолжить без открытия страницы

            # Делаем первый запрос для определения общего количества результатов
            attempts = 0
            first_api_response = None
            while attempts < 3:
                try:
                    # Проверяем, что страница еще жива и полностью загружена
                    try:
                        ready_state = await page.evaluate("() => document.readyState")
                        if ready_state != 'complete':
                            print(f"  Страница еще не загружена (readyState: {ready_state}), жду...")
                            await page.waitForFunction(
                                "() => document.readyState === 'complete'",
                                {"timeout": 30000}
                            )
                            await asyncio.sleep(2)
                    except Exception:
                        # Если контекст уничтожен, переоткрываем страницу
                        print(f"  Контекст уничтожен, переоткрываю страницу...")
                        await page.goto(base_url, timeout=120000, waitUntil='networkidle0')
                        await page.waitForFunction(
                            "() => document.readyState === 'complete'",
                            {"timeout": 30000}
                        )
                        await asyncio.sleep(3)
                    
                    print(f"  Запрос данных offset=0...")
                    first_api_response = await fetch_offers_api(page, api_params, 0, max_retries=3)
                    if first_api_response and 'items' in first_api_response:
                        print(f"  ✓ Успешно получено данных для offset=0")
                        break
                    attempts += 1
                    if attempts < 3:
                        print(f"  Повторная попытка через 2 секунды...")
                        await asyncio.sleep(2)
                except Exception as e:
                    attempts += 1
                    print(f"Ошибка при первом API запросе: {e} (попытка {attempts}/3)")
                    if attempts >= 3:
                        try:
                            browser, page, _ = await restart_browser(browser, headless=False)
                            attempts = 0
                        except Exception as restart_error:
                            print(f"  Ошибка при перезапуске браузера: {restart_error}")
                            break
                    else:
                        await asyncio.sleep(2)

            if not first_api_response:
                print(f"Не удалось получить данные из API для URL: {base_url}. Пропускаю.")
                url_index += 1
                offset = 0
                save_progress(url_index, offset, str(PROGRESS_FILE))
                continue

            # Определяем общее количество результатов и страниц
            total = first_api_response.get('total', 0)
            items_count = len(first_api_response.get('items', []))
            limit = int(api_params.get('limit', 20))
            
            logger.info(f"  Ответ API: total={total}, items в ответе={items_count}, limit={limit}")
            
            # Если total=0, но есть items, используем количество items как индикатор
            if total == 0 and items_count > 0:
                logger.warning(f"  total=0, но найдено {items_count} items. Будем запрашивать пока есть данные.")
                # Устанавливаем большое значение, чтобы цикл работал, но будем проверять наличие данных
                total = items_count + 1  # Чтобы цикл выполнился хотя бы один раз
            
            total_pages = max(1, (total + limit - 1) // limit) if total > 0 else 1
            print(f"  Всего результатов: {total}, items в первом ответе: {items_count}, страниц: {total_pages}")

            # Обрабатываем первый ответ
            first_data = process_api_response(first_api_response)
            aggregated_address = first_data.get('address')
            aggregated_complex_name = first_data.get('complexName')
            aggregated_complex_href = first_data.get('complexHref')
            aggregated_offers = first_data.get('offers', {})
            
            # Логируем первый batch
            log_apartment_photo_parsing(aggregated_offers, base_url=base_url, offset=0)

            # Обрабатываем остальные страницы
            current_offset = limit
            # Если total был установлен искусственно (из-за total=0), используем другой подход
            if total == items_count + 1:
                # Запрашиваем пока есть данные
                while True:
                    print(f"  Запрос данных offset={current_offset}...")
                    api_response = await fetch_offers_api(page, api_params, current_offset, max_retries=3)
                    
                    if api_response and 'items' in api_response:
                        response_items = api_response.get('items', [])
                        if not response_items:
                            print(f"  Нет данных для offset={current_offset}, завершаю обработку")
                            break
                        
                        data = process_api_response(api_response)
                        offers = data.get('offers', {})
                        log_apartment_photo_parsing(offers, base_url=base_url, offset=current_offset)
                        
                        # Объединяем группы офферов
                        for group, cards in offers.items():
                            if group not in aggregated_offers:
                                aggregated_offers[group] = []
                            aggregated_offers[group].extend(cards)
                        
                        offset = current_offset + limit
                        save_progress(url_index, offset, str(PROGRESS_FILE))
                        print(f"  ✓ Успешно получено {len(response_items)} элементов для offset={current_offset}")
                        
                        # Если получили меньше limit элементов, значит это последняя страница
                        if len(response_items) < limit:
                            print(f"  Получено меньше limit ({len(response_items)} < {limit}), это последняя страница")
                            break
                    else:
                        print(f"  ✗ Не удалось получить данные для offset={current_offset}, завершаю обработку")
                        break
                    
                    # Пауза между запросами
                    await asyncio.sleep(3)  # Пауза 3 секунды между запросами
                    current_offset += limit
            else:
                # Обычный случай: total известен
                while current_offset < total:
                    print(f"  Запрос данных offset={current_offset}...")
                    api_response = await fetch_offers_api(page, api_params, current_offset, max_retries=3)
                    
                    if api_response and 'items' in api_response:
                        data = process_api_response(api_response)
                        offers = data.get('offers', {})
                        log_apartment_photo_parsing(offers, base_url=base_url, offset=current_offset)
                        
                        # Объединяем группы офферов
                        for group, cards in offers.items():
                            if group not in aggregated_offers:
                                aggregated_offers[group] = []
                            aggregated_offers[group].extend(cards)
                        
                        offset = current_offset + limit
                        save_progress(url_index, offset, str(PROGRESS_FILE))
                        print(f"  ✓ Успешно получено данных для offset={current_offset}")
                    else:
                        print(f"  ✗ Не удалось получить данные для offset={current_offset}, пропускаю")
                    
                    # Пауза между запросами (кроме последнего)
                    if current_offset + limit < total:
                        await asyncio.sleep(3)  # Пауза 3 секунды между запросами
                    
                    current_offset += limit

            # Для получения фотографий ЖК и ссылки на ход строительства нужно открыть страницу комплекса
            complex_gallery_images: List[str] = []
            aggregated_hod_url: str = None
            construction_progress_data: Dict[str, Any] = None
            
            if aggregated_complex_href:
                try:
                    print(f"  Открываю страницу комплекса для получения фотографий ЖК: {aggregated_complex_href}")
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
                            // Пробуем альтернативные селекторы
                            galleryContainer = document.querySelector('[data-e2e-id*="gallery"]');
                          }
                          if (!galleryContainer) {
                            // Пробуем найти по классу
                            galleryContainer = document.querySelector('.gallery, [class*="gallery"], [class*="Gallery"]');
                          }
                          
                          console.log('Gallery container found:', !!galleryContainer);
                          
                          if (galleryContainer) {
                            // Пробуем разные селекторы для изображений
                            let imageElements = galleryContainer.querySelectorAll('[data-e2e-id^="complex-header-gallery-image__"]');
                            if (imageElements.length === 0) {
                              // Пробуем найти все изображения в контейнере
                              imageElements = galleryContainer.querySelectorAll('img');
                            }
                            
                            console.log('Image elements found:', imageElements.length);
                            
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
                          
                          console.log('Total photos found:', complexPhotos.length);
                          return complexPhotos;
                        }
                        """)
                        complex_gallery_images = complex_photos_data or []
                        print(f"  Найдено фотографий ЖК: {len(complex_gallery_images)}")
                        if complex_gallery_images:
                            print(f"  Примеры URL фото ЖК: {complex_gallery_images[:3]}")
                    except Exception as e:
                        print(f"  Ошибка при извлечении фотографий ЖК: {e}")
                        import traceback
                        traceback.print_exc()
                    
                    # Сохраняем ссылку на страницу "О ЖК" для хода строительства
                    try:
                        about_href = await page.evaluate("""
                        () => {
                          // Пробуем разные селекторы для поиска ссылки "О ЖК"
                          let a = document.querySelector('[data-e2e-id="complex-header-about"]');
                          console.log('Found by data-e2e-id:', !!a);
                          
                          if (!a) {
                            // Пробуем найти по тексту
                            const links = Array.from(document.querySelectorAll('a'));
                            a = links.find(link => {
                              const text = (link.textContent || '').toLowerCase().trim();
                              return text.includes('о жк') || text.includes('о комплексе') || text.includes('подробнее');
                            });
                            console.log('Found by text:', !!a);
                          }
                          if (!a) {
                            // Пробуем найти ссылку, содержащую "about" или "o-zhk"
                            const links = Array.from(document.querySelectorAll('a[href*="about"], a[href*="o-zhk"]'));
                            if (links.length > 0) {
                              a = links[0];
                            }
                            console.log('Found by href pattern:', !!a);
                          }
                          if (!a) {
                            // Пробуем найти ссылку на страницу комплекса с путем /about или /o-zhk
                            const currentPath = location.pathname;
                            const basePath = currentPath.split('/').slice(0, -1).join('/'); // Убираем последний сегмент
                            const links = Array.from(document.querySelectorAll(`a[href*="${basePath}/about"], a[href*="${basePath}/o-zhk"]`));
                            if (links.length > 0) {
                              a = links[0];
                            }
                            console.log('Found by base path:', !!a);
                          }
                          if (a) {
                            const href = a.getAttribute('href') || a.href || null;
                            console.log('Found href:', href);
                            if (href) {
                              // Преобразуем относительный URL в абсолютный
                              try {
                                return new URL(href, location.origin).href;
                              } catch {
                                return href.startsWith('http') ? href : location.origin + (href.startsWith('/') ? href : '/' + href);
                              }
                            }
                          }
                          console.log('No link found, returning null');
                          return null;
                        }
                        """)
                        print(f"  🔍 Результат поиска ссылки 'О ЖК': {about_href}")
                        if about_href:
                            print(f"  О ЖК URL: {about_href}")
                            # Проверяем, не содержит ли уже URL путь к ходу строительства
                            if '/hod-stroitelstva' in about_href:
                                aggregated_hod_url = about_href
                                print(f"  Ход строительства URL (уже содержит путь): {aggregated_hod_url}")
                            elif about_href.endswith('/'):
                                aggregated_hod_url = about_href + 'hod-stroitelstva'
                                print(f"  Ход строительства URL: {aggregated_hod_url}")
                            else:
                                aggregated_hod_url = about_href + '/hod-stroitelstva'
                                print(f"  Ход строительства URL: {aggregated_hod_url}")
                        else:
                            print(f"  ⚠️ Ссылка 'О ЖК' не найдена на странице. Пробую альтернативный способ...")
                            # Альтернативный способ: формируем URL напрямую из URL комплекса
                            if aggregated_complex_href:
                                # Проверяем, не содержит ли уже URL путь к ходу строительства
                                if '/hod-stroitelstva' in aggregated_complex_href:
                                    aggregated_hod_url = aggregated_complex_href
                                    print(f"  Ход строительства URL (уже содержит путь): {aggregated_hod_url}")
                                elif aggregated_complex_href.endswith('/'):
                                    aggregated_hod_url = aggregated_complex_href + 'hod-stroitelstva'
                                    print(f"  Ход строительства URL (сформирован автоматически): {aggregated_hod_url}")
                                else:
                                    aggregated_hod_url = aggregated_complex_href + '/hod-stroitelstva'
                                    print(f"  Ход строительства URL (сформирован автоматически): {aggregated_hod_url}")
                    except Exception as e:
                        print(f"  ❌ Ошибка при извлечении ссылки на ход строительства: {e}")
                        # Пробуем альтернативный способ даже при ошибке
                        if aggregated_complex_href:
                            # Проверяем, не содержит ли уже URL путь к ходу строительства
                            if '/hod-stroitelstva' in aggregated_complex_href:
                                aggregated_hod_url = aggregated_complex_href
                                print(f"  Ход строительства URL (уже содержит путь): {aggregated_hod_url}")
                            elif aggregated_complex_href.endswith('/'):
                                aggregated_hod_url = aggregated_complex_href + 'hod-stroitelstva'
                                print(f"  Ход строительства URL (сформирован автоматически после ошибки): {aggregated_hod_url}")
                            else:
                                aggregated_hod_url = aggregated_complex_href + '/hod-stroitelstva'
                                print(f"  Ход строительства URL (сформирован автоматически после ошибки): {aggregated_hod_url}")
                except Exception as e:
                    print(f"  Ошибка при открытии страницы комплекса: {e}")

            # формируем запись под Mongo-схему
            def to_db_item(complex_photos_urls: List[str] = None, processed_apartment_types: Dict[str, Any] = None) -> \
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
                                    "photos": c.get("photos") or [],  # URL к файлам в S3
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
                                    "photos": c.get("photos") or [],  # URL к файлам в S3
                                }
                                for c in apartment_list
                            ]
                        }
                    else:
                        # Если неизвестная структура, пропускаем
                        apartment_types[group] = cards
                        continue
                # Нормализуем URL для единообразия
                complex_url = normalize_complex_url(aggregated_complex_href) if aggregated_complex_href else None
                if not complex_url:
                    # Если не удалось нормализовать, используем base_url
                    complex_url = base_url
                
                return {
                    "url": complex_url,
                    "development": {
                        "complex_name": aggregated_complex_name,
                        "address": aggregated_address,
                        "source_url": base_url,
                        "photos": complex_photos_urls or [],  # URL к фотографиям ЖК в S3
                    },
                    "apartment_types": apartment_types,
                }

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
            processed_apartment_types = aggregated_offers
            if aggregated_offers:
                try:
                    processed_apartment_types = await process_all_apartment_types(aggregated_offers, complex_id)
                except Exception as e:
                    logger.error(f"Ошибка при обработке фотографий квартир: {e}")
                    processed_apartment_types = aggregated_offers

            # После сбора всех офферов: если есть hod_url — переходим и собираем ход строительства.
            # При ошибках (прокси/соединение) — перезапускаем браузер и пробуем ещё раз.
            if aggregated_hod_url:
                print(f"  Начинаю сбор хода строительства для URL: {aggregated_hod_url}")
            else:
                print(f"  ⚠️ URL хода строительства не найден, пропускаю сбор")
            
            if aggregated_hod_url:
                complex_id = get_complex_id_from_url(aggregated_complex_href or base_url)
                max_attempts_hod = 3
                attempt_hod = 0
                while attempt_hod < max_attempts_hod and not construction_progress_data:
                    attempt_hod += 1
                    try:
                        print(f"  Переход на страницу хода строительства: {aggregated_hod_url} (попытка {attempt_hod}/{max_attempts_hod})")
                        stages_data = await extract_construction_from_domclick(page, aggregated_hod_url)
                        if stages_data and stages_data.get('construction_stages'):
                            print(f"  Найдено этапов: {len(stages_data['construction_stages'])}")
                            construction_progress_data = await process_construction_stages_domclick(stages_data['construction_stages'], complex_id)
                            break
                        else:
                            print("  ⚠️ Этапы не получены со страницы хода строительства")
                            # Пробуем перезапустить браузер на следующую попытку
                            if attempt_hod < max_attempts_hod:
                                try:
                                    browser, page, _ = await restart_browser(browser, headless=False)
                                except Exception:
                                    pass
                    except Exception as e:
                        print(f"  ❌ Ошибка при сборе хода строительства: {e}")
                        if attempt_hod < max_attempts_hod:
                            try:
                                browser, page, _ = await restart_browser(browser, headless=False)
                                print("  🔄 Браузер перезапущен для повторной попытки хода строительства")
                            except Exception as restart_error:
                                print(f"  ⚠️ Ошибка перезапуска браузера: {restart_error}")

            db_item = to_db_item(complex_photos_urls, processed_apartment_types)
            if construction_progress_data:
                db_item.setdefault('development', {})['construction_progress'] = construction_progress_data

            try:
                save_to_mongodb([db_item])


            except Exception as e:
                print(f"Ошибка записи в MongoDB: {e}. Сохраню в {str(OUTPUT_FILE)} для отладки.")
                results.append({"sourceUrl": base_url,
                                "data": {"address": aggregated_address, "complexName": aggregated_complex_name,
                                         "complexHref": aggregated_complex_href, "offers": processed_apartment_types,
                                         "complexPhotosUrls": complex_photos_urls}})
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
    asyncio.run(run())
