import asyncio
import time
import json
import random
import os
import sys
import logging
from io import BytesIO
from pathlib import Path
from browser_manager import setup_stealth_browser
from db_config import get_collection, upsert_object_smart, check_duplicate_by_name
import aiohttp
from resize_img import ImageProcessor
from s3_service import S3Service

# Директория текущего скрипта
PROJECT_ROOT = Path(__file__).resolve().parent

# Файлы для работы
INPUT_JSON = PROJECT_ROOT / 'domrf_houses.json'
PROGRESS_FILE = PROJECT_ROOT / 'object_details_progress.json'
ERROR_OBJECTS_FILE = PROJECT_ROOT / 'error_objects.json'
UPLOADS_DIR = PROJECT_ROOT / 'uploads'

# Настройки повторных попыток
MAX_RETRIES = 3
RETRY_DELAY = 5
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
image_processor = ImageProcessor(logger, max_size=(800, 600), max_kb=150)


def create_object_directory(obj_id: str) -> Path:
    base_dir = UPLOADS_DIR / 'objects' / str(obj_id)
    (base_dir / 'gallery').mkdir(parents=True, exist_ok=True)
    (base_dir / 'construction').mkdir(parents=True, exist_ok=True)
    return base_dir


async def download_and_process_image(session: aiohttp.ClientSession, image_url: str, s3_key: str, s3: S3Service) -> str:
    """Скачивает, обрабатывает и загружает изображение в S3. Возвращает публичный URL."""
    try:
        async with session.get(image_url, timeout=aiohttp.ClientTimeout(total=30)) as response:
            if response.status != 200:
                logger.warning(f"HTTP {response.status} для {image_url}")
                return None
            image_bytes = await response.read()
            processed = image_processor.process(BytesIO(image_bytes))
            processed.seek(0)
            data = processed.read()
            # Загружаем в S3 вместо локального сохранения
            return s3.upload_bytes(data, s3_key, content_type="image/jpeg")
    except Exception as e:
        logger.error(f"Ошибка скачивания/обработки {image_url}: {e}")
        return None


async def process_photo_list(photo_urls, s3_key_prefix: str, prefix: str, limit: int = None):
    """Загружает список фото в S3. Возвращает список публичных URL."""
    if not photo_urls:
        return []
    if limit is not None:
        photo_urls = list(photo_urls)[:limit]
    results = []
    s3 = S3Service()
    async with aiohttp.ClientSession() as session:
        sem = asyncio.Semaphore(5)
        async def work(url, idx):
            async with sem:
                s3_key = f"{s3_key_prefix}/{prefix}_{idx + 1}.jpg"
                return await download_and_process_image(session, url, s3_key, s3)
        tasks = [work(u, i) for i, u in enumerate(photo_urls)]
        saved = await asyncio.gather(*tasks, return_exceptions=True)
        for p in saved:
            if isinstance(p, str) and p:
                results.append(p)
    return results



async def check_ban_status(page):
    """Проверяет, заблокирован ли доступ к сайту"""
    return await page.evaluate('''() => {
        const bodyText = document.body.innerText.toLowerCase();
        const banMessages = [
            "нам очень жаль, но запросы с вашего устройства похожи на автоматические",
            "подтвердите, что вы не робот — потяните ползунок",
            "потяните ползунок, чтобы развернуть картинку",
            "запросы похожи на автоматические",
            "автоматические запросы",
            "доступ ограничен",
            "проверка безопасности",
            "cloudflare",
            "blocked",
            "captcha"
        ];
        
        return banMessages.some(msg => bodyText.includes(msg));
    }''')


async def extract_gallery_images(page):
    """Собирает ссылки всех фото ЖК из верхней галереи."""
    try:
        images = await page.evaluate('''() => {
            const urls = new Set();
            try {
                // Основной контейнер галереи карточки
                const gallery = document.querySelector('[class*="NewBuildingCard__GalleryContainer"], [class*="GalleryWrapper"], [data-testid*="gallery"], .swiper');
                const scope = gallery || document;

                // Берем все <img> внутри области галереи
                scope.querySelectorAll('img').forEach(img => {
                    const src = img.getAttribute('src') || '';
                    const dataSrc = img.getAttribute('data-src') || img.getAttribute('data-lazy') || '';
                    [src, dataSrc].forEach(v => {
                        if (v && !v.startsWith('data:')) urls.add(v);
                    });
                });

                // Иногда swiper рендерит lazy-изображения в active-слайде отдельно
                const active = document.querySelector('.swiper-slide.swiper-slide-active img');
                if (active) {
                    const src = active.getAttribute('src') || active.getAttribute('data-src') || active.getAttribute('data-lazy');
                    if (src && !src.startsWith('data:')) urls.add(src);
                }
            } catch (e) {}
            return Array.from(urls);
        }''')
        return images or []
    except Exception as e:
        print(f"Ошибка при извлечении галереи: {e}")
        return []


async def fetch_flats_api_in_browser(page, obj_id, flat_type, limit=100, offset=0):
    """Выполняет API запрос для получения данных о квартирах (таймаут: 15 секунд)"""
    
    # Проверяем бан перед каждым API запросом
    ban_detected = await check_ban_status(page)
    
    if ban_detected:
        print(f"🚫 Обнаружен бан при API запросе квартир! Прерываем запрос.")
        return "BAN_DETECTED"
    
    api_url = f"https://xn--80az8a.xn--d1aqf.xn--p1ai/portal-kn/api/kn/objects/{obj_id}/flats"
    params = {
        'flatGroupType': flat_type,
        'limit': limit,
        'offset': offset
    }
    
    query = "&".join(f"{k}={v}" for k, v in params.items())
    url = f"{api_url}?{query}"
    
    js_code = f'''
        async () => {{
            try {{
                // Создаем AbortController для таймаута
                const controller = new AbortController();
                const timeoutId = setTimeout(() => controller.abort(), 15000); // 15 секунд
                
                const resp = await fetch("{url}", {{
                    headers: {{
                        'accept': 'application/json, text/plain, */*',
                        'authorization': 'Basic MTpxd2U=',
                        'sec-fetch-dest': 'empty',
                        'sec-fetch-mode': 'cors',
                        'sec-fetch-site': 'same-origin'
                    }},
                    method: 'GET',
                    mode: 'cors',
                    credentials: 'include',
                    signal: controller.signal
                }});
                
                clearTimeout(timeoutId); // Очищаем таймаут после успешного запроса
                
                if (!resp.ok) return null;
                return await resp.json();
            }} catch (e) {{
                if (e.name === 'AbortError') {{
                    console.log('Таймаут API запроса (15 секунд)');
                }} else {{
                    console.log('Ошибка при запросе API квартир:', e);
                }}
                return null;
            }}
        }}
    '''
    return await page.evaluate(js_code)


async def get_all_flats_for_type(page, obj_id, flat_type):
    """Получает все квартиры определенного типа с пагинацией"""
    all_flats = []
    offset = 0
    limit = 100
    page_num = 1
    consecutive_errors = 0  # Счетчик последовательных ошибок
    max_consecutive_errors = 3  # Максимальное количество последовательных ошибок

    while True:
        try:
            # Проверяем бан перед каждым API запросом
            ban_detected = await check_ban_status(page)
            
            if ban_detected:
                print(f"  🚫 Обнаружен бан при получении квартир типа {flat_type} (страница {page_num}). Прерываем обработку.")
                return {
                    'flats': [],
                    'total_count': 0,
                    'consecutive_errors': 999  # Специальный код для бана
                }
            
            print(f"  Получаем страницу {page_num} для {flat_type} (offset={offset}, limit={limit})")
            flats_data = await fetch_flats_api_in_browser(page, obj_id, flat_type, limit, offset)
            
            # Проверяем бан после API запроса (на случай если бан появился в результате запроса)
            ban_detected_after = await check_ban_status(page)
            if ban_detected_after:
                print(f"  🚫 Обнаружен бан после API запроса квартир типа {flat_type} (страница {page_num}). Прерываем обработку.")
                return {
                    'flats': [],
                    'total_count': 0,
                    'consecutive_errors': 999  # Специальный код для бана
                }
            
            # Проверяем, обнаружен ли бан
            if flats_data == "BAN_DETECTED":
                print(f"  🚫 Обнаружен бан при получении квартир типа {flat_type}. Прерываем обработку.")
                return {
                    'flats': [],
                    'total_count': 0,
                    'consecutive_errors': 999  # Специальный код для бана
                }
            
            if not flats_data:
                consecutive_errors += 1
                print(f"  ❌ Ошибка или пустой ответ для {flat_type} на offset={offset} (ошибка {consecutive_errors}/{max_consecutive_errors})")

                if consecutive_errors >= max_consecutive_errors:
                    print(f"  🛑 Превышено максимальное количество ошибок ({max_consecutive_errors}) для {flat_type}. Переходим к следующему типу.")
                    break
                else:
                    # Пробуем увеличить offset и повторить запрос
                    offset += limit
                    page_num += 1
                    await asyncio.sleep(0.5)  # Увеличенная задержка при ошибке
                    continue

            # Если получили данные, сбрасываем счетчик ошибок
            consecutive_errors = 0

            # Проверяем структуру ответа
            if 'data' in flats_data and isinstance(flats_data['data'], list):
                flats = flats_data['data']
                if not flats:
                    print(f"  ✅ Получены все квартиры типа {flat_type}. Всего: {len(all_flats)}")
                    break

                all_flats.extend(flats)
                print(f"  📄 Получено {len(flats)} квартир, всего: {len(all_flats)}")

                # Если получили меньше запрошенного количества, значит это последняя страница
                if len(flats) < limit:
                    print(f"  ✅ Получены все квартиры типа {flat_type}. Всего: {len(all_flats)}")
                    break

            elif isinstance(flats_data, list):
                # Если ответ - это просто массив квартир
                flats = flats_data
                if not flats:
                    print(f"  ✅ Получены все квартиры типа {flat_type}. Всего: {len(all_flats)}")
                    break

                all_flats.extend(flats)
                print(f"  📄 Получено {len(flats)} квартир, всего: {len(all_flats)}")

                # Если получили меньше запрошенного количества, значит это последняя страница
                if len(flats) < limit:
                    print(f"  ✅ Получены все квартиры типа {flat_type}. Всего: {len(all_flats)}")
                    break
            else:
                consecutive_errors += 1
                print(f"  ❌ Неожиданная структура ответа для {flat_type} (ошибка {consecutive_errors}/{max_consecutive_errors})")

                if consecutive_errors >= max_consecutive_errors:
                    print(f"  🛑 Превышено максимальное количество ошибок ({max_consecutive_errors}) для {flat_type}. Переходим к следующему типу.")
                    break
                else:
                    offset += limit
                    page_num += 1
                    await asyncio.sleep(0.5)
                    continue

            # Переходим к следующей странице
            offset += limit
            page_num += 1

            # Небольшая задержка между запросами
            await asyncio.sleep(0.2)

        except Exception as e:
            consecutive_errors += 1
            print(f"  ❌ Ошибка при получении страницы {page_num} для {flat_type}: {e} (ошибка {consecutive_errors}/{max_consecutive_errors})")

            if consecutive_errors >= max_consecutive_errors:
                print(f"  🛑 Превышено максимальное количество ошибок ({max_consecutive_errors}) для {flat_type}. Переходим к следующему типу.")
                break
            else:
                # Пробуем продолжить с увеличенным offset
                offset += limit
                page_num += 1
                await asyncio.sleep(0.5)  # Увеличенная задержка при ошибке

    return {
        'flats': all_flats,
        'total_count': len(all_flats),
        'consecutive_errors': consecutive_errors
    }


async def extract_construction_progress(page):
    """Извлекает данные о ходе строительства и фотографиях"""
    try:
        construction_data = await page.evaluate('''() => {
            const result = {
                'construction_stages': [],
                'photos': []
            };
            
            try {
                // Ищем секцию "ХОД СТРОИТЕЛЬСТВА" по классу
                const constructionSection = document.querySelector('[class*="ConstructionProgressWrapper"]');
                
                if (constructionSection) {
                    console.log('Найдена секция хода строительства');
                    
                    // Ищем все карточки этапов строительства
                    const stageCards = document.querySelectorAll('[class*="ConstructionProgressCard_CardWrapper"]');
                    console.log('Найдено карточек этапов:', stageCards.length);
                    
                    stageCards.forEach((card, index) => {
                        try {
                            const stage = {};
                            
                            // Извлекаем дату этапа из h4 с классом Date
                            const dateElement = card.querySelector('h4[class*="Date"]');
                            if (dateElement) {
                                stage.date = dateElement.innerText.trim();
                                console.log('Найдена дата:', stage.date);
                            }
                            
                            // Извлекаем количество фото из span с классом PhotosCount
                            const photosCountElement = card.querySelector('span[class*="PhotosCount"]');
                            if (photosCountElement) {
                                stage.photos_count = photosCountElement.innerText.trim();
                                console.log('Найдено количество фото:', stage.photos_count);
                            }
                            
                            // Извлекаем дату последнего обновления из span с классом LastUpdate
                            const lastUpdateElement = card.querySelector('span[class*="LastUpdate"]');
                            if (lastUpdateElement) {
                                stage.last_update = lastUpdateElement.innerText.trim();
                                console.log('Найдена дата обновления:', stage.last_update);
                            }
                            
                            // Извлекаем ссылки на фотографии из img с классом Preview
                            const images = card.querySelectorAll('img[class*="Preview"]');
                            const photoUrls = [];
                            images.forEach(img => {
                                const src = img.src;
                                if (src && !src.includes('data:') && !src.includes('placeholder')) {
                                    photoUrls.push(src);
                                    console.log('Найдено фото:', src);
                                }
                            });
                            
                            // Сохраняем фото в этап
                            stage.photos = photoUrls;
                            
                            // Также добавляем в общий массив для совместимости
                            if (photoUrls.length > 0) {
                                result.photos.push(...photoUrls);
                            }
                            
                            // Добавляем этап, если есть хотя бы дата
                            if (stage.date) {
                                stage.stage_number = index + 1;
                                result.construction_stages.push(stage);
                                console.log('Добавлен этап:', stage);
                            }
                            
                        } catch (cardError) {
                            console.log('Ошибка при обработке карточки этапа:', cardError);
                        }
                    });
                    
                    // Если карточки не найдены, ищем альтернативным способом
                    if (result.construction_stages.length === 0) {
                        console.log('Карточки не найдены, ищем по тексту страницы');
                        // Ищем по тексту страницы
                        const pageText = document.body.innerText;
                        const dateMatches = pageText.match(/([А-Яа-я]+,\\s*\\d{4})/g);
                        
                        if (dateMatches) {
                            console.log('Найденные даты в тексте:', dateMatches);
                            dateMatches.forEach((dateMatch, index) => {
                                result.construction_stages.push({
                                    stage_number: index + 1,
                                    date: dateMatch.trim(),
                                    photos_count: '',
                                    last_update: '',
                                    photos: []
                                });
                            });
                        }
                    }
                    
                    // Ищем все фотографии в секции строительства и распределяем по этапам
                    const allImages = constructionSection.querySelectorAll('img[src]');
                    const generalPhotos = [];
                    allImages.forEach(img => {
                        const src = img.src;
                        if (src && !src.includes('data:') && !result.photos.includes(src)) {
                            generalPhotos.push(src);
                        }
                    });
                    
                    // Если есть общие фото и нет фото в этапах, распределяем их
                    if (generalPhotos.length > 0 && result.construction_stages.length > 0) {
                        const photosPerStage = Math.ceil(generalPhotos.length / result.construction_stages.length);
                        let photoIndex = 0;
                        result.construction_stages.forEach(stage => {
                            if (!stage.photos || stage.photos.length === 0) {
                                stage.photos = generalPhotos.slice(photoIndex, photoIndex + photosPerStage);
                                photoIndex += photosPerStage;
                            }
                        });
                    }
                    
                    result.photos.push(...generalPhotos);
                    
                } else {
                    console.log('Секция хода строительства не найдена');
                    // Fallback: ищем по тексту страницы
                    const pageText = document.body.innerText;
                    if (pageText.includes('ХОД СТРОИТЕЛЬСТВА')) {
                        console.log('Найден текст "ХОД СТРОИТЕЛЬСТВА", ищем даты');
                        const dateMatches = pageText.match(/([А-Яа-я]+,\\s*\\d{4})/g);
                        if (dateMatches) {
                            dateMatches.forEach((dateMatch, index) => {
                                result.construction_stages.push({
                                    stage_number: index + 1,
                                    date: dateMatch.trim(),
                                    photos_count: '',
                                    last_update: '',
                                    photos: []
                                });
                            });
                        }
                    }
                }
                
                console.log('Итоговый результат:', result);
                
            } catch (e) {
                console.log('Ошибка при извлечении данных о ходе строительства:', e);
            }
            
            return result;
        }''')

        return construction_data

    except Exception as e:
        print(f"Ошибка при извлечении данных о ходе строительства: {e}")
        return {
            'construction_stages': [],
            'photos': []
        }


def load_progress():
    """Загружает сохраненный прогресс из файла"""
    if os.path.exists(PROGRESS_FILE):
        try:
            with open(PROGRESS_FILE, 'r', encoding='utf-8') as f:
                progress = json.load(f)
                print(f"Загружен прогресс: обработано {len(progress.get('processed_ids', []))} объектов")
                return progress
        except Exception as e:
            print(f"Ошибка при загрузке прогресса: {e}")
    return {'processed_ids': [], 'failed_ids': []}


def save_progress(processed_ids, failed_ids):
    """Сохраняет текущий прогресс в файл"""
    try:
        # json не умеет сериализовать set — конвертируем в списки
        if isinstance(processed_ids, set):
            processed_ids = list(processed_ids)
        if isinstance(failed_ids, set):
            failed_ids = list(failed_ids)

        progress = {'processed_ids': processed_ids, 'failed_ids': failed_ids}
        with open(PROGRESS_FILE, 'w', encoding='utf-8') as f:
            json.dump(progress, f, ensure_ascii=False, indent=2)
        print(f"Прогресс сохранен: обработано {len(processed_ids)}, ошибок {len(failed_ids)}")
    except Exception as e:
        print(f"Ошибка при сохранении прогресса: {e}")


def load_error_objects():
    """Загружает список ошибочных объектов из файла"""
    if os.path.exists(ERROR_OBJECTS_FILE):
        try:
            with open(ERROR_OBJECTS_FILE, 'r', encoding='utf-8') as f:
                error_objects = json.load(f)
                print(f"📋 Загружено {len(error_objects)} ошибочных объектов для повторной обработки")
                return error_objects
        except Exception as e:
            print(f"Ошибка при загрузке ошибочных объектов: {e}")
    return []


def save_error_objects(error_objects):
    """Сохраняет список ошибочных объектов в файл"""
    try:
        with open(ERROR_OBJECTS_FILE, 'w', encoding='utf-8') as f:
            json.dump(error_objects, f, ensure_ascii=False, indent=2)
        print(f"💾 Сохранено {len(error_objects)} ошибочных объектов в файл")
    except Exception as e:
        print(f"Ошибка при сохранении ошибочных объектов: {e}")


def add_error_object(error_objects, obj, error_reason):
    """Добавляет объект в список ошибочных с причиной ошибки"""
    error_entry = {
        'objId': obj.get('objId'),
        'objCommercNm': obj.get('objCommercNm'),
        'url': f"https://наш.дом.рф/сервисы/каталог-новостроек/объект/{obj.get('objId')}",
        'error_reason': error_reason,
        'timestamp': time.strftime('%Y-%m-%d %H:%M:%S'),
        'full_object': obj
    }
    error_objects.append(error_entry)
    print(f"❌ Объект {obj.get('objId')} добавлен в список ошибочных: {error_reason}")
    return error_objects


async def extract_object_details(page, obj_id, on_partial=None):
    """Извлекает детальную информацию об объекте со страницы"""
    url = f'https://наш.дом.рф/сервисы/каталог-новостроек/объект/{obj_id}'
    print(f"Переходим на страницу объекта: {url}")

    details = {}

    try:
        # Переходим на страницу объекта
        await page.goto(url, timeout=30000)
        print("Страница загружена, ожидаем появления элементов...")
        await asyncio.sleep(10)

        # Проверяем бан сразу после загрузки страницы
        ban_detected = await check_ban_status(page)

        if ban_detected:
            print(f"🚫 Обнаружен бан сразу после загрузки страницы объекта {obj_id}! Прерываем обработку.")
            return "BAN_DETECTED"

        # Ждем загрузки основных элементов
        await asyncio.sleep(5)

        # Проверяем бан/капчу
        try:
            ban_detected = await page.evaluate('''() => {
                const bodyText = document.body.innerText.toLowerCase();
                const banMessages = [
                    "нам очень жаль, но запросы с вашего устройства похожи на автоматические",
                    "запросы похожи на автоматические",
                    "автоматические запросы",
                    "доступ ограничен",
                    "проверка безопасности",
                    "cloudflare",
                    "blocked",
                    "captcha"
                ];
                
                return banMessages.some(msg => bodyText.includes(msg));
            }''')

            if ban_detected:
                print("🚫 Обнаружен бан или капча!")
                return "BAN_DETECTED"
        except Exception as ban_error:
            print(f"Ошибка при проверке бана: {ban_error}")

        # Извлекаем основные характеристики через JavaScript
        characteristics = await page.evaluate('''() => {
            const result = {
                'main_characteristics': {},
                'yard_improvement': {},
                'parking_space': {},
                'accessible_environment': {},
                'elevators': {},
                'energy_efficiency': '',
                'contractors': ''
            };

            // Функция для поиска значения рядом с лейблом
            function findValueByLabel(labelText, section = 'main_characteristics') {
                const spans = document.querySelectorAll('span');
                for (const span of spans) {
                    const text = span.innerText || '';
                    if (text.includes(labelText)) {
                        // Ищем следующий элемент с числом или значением
                        const parent = span.parentElement;
                        if (parent) {
                            const siblings = Array.from(parent.children);
                            const currentIndex = siblings.indexOf(span);

                            // Ищем следующий элемент с значением
                            for (let i = currentIndex + 1; i < siblings.length; i++) {
                                const sibling = siblings[i];
                                const siblingText = sibling.innerText || '';

                                // Пропускаем пустые элементы и элементы с только пробелами
                                if (siblingText.trim() && siblingText.trim() !== ',') {
                                    // Для числовых полей проверяем, что это число
                                    if (labelText.includes('Количество') || labelText.includes('площадь') || labelText.includes('потолков')) {
                                        if (/^[0-9\\s,.,]+$/.test(siblingText.trim())) {
                                            return siblingText.trim();
                                        }
                                    } else {
                                        // Для текстовых полей берем любое непустое значение
                                        return siblingText.trim();
                                    }
                                }
                            }
                        }
                    }
                }
                return null;
            }

            // Извлекаем основные характеристики
            const mainFields = [
                'Класс недвижимости',
                'Материал стен', 
                'Тип отделки',
                'Свободная планировка',
                'Количество этажей',
                'Жилая площадь',
                'Высота потолков'
            ];

            for (const field of mainFields) {
                const value = findValueByLabel(field);
                if (value) {
                    result.main_characteristics[field] = value;
                }
            }

            // Извлекаем благоустройство двора
            const yardFields = [
                'Велосипедные дорожки',
                'Количество детских площадок',
                'Количество спортивных площадок',
                'Количество площадок для сбора мусора'
            ];

            for (const field of yardFields) {
                const value = findValueByLabel(field);
                if (value) {
                    result.yard_improvement[field] = value;
                }
            }

            // Извлекаем парковочное пространство
            const parkingFields = [
                'Количество мест в паркинге',
                'Гостевые места на придомовой территории',
                'Гостевые места вне придомовой территории'
            ];

            for (const field of parkingFields) {
                const value = findValueByLabel(field);
                if (value) {
                    result.parking_space[field] = value;
                }
            }

            // Извлекаем безбарьерную среду
            const accessibleFields = [
                'Наличие пандуса',
                'Наличие понижающих площадок',
                'Количество инвалидных подъемников'
            ];

            for (const field of accessibleFields) {
                const value = findValueByLabel(field);
                if (value) {
                    result.accessible_environment[field] = value;
                }
            }

            // Извлекаем лифты
            const elevatorFields = [
                'Количество подъездов',
                'Количество пассажирских лифтов',
                'Количество грузовых и грузопассажирских лифтов'
            ];

            for (const field of elevatorFields) {
                const value = findValueByLabel(field);
                if (value) {
                    result.elevators[field] = value;
                }
            }

            // Извлекаем общую информацию
            try {
                const pageText = document.body.innerText;
                
                // Класс энергоэффективности
                const energyMatch = pageText.match(/Класс энергоэффективности здания:\\s*([A-Z])/);
                if (energyMatch) {
                    result.energy_efficiency = energyMatch[1];
                }
                
                // Генподрядчики
                const contractorMatch = pageText.match(/Генподрядчики:\\s*([^\\n]+)/);
                if (contractorMatch) {
                    result.contractors = contractorMatch[1];
                }
            } catch (e) {
                console.log('Ошибка при извлечении общей информации:', e);
            }
            

            return result;
        }''')

        details.update(characteristics)
        print(f"Извлечены характеристики для объекта {obj_id}")
        # Сохраняем частичный результат после извлечения характеристик
        if callable(on_partial):
            try:
                on_partial(details)
            except Exception as cb_err:
                print(f"Ошибка при промежуточном сохранении (характеристики): {cb_err}")

        # Дополнительная проверка капчи перед сбором галереи
        if await check_ban_status(page):
            print("🚫 Капча обнаружена перед сбором галереи")
            return "BAN_DETECTED"

        # Сбор изображений из галереи ЖК
        print(f"📷 Извлекаем фото галереи для объекта {obj_id}")
        gallery_photos_urls = await extract_gallery_images(page)
        if gallery_photos_urls:
            # Загружаем фото в S3
            s3_key_prefix = f"objects/{obj_id}/gallery"
            saved_gallery = await process_photo_list(gallery_photos_urls, s3_key_prefix, 'photo', limit=12)
            details['gallery_photos'] = saved_gallery
            print(f"📸 Галерея загружена в S3: {len(saved_gallery)} файлов")
        else:
            print("ℹ️ Фото галереи не найдены")

        # Получаем данные о квартирах через отдельные API запросы с пагинацией
        flat_types = ['oneRoom', 'twoRoom', 'threeRoom', 'fourRoom']
        flats_data = {}
        
        for flat_type in flat_types:
            try:
                # Проверяем бан перед обработкой каждого типа квартир
                ban_detected = await check_ban_status(page)
                
                if ban_detected:
                    print(f"🚫 Обнаружен бан перед обработкой квартир типа {flat_type}! Прерываем обработку объекта {obj_id}")
                    return "BAN_DETECTED"
                
                print(f"🏠 Получаем ВСЕ квартиры типа {flat_type} для объекта {obj_id}")
                flats_result = await get_all_flats_for_type(page, obj_id, flat_type)
                
                # Проверяем, был ли обнаружен бан
                if flats_result.get('consecutive_errors') == 999:
                    print(f"🚫 Обнаружен бан при получении квартир! Прерываем обработку объекта {obj_id}")
                    return "BAN_DETECTED"
                
                if flats_result['total_count'] > 0:
                    flats_data[flat_type] = {
                        'flats': flats_result['flats'],
                        'total_count': flats_result['total_count']
                    }
                    print(f"✅ Получено {flats_result['total_count']} квартир типа {flat_type}")
                else:
                    if flats_result.get('consecutive_errors', 0) >= 3:
                        print(f"⚠️  Квартир типа {flat_type} не найдено (превышено количество ошибок)")
                    else:
                        print(f"ℹ️  Квартир типа {flat_type} не найдено")

                # Проверяем бан после обработки каждого типа квартир
                ban_detected_after_type = await check_ban_status(page)
                if ban_detected_after_type:
                    print(f"🚫 Обнаружен бан после обработки квартир типа {flat_type}! Прерываем обработку объекта {obj_id}")
                    return "BAN_DETECTED"

                # Промежуточное сохранение после каждого типа квартир
                if callable(on_partial):
                    try:
                        details_partial = dict(details)
                        if flats_data:
                            details_partial['flats_data'] = dict(flats_data)
                        on_partial(details_partial)
                    except Exception as cb_err:
                        print(f"Ошибка при промежуточном сохранении (квартиры {flat_type}): {cb_err}")

                # Пауза между запросами разных типов квартир
                if flat_type != 'fourRoom':  # Не делаем паузу после последнего типа
                    await asyncio.sleep(1)

            except Exception as e:
                print(f"❌ Критическая ошибка при получении данных о {flat_type} квартирах: {e}")

        # Добавляем данные о квартирах к общим данным
        if flats_data:
            details['flats_data'] = flats_data
            total_flats = sum(data['total_count'] for data in flats_data.values())
            print(f"✅ Всего получено {total_flats} квартир для объекта {obj_id}")
        else:
            print(f"ℹ️  Квартир не найдено для объекта {obj_id}")

        # Повторная проверка капчи перед разделом хода строительства
        if await check_ban_status(page):
            print("🚫 Капча обнаружена перед ходом строительства")
            return "BAN_DETECTED"

        # Получаем данные о ходе строительства и фотографиях
        print(f"🏗️  Извлекаем данные о ходе строительства для объекта {obj_id}")
        construction_data = await extract_construction_progress(page)
        # Загружаем фото хода строительства в S3 (если есть)
        if construction_data:
            # Фото по этапам
            stages = construction_data.get('construction_stages') or []
            for idx, stage in enumerate(stages):
                photos = stage.get('photos') or []
                if not photos:
                    continue
                stage_num = stage.get('stage_number') or (idx + 1)
                s3_key_prefix = f"objects/{obj_id}/construction/stage_{stage_num}"
                saved_stage = await process_photo_list(photos, s3_key_prefix, 'photo', limit=10)
                stage['photos'] = saved_stage
            
            # Убираем общий массив photos, оставляем только по этапам
            if 'photos' in construction_data:
                del construction_data['photos']

        if construction_data and construction_data.get('construction_stages'):
            details['construction_progress'] = construction_data
            print(f"✅ Найдено {len(construction_data['construction_stages'])} этапов строительства")
            # Подсчитываем общее количество фото по всем этапам
            total_photos = sum(len(stage.get('photos', [])) for stage in construction_data['construction_stages'])
            if total_photos > 0:
                print(f"📸 Найдено {total_photos} фотографий по этапам")
        else:
            print(f"ℹ️  Данные о ходе строительства не найдены для объекта {obj_id}")

        # Промежуточное сохранение после хода строительства
        if callable(on_partial):
            try:
                on_partial(details)
            except Exception as cb_err:
                print(f"Ошибка при промежуточном сохранении (ход строительства): {cb_err}")

    except Exception as e:
        error_message = str(e)
        print(f"Ошибка при извлечении данных объекта {obj_id}: {e}")

        # Проверяем, является ли это ошибкой прокси или соединения
        connection_errors = [
            "ERR_PROXY_CONNECTION_FAILED",
            "ERR_CONNECTION_CLOSED", 
            "ERR_CONNECTION_REFUSED",
            "ERR_CONNECTION_RESET",
            "ERR_CONNECTION_ABORTED",
            "PROXY",
            "CONNECTION_CLOSED"
        ]
        
        if any(err in error_message for err in connection_errors):
            print("🔌 Обнаружена ошибка подключения/прокси!")
            return "PROXY_ERROR"

        return None

    return details


async def process_objects_batch(objects_to_process, collection, processed_ids, failed_ids, is_retry=False):
    """Обрабатывает пакет объектов. Возвращает список ошибочных объектов."""
    # Список для сохранения ошибочных объектов
    error_objects = []
    
    # Создаем браузер один раз для всех объектов
    browser = None
    page = None
    error_count = 0

    try:
        # Создаем браузер с антидетект-настройками (прокси настроено в open_browser.py)
        browser, page = await setup_stealth_browser()
        print("Браузер создан с антидетект-настройками")

        # Обрабатываем объекты
        retry_suffix = " (ПОВТОРНАЯ ПОПЫТКА)" if is_retry else ""
        for i, obj in enumerate(objects_to_process):
            obj_id = obj.get('objId')
            obj_commerc_nm = obj.get('objCommercNm')
            print(f"\n🔄 Обрабатываем объект {i + 1}/{len(objects_to_process)} (ID: {obj_id}){retry_suffix}")

            # Проверяем дубликат в самом начале
            if check_duplicate_by_name(collection, obj_id, obj_commerc_nm):
                print(f"⏭️  Пропускаем объект {obj_id} из-за дубликата")
                # Сохраняем прогресс
                processed_ids.add(obj_id)
                save_progress(processed_ids, failed_ids)
                continue

            # Цикл повторных попыток для одного объекта
            max_retries_obj = 3
            retry_obj = 0
            obj_processed = False
            error_reason = None

            while retry_obj < max_retries_obj and not obj_processed:
                try:
                    # Колбэк для промежуточного сохранения текущего объекта
                    def on_partial_save(details_partial):
                        obj_copy = obj.copy()
                        obj_copy['object_details'] = details_partial
                        obj_copy['details_extracted_at'] = time.strftime('%Y-%m-%d %H:%M:%S')
                        try:
                            upsert_object_smart(collection, obj_id, obj_copy)
                        except Exception as inner_err:
                            print(f"Ошибка при промежуточном сохранении в MongoDB: {inner_err}")

                    # Извлекаем детали объекта с промежуточными сохранениями
                    details = await extract_object_details(page, obj_id, on_partial=on_partial_save)

                    if details == "PROXY_ERROR":
                        retry_obj += 1
                        error_reason = "Ошибка прокси"
                        print(f"🔌 Ошибка прокси для объекта {obj_id}! Попытка {retry_obj}/{max_retries_obj}")
                        error_count += 1

                        if retry_obj < max_retries_obj:
                            # Перезапускаем браузер при ошибке прокси
                            try:
                                await browser.close()
                            except:
                                pass
                            await asyncio.sleep(2)
                            browser, page = await setup_stealth_browser()
                            print(f"🔄 Браузер перезапущен с новым прокси, повторяем объект {obj_id}")
                            continue  # Повторяем while для того же объекта
                        else:
                            print(f"❌ Исчерпаны попытки для объекта {obj_id}")
                            error_objects = add_error_object(error_objects, obj, error_reason)
                            save_error_objects(error_objects)
                            break
                            
                    elif details == "BAN_DETECTED":
                        retry_obj += 1
                        error_reason = "Обнаружен бан/капча"
                        print(f"🚫 Обнаружен бан/капча для объекта {obj_id}! Попытка {retry_obj}/{max_retries_obj}")
                        error_count += 1

                        if retry_obj < max_retries_obj:
                            # Перезапускаем браузер при бане/капче
                            try:
                                await browser.close()
                            except:
                                pass
                            await asyncio.sleep(5)  # Увеличиваем задержку при бане
                            browser, page = await setup_stealth_browser()
                            print(f"🔄 Браузер перезапущен после обнаружения бана, повторяем объект {obj_id}")
                            continue  # Повторяем while для того же объекта
                        else:
                            print(f"❌ Исчерпаны попытки для объекта {obj_id}")
                            error_objects = add_error_object(error_objects, obj, error_reason)
                            save_error_objects(error_objects)
                            break
                            
                    elif details:
                        # Добавляем детали к объекту
                        obj_copy = obj.copy()
                        obj_copy['object_details'] = details
                        obj_copy['details_extracted_at'] = time.strftime('%Y-%m-%d %H:%M:%S')

                        # Обновляем запись в MongoDB используя умное сохранение
                        try:
                            if upsert_object_smart(collection, obj_id, obj_copy):
                                print(f"✅ Данные объекта {obj_id} сохранены в MongoDB (коллекция domrf)")
                                processed_ids.add(obj_id)

                                # Сохраняем прогресс после каждого успешного объекта
                                save_progress(processed_ids, failed_ids)

                                # Сбрасываем счетчик ошибок при успехе
                                error_count = 0
                                error_reason = None  # Сбрасываем причину ошибки при успехе
                                obj_processed = True  # Успешно обработан, выходим из while
                            else:
                                print(f"❌ Не удалось сохранить данные объекта {obj_id}")
                                error_reason = "Не удалось сохранить данные в MongoDB"
                                error_count += 1
                                # Если это последняя попытка, добавляем в ошибки
                                if retry_obj >= max_retries_obj - 1:
                                    error_objects = add_error_object(error_objects, obj, error_reason)
                                    save_error_objects(error_objects)

                        except Exception as e:
                            print(f"❌ Ошибка при сохранении в MongoDB: {e}")
                            error_reason = f"Ошибка сохранения в MongoDB: {str(e)}"
                            error_count += 1
                            # Если это последняя попытка, добавляем в ошибки
                            if retry_obj >= max_retries_obj - 1:
                                error_objects = add_error_object(error_objects, obj, error_reason)
                                save_error_objects(error_objects)
                    else:
                        print(f"❌ Не удалось извлечь данные для объекта {obj_id}")
                        if not error_reason:
                            error_reason = "Не удалось извлечь данные (пустой результат)"
                        error_objects = add_error_object(error_objects, obj, error_reason)
                        save_error_objects(error_objects)
                        error_count += 1
                        break  # Выходим из while, переходим к следующему объекту

                except Exception as e:
                    retry_obj += 1
                    error_message = str(e)
                    error_reason = f"Исключение: {error_message}"
                    print(f"Ошибка при работе с объектом {obj_id}: {e} (попытка {retry_obj}/{max_retries_obj})")

                    # Проверяем, является ли это ошибкой прокси или соединения
                    connection_errors = [
                        "ERR_PROXY_CONNECTION_FAILED",
                        "ERR_CONNECTION_CLOSED", 
                        "ERR_CONNECTION_REFUSED",
                        "ERR_CONNECTION_RESET",
                        "ERR_CONNECTION_ABORTED",
                        "PROXY",
                        "CONNECTION_CLOSED"
                    ]
                    
                    if any(err in error_message for err in connection_errors):
                        if retry_obj < max_retries_obj:
                            print(f"🔌 Обнаружена ошибка подключения/прокси! Перезапускаем браузер...")
                            try:
                                await browser.close()
                            except:
                                pass
                            await asyncio.sleep(2)
                            browser, page = await setup_stealth_browser()
                            print(f"🔄 Браузер перезапущен с новым прокси, повторяем объект {obj_id}")
                            error_count += 1
                            continue  # Повторяем while для того же объекта
                        else:
                            print(f"❌ Исчерпаны попытки для объекта {obj_id}")
                            error_objects = add_error_object(error_objects, obj, error_reason)
                            save_error_objects(error_objects)
                            break

                    error_count += 1
                    error_objects = add_error_object(error_objects, obj, error_reason)
                    save_error_objects(error_objects)
                    break  # При других ошибках переходим к следующему объекту

            # Перезапускаем браузер при накоплении ошибок
            if error_count >= 10:
                print(f"🚨 Накоплено {error_count} ошибок, перезапускаем браузер...")
                try:
                    await browser.close()
                except:
                    pass
                await asyncio.sleep(3)
                browser, page = await setup_stealth_browser()
                print("Браузер перезапущен из-за накопления ошибок")
                error_count = 0

            # Пауза между объектами
            await asyncio.sleep(random.uniform(2, 5))

    except Exception as e:
        print(f"Критическая ошибка: {e}")
        if browser:
            try:
                await browser.close()
            except:
                pass
    finally:
        if browser:
            try:
                await browser.close()
            except:
                pass

    return error_objects


async def process_objects():
    """Основная функция обработки объектов"""
    # Загружаем JSON файл с объектами
    if not os.path.exists(INPUT_JSON):
        print(f"Файл {INPUT_JSON} не найден!")
        return

    try:
        collection = get_collection()
        with open(INPUT_JSON, 'r', encoding='utf-8') as f:
            objects = json.load(f)
        print(f"Загружено {len(objects)} объектов из JSON файла")
    except Exception as e:
        print(f"Ошибка при загрузке JSON файла: {e}")
        return

    # Загружаем прогресс
    progress = load_progress()
    processed_ids = set(progress.get('processed_ids', []))
    failed_ids = set(progress.get('failed_ids', []))

    # Получаем список объектов для обработки
    objects_to_process = []
    for obj in objects:
        obj_id = obj.get('objId')
        if obj_id and obj_id not in processed_ids and obj_id not in failed_ids:
            objects_to_process.append(obj)

    print(f"Найдено {len(objects_to_process)} объектов для обработки")

    if not objects_to_process:
        print("Все объекты уже обработаны")
        return

    # Первый проход - обработка основного списка объектов
    print("\n" + "="*80)
    print("🚀 ПЕРВЫЙ ПРОХОД - обработка основного списка объектов")
    print("="*80 + "\n")
    error_objects = await process_objects_batch(objects_to_process, collection, processed_ids, failed_ids, is_retry=False)

    # Финальное сохранение прогресса после первого прохода
    save_progress(list(processed_ids), list(failed_ids))

    # Второй проход - повторная обработка ошибочных объектов
    if error_objects:
        print("\n" + "="*80)
        print(f"🔄 ВТОРОЙ ПРОХОД - повторная обработка {len(error_objects)} ошибочных объектов")
        print("="*80 + "\n")
        
        # Извлекаем полные объекты из ошибочных записей
        retry_objects = [error_obj['full_object'] for error_obj in error_objects]
        
        # Повторно обрабатываем ошибочные объекты
        remaining_errors = await process_objects_batch(retry_objects, collection, processed_ids, failed_ids, is_retry=True)
        
        # Сохраняем оставшиеся ошибки
        if remaining_errors:
            print(f"\n⚠️  После повторной обработки осталось {len(remaining_errors)} ошибочных объектов")
            save_error_objects(remaining_errors)
        else:
            print(f"\n✅ Все ошибочные объекты успешно обработаны при повторной попытке!")
            # Очищаем файл с ошибками
            if os.path.exists(ERROR_OBJECTS_FILE):
                os.remove(ERROR_OBJECTS_FILE)
                print("🗑️  Файл с ошибочными объектами удален")
    else:
        print("\n✅ Ошибочных объектов не обнаружено!")

    # Финальное сохранение прогресса
    save_progress(list(processed_ids), list(failed_ids))

    print(f"\n" + "="*80)
    print(f"✅ ОБРАБОТКА ЗАВЕРШЕНА!")
    print(f"="*80)
    print(f"Успешно обработано: {len(processed_ids)}")
    print(f"Ошибок: {len(failed_ids)}")
    print(f"Всего в JSON файле: {len(objects)} объектов")


def main():
    """Главная функция"""
    print("🚀 Запуск скрипта извлечения деталей объектов...")
    asyncio.get_event_loop().run_until_complete(process_objects())


if __name__ == '__main__':
    main()
