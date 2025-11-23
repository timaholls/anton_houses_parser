#!/usr/bin/env python3
"""
Скрипт для загрузки одной фотографии в S3.

Использование:
    python upload_single_photo.py <URL_фото> [путь_в_s3]

Примеры:
    python upload_single_photo.py https://example.com/photo.jpg
    python upload_single_photo.py https://example.com/photo.jpg cian/apartments/123/main.jpg
"""
import asyncio
import sys
import logging
from pathlib import Path
from io import BytesIO
import aiohttp
import hashlib
from dotenv import load_dotenv

# Директория текущего скрипта
PROJECT_ROOT = Path(__file__).resolve().parent

# Загружаем переменные окружения из .env файла (из корня проекта)
PROJECT_ROOT_PARENT = PROJECT_ROOT.parent
load_dotenv(dotenv_path=PROJECT_ROOT_PARENT / ".env")

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


async def upload_photo(image_url: str, s3_key: str = None) -> str:
    """
    Скачивает фотографию, обрабатывает и загружает в S3.
    
    Args:
        image_url: URL фотографии для скачивания
        s3_key: Путь в S3 (если не указан, будет сгенерирован автоматически)
    
    Returns:
        str: Публичный URL загруженной фотографии
    """
    print(f"Скачиваю фотографию: {image_url}")
    
    # Скачиваем фотографию
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(image_url, timeout=aiohttp.ClientTimeout(total=10)) as response:
                if response.status != 200:
                    raise Exception(f"Ошибка скачивания: HTTP {response.status}")
                image_bytes = await response.read()
                print(f"✓ Скачано: {len(image_bytes)} байт")
    except Exception as e:
        raise Exception(f"Ошибка скачивания фотографии: {e}")
    
    # Обрабатываем изображение
    try:
        print("Обрабатываю изображение...")
        input_bytes = BytesIO(image_bytes)
        processed_bytes = image_processor.process(input_bytes)
        processed_bytes.seek(0)
        processed_data = processed_bytes.read()
        print(f"✓ Обработано: {len(processed_data)} байт")
    except Exception as e:
        raise Exception(f"Ошибка обработки изображения: {e}")
    
    # Генерируем ключ для S3, если не указан
    if not s3_key:
        # Извлекаем имя файла из URL и добавляем хеш для уникальности
        from urllib.parse import urlparse
        parsed_url = urlparse(image_url)
        filename = Path(parsed_url.path).name
        if not filename or '.' not in filename:
            filename = "image"
        # Добавляем хеш URL для уникальности
        url_hash = hashlib.md5(image_url.encode()).hexdigest()[:8]
        # Убираем расширение, если есть, и добавляем .jpg
        filename_base = filename.rsplit('.', 1)[0] if '.' in filename else filename
        s3_key = f"cian/manual_uploads/{filename_base}_{url_hash}.jpg"
    
    # Загружаем в S3
    try:
        print(f"Загружаю в S3: {s3_key}")
        s3 = S3Service()
        url_public = upload_with_watermark(s3, processed_data, s3_key)
        print(f"✓ Загружено успешно!")
        return url_public
    except Exception as e:
        raise Exception(f"Ошибка загрузки в S3: {e}")


async def main():
    """Основная функция."""
    if len(sys.argv) < 2:
        print("Использование: python upload_single_photo.py <URL_фото> [путь_в_s3]")
        print("\nПримеры:")
        print("  python upload_single_photo.py https://example.com/photo.jpg")
        print("  python upload_single_photo.py https://example.com/photo.jpg cian/apartments/123/main.jpg")
        sys.exit(1)
    
    image_url = sys.argv[1]
    s3_key = sys.argv[2] if len(sys.argv) > 2 else None
    
    try:
        result_url = await upload_photo(image_url, s3_key)
        print(f"\n{'='*60}")
        print(f"✓ Успешно загружено!")
        print(f"URL: {result_url}")
        print(f"{'='*60}")
        print(f"\nСкопируйте эту ссылку в JSON файл:")
        print(f'"{result_url}"')
    except Exception as e:
        print(f"\n✗ Ошибка: {e}")
        sys.exit(1)


if __name__ == "__main__":
    asyncio.get_event_loop().run_until_complete(main())

