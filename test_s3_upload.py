#!/usr/bin/env python3
"""
Тестовый скрипт для проверки загрузки изображения в S3.
Использование:
    python test_s3_upload.py <путь_к_локальному_изображению>
"""
import sys
import logging
from pathlib import Path
from io import BytesIO

from s3_service import S3Service
from domclick.resize_img import ImageProcessor

# Настройка логгера
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
handler = logging.StreamHandler()
handler.setFormatter(logging.Formatter('%(message)s'))
logger.addHandler(handler)

# Инициализация обработчика изображений
image_processor = ImageProcessor(logger, max_size=(800, 600), max_kb=150)


def test_upload_image(image_path: str):
    """Тестирует загрузку локального изображения в S3"""
    
    # Проверяем существование файла
    if not Path(image_path).exists():
        print(f"❌ Файл не найден: {image_path}")
        return
    
    print(f"📂 Загружаем локальное изображение: {image_path}")
    
    try:
        # Читаем локальное изображение
        with open(image_path, 'rb') as f:
            raw_bytes = f.read()
        
        print(f"✅ Файл прочитан: {len(raw_bytes)} байт")
        
        # Обрабатываем через resize_img.py
        print("🔧 Обработка через resize_img.py...")
        input_bytes = BytesIO(raw_bytes)
        processed = image_processor.process(input_bytes)
        
        if not processed:
            print("❌ Ошибка обработки изображения")
            return
        
        processed.seek(0)
        processed_data = processed.read()
        print(f"✅ Изображение обработано: {len(processed_data)} байт")
        
        # Загружаем в S3
        print("☁️  Загружаем в S3...")
        s3 = S3Service()
        
        # Формируем ключ для тестового изображения
        filename = Path(image_path).name
        test_key = f"test_uploads/{filename}"
        
        public_url = s3.upload_bytes(processed_data, test_key, content_type="image/jpeg")
        
        print("\n" + "=" * 60)
        print("✅ УСПЕШНО ЗАГРУЖЕНО В S3!")
        print("=" * 60)
        print(f"📍 S3 Key: {test_key}")
        print(f"🌐 Публичный URL: {public_url}")
        print("=" * 60)
        
        return public_url
        
    except Exception as e:
        print(f"\n❌ Ошибка при загрузке в S3: {e}")
        import traceback
        traceback.print_exc()
        return None


if __name__ == "__main__":
    
    image_path = "/home/art/PycharmProjects/anton_houses/media/complexes/8-nebo__121663/complex_photos/photo_1.jpg"
    test_upload_image(image_path)


