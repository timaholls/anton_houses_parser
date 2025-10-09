import os
from pathlib import Path
from pymongo import MongoClient
from pymongo.errors import PyMongoError
from typing import Dict, Any, Optional
from dotenv import load_dotenv

# Директория текущего скрипта
PROJECT_ROOT = Path(__file__).resolve().parent

# Загружаем переменные окружения
load_dotenv(dotenv_path=PROJECT_ROOT / ".env")
DB_NAME = os.getenv("DB_NAME")
COLLECTION_NAME = os.getenv("COLLECTION_NAME")
MONGO_URI = os.getenv("MONGO_URI")


def get_collection():
    """Подключается к существующей коллекции MongoDB.
    Возвращает объект коллекции.
    """
    client = MongoClient(MONGO_URI, appname="domrf-parser")
    db = client[DB_NAME]
    collection = db[COLLECTION_NAME]
    return collection


def compare_and_merge_data(existing_data: Dict[str, Any], new_data: Dict[str, Any]) -> Dict[str, Any]:
    """
    Сравнивает и объединяет данные. 
    
    Логика:
    - Если в new_data есть значимые данные - используем их
    - Если в new_data нет данных (пустые списки/словари/None), оставляем старые из existing_data
    - Сравниваем блоки и обновляем только отличающиеся
    
    Args:
        existing_data: Существующие данные из базы
        new_data: Новые собранные данные
        
    Returns:
        Объединенные данные
    """
    merged = existing_data.copy()

    for key, new_value in new_data.items():
        # Если ключа не было в старых данных - добавляем
        if key not in existing_data:
            merged[key] = new_value
            continue

        old_value = existing_data[key]

        # Проверяем, является ли новое значение "пустым"
        is_empty_new = (
                new_value is None or
                (isinstance(new_value, (list, dict, str)) and not new_value)
        )

        # Если новое значение пустое - оставляем старое
        if is_empty_new:
            merged[key] = old_value
            continue

        # Если новое значение не пустое и отличается от старого - обновляем
        if isinstance(new_value, dict) and isinstance(old_value, dict):
            # Рекурсивно сравниваем словари
            merged[key] = compare_and_merge_data(old_value, new_value)
        elif isinstance(new_value, list) and isinstance(old_value, list):
            # Для списков: если новый список не пустой и отличается - заменяем
            if new_value != old_value:
                merged[key] = new_value
            else:
                merged[key] = old_value
        else:
            # Для примитивных типов: если отличается - обновляем
            if new_value != old_value:
                merged[key] = new_value
            else:
                merged[key] = old_value

    return merged


def upsert_object_smart(collection, obj_id: str, new_data: Dict[str, Any]) -> bool:
    """
    Умное обновление/создание записи в MongoDB.
    
    Логика:
    1. Ищет запись по objId
    2. Если не нашла - создает новую
    3. Если нашла - сравнивает данные и обновляет только отличающиеся блоки
    4. Сохраняет старые данные, если новые не были собраны
    
    Args:
        collection: Коллекция MongoDB
        obj_id: Идентификатор объекта (URL/objId)
        new_data: Новые данные для сохранения
        
    Returns:
        True если данные были обновлены, False если произошла ошибка
    """
    try:
        # Ищем существующую запись по objId
        existing_record = collection.find_one({'objId': obj_id})

        if existing_record is None:
            # Записи нет - создаем новую
            print(f"📝 Создаем новую запись для объекта {obj_id}")
            collection.insert_one(new_data)
            return True

        # Запись найдена - сравниваем и объединяем данные
        print(f"🔄 Обновляем существующую запись для объекта {obj_id}")

        # Убираем _id из existing_record для корректного сравнения
        existing_data = {k: v for k, v in existing_record.items() if k != '_id'}

        # Объединяем данные
        merged_data = compare_and_merge_data(existing_data, new_data)

        # Проверяем, изменились ли данные
        if merged_data == existing_data:
            print(f"ℹ️  Данные для объекта {obj_id} не изменились")
            return True

        # Обновляем запись
        result = collection.update_one(
            {'objId': obj_id},
            {'$set': merged_data}
        )

        if result.modified_count > 0:
            print(f"✅ Данные для объекта {obj_id} обновлены")

        return True

    except Exception as e:
        print(f"❌ Ошибка при сохранении данных для объекта {obj_id}: {e}")
        return False
