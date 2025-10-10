import os
import re
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


def transliterate_russian_to_latin(text: str) -> str:
    """Транслитерирует русский текст в латиницу"""
    translit_dict = {
        'а': 'a', 'б': 'b', 'в': 'v', 'г': 'g', 'д': 'd', 'е': 'e', 'ё': 'yo',
        'ж': 'zh', 'з': 'z', 'и': 'i', 'й': 'y', 'к': 'k', 'л': 'l', 'м': 'm',
        'н': 'n', 'о': 'o', 'п': 'p', 'р': 'r', 'с': 's', 'т': 't', 'у': 'u',
        'ф': 'f', 'х': 'h', 'ц': 'ts', 'ч': 'ch', 'ш': 'sh', 'щ': 'sch',
        'ъ': '', 'ы': 'y', 'ь': '', 'э': 'e', 'ю': 'yu', 'я': 'ya',
        'А': 'A', 'Б': 'B', 'В': 'V', 'Г': 'G', 'Д': 'D', 'Е': 'E', 'Ё': 'Yo',
        'Ж': 'Zh', 'З': 'Z', 'И': 'I', 'Й': 'Y', 'К': 'K', 'Л': 'L', 'М': 'M',
        'Н': 'N', 'О': 'O', 'П': 'P', 'Р': 'R', 'С': 'S', 'Т': 'T', 'У': 'U',
        'Ф': 'F', 'Х': 'H', 'Ц': 'Ts', 'Ч': 'Ch', 'Ш': 'Sh', 'Щ': 'Sch',
        'Ъ': '', 'Ы': 'Y', 'Ь': '', 'Э': 'E', 'Ю': 'Yu', 'Я': 'Ya'
    }
    
    result = ""
    for char in text:
        result += translit_dict.get(char, char)
    return result


def normalize_name(name: str) -> str:
    """Нормализует название ЖК для поиска с поддержкой транслитерации"""
    if not name:
        return ""
    
    # Приводим к нижнему регистру
    normalized = name.lower()
    
    # Убираем содержимое в скобках (часто там транслитерации)
    normalized = re.sub(r'\([^)]*\)', '', normalized)
    
    # Убираем лишние символы и пробелы
    normalized = re.sub(r'[^\w\s]', '', normalized)
    normalized = re.sub(r'\s+', ' ', normalized).strip()
    
    # Убираем общие слова (но НЕ убираем значимые слова типа "village", "park")
    common_words = ['жк', 'жилой', 'комплекс', 'дома', 'квартиры', 'поселок',
                   'литер', 'литера', 'секции', 'этап', 'очередь',
                   'клубный', 'микрорайон', 'красочный',
                   'апартаментов', 'апартаменты', 'высотных', 'экогород',
                   'клубная', 'резиденция', 'группа', 'компаний', 'комплекса']
    
    # Значимые слова, которые НЕ должны удаляться
    significant_words = {'village', 'виллидж', 'park', 'парк', 'city', 'сити', 
                        'town', 'таун', 'garden', 'гарден', 'house', 'хаус',
                        'collection', 'коллекшн', 'квартал', 'premiere', 'премьер',
                        'умный', 'smart', 'дом', 'the', 'prime'}
    
    for word in common_words:
        # Убираем слово только если оно отдельное (не часть другого слова) и не значимое
        if word not in significant_words:
            normalized = re.sub(r'\b' + word + r'\b', '', normalized)
    
    # Убираем отдельно стоящие цифры и короткие буквы (номера литеров, этапов, секций)
    # Но оставляем цифры, которые являются частью названия (8 марта, 535)
    words = normalized.split()
    filtered_words = []
    for i, word in enumerate(words):
        # Пропускаем слова, которые являются номерами литеров/секций/этапов
        if (word.isdigit() or  # Одиночные цифры
            (len(word) <= 3 and word.isalpha() and word not in significant_words) or  # Короткие буквы
            word in ['литер', 'литера', 'секции', 'секция', 'этап', 'очередь', 'паркинг']):  # Служебные слова
            continue
        filtered_words.append(word)
    
    normalized = ' '.join(filtered_words)
    
    # Убираем лишние пробелы после удаления слов
    normalized = re.sub(r'\s+', ' ', normalized).strip()
    
    # Транслитерируем русский текст в латиницу для лучшего сопоставления
    transliterated = transliterate_russian_to_latin(normalized)
    
    # Возвращаем оба варианта через пробел для сравнения
    if normalized != transliterated:
        return f"{normalized} {transliterated}"
    else:
        return normalized


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
    1. Добавляет нормализованное название в данные
    2. Проверяет, есть ли запись с таким же normalized_name
    3. Если есть - не сохраняет дубликат (этапы одного ЖК)
    4. Если нет - ищет запись по objId
    5. Если не нашла - создает новую
    6. Если нашла - сравнивает данные и обновляет только отличающиеся блоки
    7. Сохраняет старые данные, если новые не были собраны
    
    Args:
        collection: Коллекция MongoDB
        obj_id: Идентификатор объекта (URL/objId)
        new_data: Новые данные для сохранения
        
    Returns:
        True если данные были обновлены, False если произошла ошибка
    """
    try:
        # Получаем название ЖК из новых данных
        obj_commerc_nm = new_data.get('objCommercNm')
        
        # Добавляем нормализованное название в данные
        if obj_commerc_nm:
            normalized_name = normalize_name(obj_commerc_nm)
            new_data['normalized_name'] = normalized_name
            
            # Проверяем дубликаты по нормализованному названию
            existing_by_normalized = collection.find_one({
                'normalized_name': normalized_name,
                'objId': {'$ne': obj_id}
            })
            
            if existing_by_normalized:
                print(f"⚠️  Найден дубликат по нормализованному названию '{normalized_name}' (objId: {existing_by_normalized.get('objId')}, название: '{existing_by_normalized.get('objCommercNm')}'). Пропускаем сохранение объекта {obj_id} ('{obj_commerc_nm}')")
                return False

        # Ищем существующую запись по objId
        existing_record = collection.find_one({'objId': obj_id})

        if existing_record is None:
            # Записи нет - создаем новую
            print(f"📝 Создаем новую запись для объекта {obj_id} ('{obj_commerc_nm}')")
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
