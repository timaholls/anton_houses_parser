#!/usr/bin/env python3
"""
Повторная дедупликация DomRF коллекции с улучшенным алгоритмом нормализации.
Обновляет normalized_name для всех записей и удаляет дубликаты.
"""
import os
import re
from pathlib import Path
from typing import List, Dict, Any

from dotenv import load_dotenv
from pymongo import MongoClient
from pymongo.errors import PyMongoError, DuplicateKeyError


PROJECT_ROOT = Path(__file__).resolve().parent


def load_domrf_env() -> Dict[str, str]:
    env_path = PROJECT_ROOT / 'domrf' / '.env'
    load_dotenv(dotenv_path=env_path)
    return {
        'DB_NAME': os.getenv('DB_NAME'),
        'COLLECTION_NAME': os.getenv('COLLECTION_NAME'),
        'MONGO_URI': os.getenv('MONGO_URI'),
    }


def transliterate_russian_to_latin(text: str) -> str:
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

    result = ''
    for char in text:
        result += translit_dict.get(char, char)
    return result


def normalize_name(name: str) -> str:
    """Улучшенная нормализация названий ЖК"""
    if not name:
        return ''

    normalized = name.lower()
    normalized = re.sub(r'\([^)]*\)', '', normalized)
    normalized = re.sub(r'[^\w\s]', '', normalized)
    normalized = re.sub(r'\s+', ' ', normalized).strip()

    common_words = ['жк', 'жилой', 'комплекс', 'дома', 'квартиры', 'поселок',
                    'литер', 'литера', 'секции', 'этап', 'очередь',
                    'клубный', 'микрорайон', 'красочный',
                    'апартаментов', 'апартаменты', 'высотных', 'экогород',
                    'клубная', 'резиденция', 'группа', 'компаний', 'комплекса']

    significant_words = {'village', 'виллидж', 'park', 'парк', 'city', 'сити',
                         'town', 'таун', 'garden', 'гарден', 'house', 'хаус',
                         'collection', 'коллекшн', 'квартал', 'premiere', 'премьер',
                         'умный', 'smart', 'дом', 'the', 'prime'}

    for word in common_words:
        if word not in significant_words:
            normalized = re.sub(r'\b' + word + r'\b', '', normalized)

    # Улучшенная логика фильтрации слов
    words = normalized.split()
    filtered_words = []
    for word in words:
        # Пропускаем слова, которые являются номерами литеров/секций/этапов
        if (word.isdigit() or  # Одиночные цифры
            (len(word) <= 3 and word.isalpha() and word not in significant_words) or  # Короткие буквы
            word in ['литер', 'литера', 'секции', 'секция', 'этап', 'очередь', 'паркинг']):  # Служебные слова
            continue
        filtered_words.append(word)

    normalized = ' '.join(filtered_words)
    normalized = re.sub(r'\s+', ' ', normalized).strip()

    transliterated = transliterate_russian_to_latin(normalized)
    if normalized != transliterated:
        return f"{normalized} {transliterated}"
    return normalized


def drop_unique_index(collection):
    """Удаляет уникальный индекс на normalized_name если он существует"""
    try:
        collection.drop_index('normalized_name_1')
        print('🗑️ Удален уникальный индекс на normalized_name')
    except PyMongoError:
        print('ℹ️ Индекс на normalized_name не найден или уже удален')


def update_normalized_names(collection) -> int:
    """Обновляет normalized_name для всех документов"""
    updated = 0
    cursor = collection.find({}, {'objCommercNm': 1, 'normalized_name': 1, '_id': 1})
    
    for doc in cursor:
        obj_name = doc.get('objCommercNm')
        if not obj_name:
            continue
            
        new_normalized = normalize_name(obj_name)
        old_normalized = doc.get('normalized_name')
        
        if old_normalized != new_normalized:
            collection.update_one({'_id': doc['_id']}, {'$set': {'normalized_name': new_normalized}})
            updated += 1
            print(f"🔄 Обновлено: '{obj_name}' → '{new_normalized}'")
    
    return updated


def remove_duplicates(collection) -> Dict[str, int]:
    """Удаляет дубликаты по normalized_name"""
    removed = 0
    kept = 0
    
    # Находим группы дубликатов
    pipeline = [
        {'$group': {
            '_id': '$normalized_name', 
            'ids': {'$push': '$_id'}, 
            'count': {'$sum': 1},
            'names': {'$push': '$objCommercNm'}
        }},
        {'$match': {'count': {'$gt': 1}, '_id': {'$ne': None}}}
    ]
    
    for group in collection.aggregate(pipeline):
        ids = group['ids']
        names = group['names']
        normalized = group['_id']
        
        if not ids:
            continue
            
        print(f"\n📋 Найдены дубликаты для '{normalized}':")
        for i, name in enumerate(names):
            print(f"  {i+1}. {name}")
        
        # Оставляем первый документ, удаляем остальные
        keep_id = ids[0]
        to_delete = ids[1:]
        
        if to_delete:
            result = collection.delete_many({'_id': {'$in': to_delete}})
            removed += result.deleted_count
            kept += 1
            print(f"  ✅ Оставлен: {names[0]}")
            print(f"  ❌ Удалено: {result.deleted_count} дубликатов")
    
    return {'removed': removed, 'kept': kept}


def create_unique_index(collection):
    """Создает уникальный индекс на normalized_name"""
    try:
        collection.create_index('normalized_name', unique=True, sparse=True)
        print('✅ Создан уникальный индекс на поле normalized_name')
    except DuplicateKeyError:
        print('⚠️ Невозможно создать уникальный индекс: найдены дубликаты')
    except PyMongoError as e:
        print(f'⚠️ Ошибка создания индекса: {e}')


def main():
    print("🔄 ПОВТОРНАЯ ДЕДУПЛИКАЦИЯ DOMRF")
    print("="*60)
    
    cfg = load_domrf_env()
    client = MongoClient(cfg['MONGO_URI'])
    db = client[cfg['DB_NAME']]
    collection = db[cfg['COLLECTION_NAME']]

    print(f"📊 Подключились к базе: {cfg['DB_NAME']}")
    print(f"📊 Коллекция: {cfg['COLLECTION_NAME']}")
    print(f"📊 Всего документов: {collection.count_documents({})}")
    
    print('\n🗑️ Удаляем уникальный индекс...')
    drop_unique_index(collection)
    
    print('\n📥 Обновляем normalized_name для всех документов...')
    updated = update_normalized_names(collection)
    print(f'✅ Обновлено документов: {updated}')

    print('\n🧹 Удаляем дубликаты по normalized_name...')
    stats = remove_duplicates(collection)
    print(f"\n✅ Оставлено уникальных: {stats['kept']}")
    print(f"✅ Удалено дубликатов: {stats['removed']}")

    print('\n🔒 Создаем уникальный индекс...')
    create_unique_index(collection)
    
    print(f'\n📊 Финальное количество документов: {collection.count_documents({})}')

    client.close()
    print('\n🎉 Готово!')


if __name__ == '__main__':
    main()
