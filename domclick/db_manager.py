#!/usr/bin/env python3
"""
Модуль для работы с MongoDB
Содержит функции для подключения, сохранения и обновления данных
"""
import os
from pathlib import Path
from datetime import datetime
from typing import Dict, List, Optional
from pymongo import MongoClient
from dotenv import load_dotenv
from urllib.parse import urlparse

# Директория текущего скрипта
PROJECT_ROOT = Path(__file__).resolve().parent

load_dotenv(dotenv_path=PROJECT_ROOT / ".env")
# MongoDB настройки
MONGO_URI = os.getenv("MONGO_URI")
DB_NAME = os.getenv("DB_NAME")
COLLECTION_NAME = os.getenv("COLLECTION_NAME")


def get_mongo_client():
    """Создает подключение к MongoDB"""
    try:
        client = MongoClient(MONGO_URI)
        # Проверяем подключение
        client.admin.command('ping')
        print(f"✅ Подключение к MongoDB успешно: {MONGO_URI}")
        return client
    except Exception as e:
        print(f"❌ Ошибка подключения к MongoDB: {e}")
        return None


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


def find_existing_record(collection, url: str):
    """
    Ищет существующую запись по URL, учитывая разные варианты доменов.
    Ищет по нормализованному URL и по slug комплекса.
    """
    if not url:
        return None
    
    # Нормализуем URL
    normalized_url = normalize_complex_url(url)
    
    # Сначала ищем по точному совпадению нормализованного URL
    existing = collection.find_one({'url': normalized_url})
    if existing:
        return existing
    
    # Если не нашли, ищем по исходному URL
    if url != normalized_url:
        existing = collection.find_one({'url': url})
        if existing:
            return existing
    
    # Если не нашли, пытаемся найти по slug комплекса
    try:
        parsed = urlparse(normalized_url)
        path_parts = parsed.path.split('/')
        if 'complexes' in path_parts:
            complex_index = path_parts.index('complexes')
            if complex_index + 1 < len(path_parts):
                slug = path_parts[complex_index + 1]
                # Ищем записи, где URL содержит этот slug
                existing = collection.find_one({
                    'url': {'$regex': f'/complexes/{slug}'}
                })
                if existing:
                    return existing
    except Exception:
        pass
    
    return None


def compare_and_merge_data(existing_data, new_data):
    """Сравнивает и объединяет данные, обновляя только отличающиеся части"""
    if not existing_data:
        return new_data, []
    
    merged = existing_data.copy()
    changes = []
    
    # Обновляем development только если есть новые данные
    if 'development' in new_data and new_data['development']:
        for key, value in new_data['development'].items():
            if value:  # Только если значение не пустое
                if key not in merged.get('development', {}) or merged['development'][key] != value:
                    if 'development' not in merged:
                        merged['development'] = {}
                    merged['development'][key] = value
                    changes.append(f"development.{key}")
    
    # Обновляем apartment_types - сравниваем по типу квартиры
    if 'apartment_types' in new_data and new_data['apartment_types']:
        if 'apartment_types' not in merged:
            merged['apartment_types'] = {}
        
        for apt_type, apt_data in new_data['apartment_types'].items():
            if apt_data and 'apartments' in apt_data:  # Только если есть данные квартир
                old_apartments = merged['apartment_types'].get(apt_type, {}).get('apartments', [])
                new_apartments = apt_data['apartments']
                
                # Сравниваем не только количество, но и содержимое (особенно пути к фото)
                apartments_changed = False
                if len(old_apartments) != len(new_apartments):
                    apartments_changed = True
                else:
                    # Проверяем изменения в фотографиях квартир
                    for old_apt, new_apt in zip(old_apartments, new_apartments):
                        old_photos = old_apt.get('photos', [])
                        new_photos = new_apt.get('photos', [])
                        if old_photos != new_photos:
                            apartments_changed = True
                            break
                
                if apt_type not in merged['apartment_types'] or apartments_changed:
                    merged['apartment_types'][apt_type] = apt_data
                    old_count = len(old_apartments)
                    new_count = len(new_apartments)
                    changes.append(f"apartment_types.{apt_type} ({old_count} → {new_count} квартир)")
    
    # Обновляем total_apartments
    if 'apartment_types' in merged:
        merged['total_apartments'] = sum(
            len(apt_data.get('apartments', [])) 
            for apt_data in merged['apartment_types'].values()
        )
    
    # Обновляем scraped_at
    merged['scraped_at'] = new_data.get('scraped_at', datetime.now().isoformat())
    
    return merged, changes


def save_to_mongodb(data):
    """Умное сохранение с поиском по URL и обновлением только изменений"""
    try:
        client = get_mongo_client()
        if not client:
            return False
            
        db = client[DB_NAME]
        collection = db[COLLECTION_NAME]
        
        for item in data:
            url = item.get('url')
            if not url:
                print("⚠️ URL не найден в данных, пропускаем")
                continue
            
            # Нормализуем URL перед сохранением
            normalized_url = normalize_complex_url(url)
            item['url'] = normalized_url  # Сохраняем нормализованный URL
            
            # Ищем существующую запись по URL (с учетом разных вариантов доменов)
            existing = find_existing_record(collection, normalized_url)
            
            if existing:
                existing_url = existing.get('url', '')
                print(f"📝 Найдена существующая запись для: {existing_url} (искали: {normalized_url})")
                
                # Если URL в существующей записи отличается, обновляем его на нормализованный
                if existing_url != normalized_url:
                    print(f"  Обновляю URL с {existing_url} на {normalized_url}")
                
                # Сравниваем и объединяем данные
                merged_data, changes = compare_and_merge_data(existing, item)
                
                # Убеждаемся, что URL нормализован
                merged_data['url'] = normalized_url
                
                if changes:
                    print(f"🔄 Обнаружены изменения:")
                    for change in changes:
                        print(f"   - {change}")
                    
                    # Обновляем запись по _id существующей записи
                    collection.update_one(
                        {'_id': existing['_id']},
                        {'$set': merged_data}
                    )
                    print(f"✅ Запись обновлена")
                else:
                    # Даже если нет изменений, обновляем URL если он отличается
                    if existing_url != normalized_url:
                        collection.update_one(
                            {'_id': existing['_id']},
                            {'$set': {'url': normalized_url}}
                        )
                        print(f"✅ URL обновлен на нормализованный")
                    else:
                        print(f"ℹ️ Нет изменений, запись не обновлена")
            else:
                print(f"➕ Создаем новую запись для: {normalized_url}")
                # Удаляем _id если он есть
                if '_id' in item:
                    del item['_id']
                collection.insert_one(item)
                print(f"✅ Новая запись создана")
        
        client.close()
        return True
    except Exception as e:
        print(f"❌ Ошибка сохранения в MongoDB: {e}")
        return False

