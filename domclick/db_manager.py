#!/usr/bin/env python3
"""
Модуль для работы с MongoDB
Содержит функции для подключения, сохранения и обновления данных
"""
import os
from pathlib import Path
from datetime import datetime
from typing import Dict, List
from pymongo import MongoClient
from dotenv import load_dotenv

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
                old_count = len(merged['apartment_types'].get(apt_type, {}).get('apartments', []))
                new_count = len(apt_data['apartments'])
                
                if apt_type not in merged['apartment_types'] or old_count != new_count:
                    merged['apartment_types'][apt_type] = apt_data
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
            
            # Ищем существующую запись по URL
            existing = collection.find_one({'url': url})
            
            if existing:
                print(f"📝 Найдена существующая запись для: {url}")
                
                # Сравниваем и объединяем данные
                merged_data, changes = compare_and_merge_data(existing, item)
                
                if changes:
                    print(f"🔄 Обнаружены изменения:")
                    for change in changes:
                        print(f"   - {change}")
                    
                    # Обновляем запись
                    collection.update_one(
                        {'url': url},
                        {'$set': merged_data}
                    )
                    print(f"✅ Запись обновлена")
                else:
                    print(f"ℹ️ Нет изменений, запись не обновлена")
            else:
                print(f"➕ Создаем новую запись для: {url}")
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

