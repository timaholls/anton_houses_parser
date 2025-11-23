#!/usr/bin/env python3
"""
Модуль для работы с MongoDB
Содержит функции для подключения, сохранения и обновления данных
"""
import os
import re
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


def extract_slug_from_url(url: Optional[str]) -> Optional[str]:
    """Возвращает slug комплекса из URL."""
    if not url:
        return None
    try:
        parsed = urlparse(url)
        parts = [part for part in parsed.path.split('/') if part]
        if 'complexes' in parts:
            idx = parts.index('complexes')
            if idx + 1 < len(parts):
                return parts[idx + 1]
    except Exception:
        pass
    return None


def normalize_complex_url(url: str) -> str:
    """
    Нормализует URL комплекса, приводя к единому формату.
    Всегда использует ufa.domclick.ru для единообразия.
    """
    slug = extract_slug_from_url(url)
    if slug:
        return f"https://ufa.domclick.ru/complexes/{slug}"
    return url


def normalize_complex_name(name: Optional[str]) -> Optional[str]:
    if not name:
        return None
    return re.sub(r'\s+', ' ', name).strip().lower()


def find_existing_record(collection, url: str, complex_name: Optional[str] = None):
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
    slug = extract_slug_from_url(normalized_url)
    if slug:
        existing = collection.find_one({
            'url': {'$regex': f'/complexes/{re.escape(slug)}$', '$options': 'i'}
        })
        if existing:
            return existing
    
    # Ищем по slug в оригинальном URL (для совместимости)
    if slug:
        existing = collection.find_one({
            'url': {'$regex': re.escape(slug), '$options': 'i'}
        })
        if existing:
            return existing
    
    # Ищем по названию комплекса
    normalized_name = normalize_complex_name(complex_name)
    if normalized_name:
        existing = collection.find_one({'normalized_complex_name': normalized_name})
        if existing:
            return existing
        existing = collection.find_one({
            'development.complex_name': {'$regex': f'^{re.escape(complex_name)}$', '$options': 'i'}
        })
        if existing:
            return existing
    
    return None


def compare_and_merge_data(existing_data, new_data):
    """Сравнивает и объединяет данные, обновляя только отличающиеся части"""
    if not existing_data:
        return new_data, []
    
    merged = existing_data.copy()
    changes = []
    
    # Обновляем координаты (корневые поля)
    for coord_field in ("latitude", "longitude"):
        new_value = new_data.get(coord_field)
        if new_value not in (None, "", []):
            if existing_data.get(coord_field) != new_value:
                merged[coord_field] = new_value
                changes.append(coord_field)

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
                
                # Сравниваем не только количество, но и все поля каждой квартиры
                apartments_changed = False
                
                # Создаем словарь старых квартир по title для быстрого поиска
                old_apts_by_title = {apt.get('title', ''): apt for apt in old_apartments}
                
                # Проверяем изменения во всех полях каждой квартиры
                for new_apt in new_apartments:
                    new_title = new_apt.get('title', '')
                    old_apt = old_apts_by_title.get(new_title)
                    
                    # Если квартиры с таким title нет в старых данных - есть изменения
                    if not old_apt:
                        apartments_changed = True
                        break
                    
                    # Список полей для сравнения
                    fields_to_check = [
                        'title', 'photos', 'area', 'totalArea', 
                        'price', 'pricePerSquare', 'completionDate', 'url'
                    ]
                    
                    for field in fields_to_check:
                        old_value = old_apt.get(field)
                        new_value = new_apt.get(field)
                        
                        # Если поле отсутствует в старых данных, но есть в новых (и не пустое) - это изменение
                        if field not in old_apt and new_value not in (None, '', []):
                            apartments_changed = True
                            break
                        
                        # Нормализуем значения для сравнения (пустые строки и None считаем одинаковыми)
                        old_normalized = old_value if old_value not in (None, '') else None
                        new_normalized = new_value if new_value not in (None, '') else None
                        
                        # Для списков (photos) сравниваем содержимое
                        if field == 'photos':
                            old_list = old_value if isinstance(old_value, list) else []
                            new_list = new_value if isinstance(new_value, list) else []
                            if old_list != new_list:
                                apartments_changed = True
                                break
                        # Для чисел сравниваем с учетом None
                        elif field in ('totalArea',):
                            if old_normalized != new_normalized:
                                apartments_changed = True
                                break
                        # Для строк сравниваем значения
                        else:
                            if old_normalized != new_normalized:
                                apartments_changed = True
                                break
                    
                    if apartments_changed:
                        break
                
                # Также проверяем, не удалились ли какие-то квартиры
                if not apartments_changed and len(old_apartments) != len(new_apartments):
                    apartments_changed = True
                
                if apt_type not in merged['apartment_types'] or apartments_changed:
                    merged['apartment_types'][apt_type] = apt_data
                    old_count = len(old_apartments)
                    new_count = len(new_apartments)
                    if apartments_changed:
                        changes.append(f"apartment_types.{apt_type} ({old_count} → {new_count} квартир, данные обновлены)")
                    else:
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
            complex_name = item.get('development', {}).get('complex_name')
            normalized_name = None
            if complex_name:
                normalized_name = normalize_complex_name(complex_name)
                if normalized_name:
                    item['normalized_complex_name'] = normalized_name
            
            existing = find_existing_record(collection, normalized_url, complex_name)
            
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
                
                if normalized_name:
                    merged_data['normalized_complex_name'] = normalized_name
                elif 'normalized_complex_name' in existing:
                    merged_data['normalized_complex_name'] = existing['normalized_complex_name']
                
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

