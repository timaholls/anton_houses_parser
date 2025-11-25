#!/usr/bin/env python3
"""
Скрипт для очистки названий квартир от упоминания ЖК
Удаляет из title часть "в ЖК «...»" и оставляет только основную информацию
"""

import os
import re
from pathlib import Path
from pymongo import MongoClient
from bson import ObjectId
from dotenv import load_dotenv

# Загружаем переменные окружения
PROJECT_ROOT = Path(__file__).resolve().parent
load_dotenv(dotenv_path=PROJECT_ROOT / ".env")

COLLECTION_NAME = "unified_houses_3"
BUILDING_NAME = "ЖК «Зубово Life Garden»"


def get_mongo_connection():
    """Получить подключение к MongoDB"""
    MONGO_URI = os.getenv("MONGO_URI", "mongodb://root:Kfleirb_17@176.98.177.188:27017/admin")
    DB_NAME = os.getenv("DB_NAME", "houses")
    client = MongoClient(MONGO_URI)
    db = client[DB_NAME]
    return db


def normalize_building_name(name: str) -> str:
    """Нормализует название ЖК для поиска"""
    if not name:
        return ""
    cleaned = name.lower()
    cleaned = cleaned.replace("«", "").replace("»", "")
    cleaned = re.sub(r"[^a-zа-я0-9]+", " ", cleaned)
    return " ".join(cleaned.split())


def clean_title(title: str, building_name: str) -> str:
    """
    Удаляет из title упоминание ЖК
    Паттерны для удаления:
    - "в ЖК «Зубово Life Garden (Зубово Лайф Гарден)»"
    - "в ЖК «Зубово Life Garden»"
    - "в ЖК «...»" (любое упоминание)
    """
    if not title:
        return title
    
    # Различные варианты паттернов для удаления
    patterns = [
        r'\s*в\s+ЖК\s*«[^»]+»',  # "в ЖК «...»"
        r'\s*в\s+жк\s*«[^»]+»',    # "в жк «...»" (нижний регистр)
        r'\s*в\s+ЖК\s*"[^"]+"',    # "в ЖК "...""
        r'\s*в\s+жк\s*"[^"]+"',    # "в жк "..."" (нижний регистр)
    ]
    
    cleaned = title
    for pattern in patterns:
        cleaned = re.sub(pattern, '', cleaned, flags=re.IGNORECASE)
    
    # Убираем лишние пробелы в конце
    cleaned = cleaned.strip()
    
    return cleaned


def process_building():
    """Обрабатывает ЖК и очищает названия квартир"""
    db = get_mongo_connection()
    collection = db[COLLECTION_NAME]
    
    # Ищем ЖК по названию
    building = None
    
    # Пробуем разные варианты поиска
    search_patterns = [
        {"development.name": BUILDING_NAME},
        {"development.name": {"$regex": "Зубово", "$options": "i"}},
        {"name": BUILDING_NAME},
    ]
    
    for pattern in search_patterns:
        building = collection.find_one(pattern)
        if building:
            break
    
    if not building:
        print(f"❌ ЖК '{BUILDING_NAME}' не найден в коллекции {COLLECTION_NAME}")
        return
    
    building_id = building["_id"]
    building_display_name = building.get("development", {}).get("name") or building.get("name", BUILDING_NAME)
    
    print(f"🏢 Найден ЖК: {building_display_name}")
    print(f"📝 ID: {building_id}")
    print("-" * 80)
    
    apartment_types = building.get("apartment_types", {})
    total_updated = 0
    total_checked = 0
    
    updates = {}
    
    for apt_type, type_data in apartment_types.items():
        apartments = type_data.get("apartments", [])
        type_updates = []
        
        for idx, apt in enumerate(apartments):
            total_checked += 1
            original_title = apt.get("title", "")
            
            if not original_title:
                continue
            
            cleaned_title = clean_title(original_title, building_display_name)
            
            if cleaned_title != original_title:
                type_updates.append({
                    "index": idx,
                    "original": original_title,
                    "cleaned": cleaned_title
                })
                total_updated += 1
        
        if type_updates:
            updates[apt_type] = type_updates
    
    # Выводим статистику
    print(f"📊 Статистика:")
    print(f"   Всего квартир проверено: {total_checked}")
    print(f"   Квартир для обновления: {total_updated}")
    print("-" * 80)
    
    if not updates:
        print("✅ Все названия уже чистые, обновлений не требуется")
        return
    
    # Показываем примеры изменений
    print("\n📋 Примеры изменений:")
    example_count = 0
    for apt_type, type_updates in updates.items():
        for update in type_updates[:3]:  # Показываем первые 3 примера каждого типа
            if example_count >= 10:  # Максимум 10 примеров
                break
            print(f"\n   Тип: {apt_type}")
            print(f"   Было: {update['original']}")
            print(f"   Стало: {update['cleaned']}")
            example_count += 1
        if example_count >= 10:
            break
    
    if total_updated > 10:
        print(f"\n   ... и еще {total_updated - 10} квартир")
    
    # Подтверждение
    print("\n" + "=" * 80)
    response = input(f"⚠️  Обновить {total_updated} квартир? (yes/no): ").strip().lower()
    
    if response not in ('yes', 'y', 'да', 'д'):
        print("❌ Операция отменена")
        return
    
    # Выполняем обновления
    print("\n🔄 Обновляю названия...")
    
    for apt_type, type_updates in updates.items():
        field_path = f"apartment_types.{apt_type}.apartments"
        
        for update in type_updates:
            idx = update["index"]
            cleaned_title = update["cleaned"]
            
            # Обновляем title по индексу
            result = collection.update_one(
                {"_id": building_id},
                {"$set": {f"{field_path}.{idx}.title": cleaned_title}}
            )
            
            if result.modified_count > 0:
                print(f"   ✅ {apt_type}[{idx}]: обновлено")
            else:
                print(f"   ⚠️  {apt_type}[{idx}]: не удалось обновить")
    
    print("\n" + "=" * 80)
    print(f"✅ Готово! Обновлено {total_updated} квартир")
    print(f"📝 ЖК: {building_display_name}")


if __name__ == "__main__":
    print("🧹 Скрипт очистки названий квартир")
    print("=" * 80)
    process_building()

