#!/usr/bin/env python3
"""
Скрипт для миграции данных из CIAN в unified_houses
Создает новую запись с данными из CIAN, сохраняя критичные поля из старой записи
"""

import os
import json
import re
from pathlib import Path
from datetime import datetime, timezone
from typing import Dict, List, Any, Optional, Tuple
from pymongo import MongoClient
from bson import ObjectId
from dotenv import load_dotenv

# Загружаем переменные окружения
PROJECT_ROOT = Path(__file__).resolve().parent
load_dotenv(dotenv_path=PROJECT_ROOT / ".env")

CIAN_DATA_FILE = PROJECT_ROOT / "cian" / "cian_apartments_data.json"
BUILDING_NAME = "ЖК «8 NEBO»"


def get_mongo_connection():
    """Получить подключение к MongoDB"""
    MONGO_URI = os.getenv("MONGO_URI", "mongodb://root:Kfleirb_17@176.98.177.188:27017/admin")
    DB_NAME = os.getenv("DB_NAME", "houses")
    client = MongoClient(MONGO_URI)
    db = client[DB_NAME]
    return db


def parse_rooms_from_title(title: str) -> Optional[int]:
    """
    Извлекает количество комнат из title
    Возвращает: 0 для студии, 1-4 для комнат, None если не найдено
    """
    if not title:
        return None
    
    title_lower = title.lower()
    
    # Проверяем студию
    if 'студия' in title_lower or 'studio' in title_lower:
        return 0
    
    # Ищем паттерны: "1-комн", "1 ком", "1-к.", "2-комн" и т.д.
    patterns = [
        r'(\d+)[-\s]*комн',      # "1-комн", "2 комн"
        r'(\d+)[-\s]*к\.',       # "1-к.", "2 к."
        r'(\d+)[-\s]*ком',       # "1 ком", "2 ком"
        r'^(\d+)[-\s]*комн',    # "1-комн" в начале
    ]
    
    for pattern in patterns:
        match = re.search(pattern, title_lower)
        if match:
            try:
                rooms = int(match.group(1))
                if 1 <= rooms <= 10:  # Ограничение разумных значений
                    return rooms
            except ValueError:
                continue
    
    return None


def parse_floor_info(floor_str: str) -> Tuple[Optional[int], Optional[int]]:
    """
    Парсит строку этажа и извлекает минимальный и максимальный этаж
    Форматы: "12 из 32", "14/27", "5-10", "12"
    Возвращает: (floorMin: int, floorMax: int) или (None, None) если не удалось распарсить
    """
    if not floor_str:
        return None, None
    
    # Паттерн 1: "12 из 32" или "12 из 32 этаж"
    match = re.search(r'(\d+)\s+из\s+(\d+)', floor_str)
    if match:
        try:
            floor_min = int(match.group(1))
            floor_max = int(match.group(2))
            return floor_min, floor_max
        except ValueError:
            pass
    
    # Паттерн 2: "14/27" или "14/27 эт"
    match = re.search(r'(\d+)/(\d+)', floor_str)
    if match:
        try:
            floor_min = int(match.group(1))
            floor_max = int(match.group(2))
            return floor_min, floor_max
        except ValueError:
            pass
    
    # Паттерн 3: "5-10" (диапазон)
    match = re.search(r'(\d+)-(\d+)', floor_str)
    if match:
        try:
            floor_min = int(match.group(1))
            floor_max = int(match.group(2))
            return floor_min, floor_max
        except ValueError:
            pass
    
    # Паттерн 4: просто число "12"
    match = re.search(r'(\d+)', floor_str)
    if match:
        try:
            floor_min = int(match.group(1))
            floor_max = floor_min  # Если один этаж, min = max
            return floor_min, floor_max
        except ValueError:
            pass
    
    return None, None


def parse_area_from_string(area_str: str) -> Tuple[Optional[str], Optional[float]]:
    """
    Парсит площадь из строки типа "57,03 м²"
    Возвращает: (area: string, totalArea: float) или (None, None)
    """
    if not area_str:
        return None, None
    
    # Ищем паттерн типа "57,03 м²" или "57.03 м²"
    match = re.search(r'(\d+[,.]?\d*)\s*м²', area_str)
    if match:
        area_str_clean = match.group(1).replace(',', '.')
        try:
            area_float = float(area_str_clean)
            return area_str_clean, area_float
        except ValueError:
            pass
    
    return None, None


def extract_factoid_value(factoids: List[Dict], label: str) -> Optional[str]:
    """Извлекает значение из factoids по label"""
    for factoid in factoids:
        if factoid.get("label") == label:
            return factoid.get("value")
    return None


def extract_summary_value(summary_info: List[Dict], label: str) -> Optional[str]:
    """Извлекает значение из summary_info по label"""
    for item in summary_info:
        if item.get("label") == label:
            return item.get("value")
    return None


def convert_cian_apartment_to_unified(cian_apt: Dict) -> Optional[Dict]:
    """
    Преобразует квартиру из формата CIAN в формат unified_houses
    """
    # Определяем тип квартиры
    title = cian_apt.get("title", "")
    rooms = parse_rooms_from_title(title)
    
    if rooms is None:
        print(f"⚠️ Не удалось определить количество комнат для: {title}")
        return None
    
    # Пропускаем квартиры без основного фото
    main_photo = cian_apt.get("main_photo")
    if not main_photo:
        print(f"⚠️ Пропускаем квартиру без фото: {title}")
        return None
    
    # Базовые поля
    apartment = {
        "title": title,
        "rooms": rooms,
        "url": cian_apt.get("url", ""),
        "price": cian_apt.get("price", ""),
        "pricePerSquare": cian_apt.get("price_per_square", ""),
        "images_apartment": [main_photo] if main_photo else [],  # Массив основных фото
    }
    
    # Площадь из factoids
    factoids = cian_apt.get("factoids", [])
    area_str = extract_factoid_value(factoids, "Общая площадь")
    if area_str:
        area, total_area = parse_area_from_string(area_str)
        if area:
            apartment["area"] = area
        if total_area:
            apartment["totalArea"] = total_area
    
    # Год сдачи
    completion_date = extract_factoid_value(factoids, "Год сдачи")
    if completion_date:
        apartment["completionDate"] = completion_date
    
    # Этаж
    floor_str = extract_factoid_value(factoids, "Этаж")
    if floor_str:
        floor_min, floor_max = parse_floor_info(floor_str)
        if floor_min is not None:
            apartment["floorMin"] = floor_min
        if floor_max is not None:
            apartment["floorMax"] = floor_max
    
    # Дополнительные параметры из factoids
    living_area = extract_factoid_value(factoids, "Жилая площадь")
    if living_area:
        apartment["livingArea"] = living_area.replace(" м²", "").replace(",", ".")
    
    kitchen_area = extract_factoid_value(factoids, "Площадь кухни")
    if kitchen_area:
        apartment["kitchenArea"] = kitchen_area.replace(" м²", "").replace(",", ".")
    
    house_status = extract_factoid_value(factoids, "Дом")
    if house_status:
        apartment["houseStatus"] = house_status
    
    decoration_type = extract_factoid_value(factoids, "Отделка")
    if decoration_type:
        apartment["decorationType"] = decoration_type
    
    # Дополнительные параметры из summary_info
    summary_info = cian_apt.get("summary_info", [])
    housing_type = extract_summary_value(summary_info, "Тип жилья")
    if housing_type:
        apartment["housingType"] = housing_type
    
    ceiling_height = extract_summary_value(summary_info, "Высота потолков")
    if ceiling_height:
        apartment["ceilingHeight"] = ceiling_height
    
    house_type = extract_summary_value(summary_info, "Тип дома")
    if house_type:
        apartment["houseType"] = house_type
    
    deal_type = extract_summary_value(summary_info, "Тип сделки")
    if deal_type:
        apartment["dealType"] = deal_type
    
    # Объект отделки
    decoration = cian_apt.get("decoration", {})
    if decoration:
        decoration_obj = {
            "description": decoration.get("description", ""),
            "photos": decoration.get("photos", [])
        }
        if decoration_obj["description"] or decoration_obj["photos"]:
            apartment["decoration"] = decoration_obj
    
    return apartment


def load_cian_data() -> Optional[Dict]:
    """Загружает данные из CIAN JSON файла"""
    if not CIAN_DATA_FILE.exists():
        print(f"❌ Файл не найден: {CIAN_DATA_FILE}")
        return None
    
    try:
        with open(CIAN_DATA_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)
        
        # Ищем ЖК «8 NEBO»
        for building in data:
            if building.get("building_title") == BUILDING_NAME:
                print(f"✅ Найден ЖК: {BUILDING_NAME}")
                return building
        
        print(f"❌ ЖК '{BUILDING_NAME}' не найден в файле")
        return None
    except Exception as e:
        print(f"❌ Ошибка загрузки данных из CIAN: {e}")
        return None


def find_unified_record(db, building_name: str):
    """Находит запись в unified_houses по названию ЖК"""
    unified_col = db['unified_houses']
    
    # Пробуем разные варианты поиска
    search_patterns = [
        {"development.name": building_name},
        {"development.name": {"$regex": building_name.replace("«", "").replace("»", ""), "$options": "i"}},
        {"development.name": {"$regex": "8 NEBO", "$options": "i"}},
        {"development.name": {"$regex": "8Nebo", "$options": "i"}},
    ]
    
    for pattern in search_patterns:
        record = unified_col.find_one(pattern)
        if record:
            print(f"✅ Найдена запись в unified_houses: {record.get('development', {}).get('name', 'Без названия')}")
            return record
    
    print(f"❌ Запись с названием '{building_name}' не найдена в unified_houses")
    return None


def create_new_unified_record(old_record: Dict, cian_building: Dict) -> Dict:
    """
    Создает новую запись unified_houses на основе старой записи и данных из CIAN
    """
    # Сохраняем критичные поля из старой записи
    new_record = {
        "latitude": old_record.get("latitude"),
        "longitude": old_record.get("longitude"),
        "source": "manual",
        "created_by": "manual",
        "is_featured": old_record.get("is_featured", False),
        "agent_id": old_record.get("agent_id"),
        "updated_at": datetime.now(timezone.utc),
    }
    
    # Сохраняем адресные поля
    address_fields = [
        "address_full", "address_city", "address_district", 
        "address_street", "address_house"
    ]
    for field in address_fields:
        if field in old_record:
            new_record[field] = old_record[field]
    
    # Сохраняем поля города, района, улицы на верхнем уровне (если есть)
    if "city" in old_record:
        new_record["city"] = old_record["city"]
    
    if "district" in old_record:
        new_record["district"] = old_record["district"]
    
    if "street" in old_record:
        new_record["street"] = old_record["street"]
    
    if "name" in old_record:
        new_record["name"] = old_record["name"]
    
    # Сохраняем рейтинг
    rating_fields = [
        "rating", "rating_description", 
        "rating_created_at", "rating_updated_at"
    ]
    for field in rating_fields:
        if field in old_record:
            new_record[field] = old_record[field]
    
    # Сохраняем ход строительства из старой записи (если есть)
    if "construction_progress" in old_record:
        new_record["construction_progress"] = old_record["construction_progress"]
        print(f"📊 Сохранен ход строительства из старой записи")
    
    # Development из CIAN
    new_record["development"] = {
        "name": cian_building.get("building_title", ""),
        "photos": cian_building.get("building_photos", [])
    }
    
    # Сохраняем дополнительные поля из старой записи в development
    old_dev = old_record.get("development", {})
    if old_dev.get("address"):
        new_record["development"]["address"] = old_dev["address"]
    
    # Сохраняем корпуса из старой записи (если есть)
    if old_dev.get("korpuses"):
        new_record["development"]["korpuses"] = old_dev["korpuses"]
        print(f"📊 Сохранены корпуса из старой записи: {len(old_dev['korpuses'])} корпусов")
    
    # Сохраняем price_range из старой записи (если есть)
    if old_dev.get("price_range"):
        new_record["development"]["price_range"] = old_dev["price_range"]
    
    # Сохраняем parameters из старой записи (если есть)
    if old_dev.get("parameters"):
        new_record["development"]["parameters"] = old_dev["parameters"]
    
    # Сохраняем _source_ids если есть, добавляем cian если нужно
    if "_source_ids" in old_record:
        new_record["_source_ids"] = old_record["_source_ids"].copy()
    else:
        new_record["_source_ids"] = {}
    
    # Группируем квартиры по типам
    apartment_types = {}
    apartments = cian_building.get("apartments", [])
    
    print(f"📦 Обрабатываем {len(apartments)} квартир из CIAN...")
    
    for cian_apt in apartments:
        unified_apt = convert_cian_apartment_to_unified(cian_apt)
        if not unified_apt:
            continue
        
        # Определяем тип для группировки
        rooms = unified_apt.get("rooms")
        if rooms == 0:
            apt_type = "Студия"
        else:
            apt_type = str(rooms)
        
        # Добавляем в соответствующую группу
        if apt_type not in apartment_types:
            apartment_types[apt_type] = {"apartments": []}
        
        apartment_types[apt_type]["apartments"].append(unified_apt)
    
    new_record["apartment_types"] = apartment_types
    
    # Статистика
    total_apartments = sum(len(apt_type_data.get("apartments", [])) 
                          for apt_type_data in apartment_types.values())
    print(f"✅ Создано {total_apartments} квартир в {len(apartment_types)} типах")
    
    return new_record


def main():
    """Основная функция миграции"""
    print("🔄 Начинаем миграцию данных из CIAN в unified_houses...")
    print(f"📁 Ищем ЖК: {BUILDING_NAME}")
    
    # Загружаем данные из CIAN
    cian_building = load_cian_data()
    if not cian_building:
        return
    
    print(f"📊 Найдено квартир в CIAN: {len(cian_building.get('apartments', []))}")
    print(f"📸 Найдено фото ЖК: {len(cian_building.get('building_photos', []))}")
    
    # Подключаемся к MongoDB
    db = get_mongo_connection()
    
    # Находим старую запись
    old_record = find_unified_record(db, BUILDING_NAME)
    if not old_record:
        return
    
    print(f"📍 Старая запись ID: {old_record.get('_id')}")
    print(f"📍 Координаты: {old_record.get('latitude')}, {old_record.get('longitude')}")
    
    # Создаем новую запись
    print("\n🔨 Создаем новую запись...")
    new_record = create_new_unified_record(old_record, cian_building)
    
    # Сохраняем новую запись
    unified_col = db['unified_houses']
    result = unified_col.insert_one(new_record)
    
    print(f"\n✅ Новая запись создана!")
    print(f"📝 Новый ID: {result.inserted_id}")
    print(f"📝 Старый ID: {old_record.get('_id')} (сохранен)")
    print(f"📊 Квартир в новой записи: {sum(len(apt_type_data.get('apartments', [])) for apt_type_data in new_record.get('apartment_types', {}).values())}")
    print(f"📸 Фото ЖК: {len(new_record.get('development', {}).get('photos', []))}")


if __name__ == "__main__":
    main()

