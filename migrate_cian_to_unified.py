#!/usr/bin/env python3
"""
Скрипт для миграции данных из CIAN в unified_houses
Обновляет существующую запись данными из CIAN, сохраняя все остальные поля
"""

import os
import json
import re
import argparse
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
CIAN_COLLECTION_NAME = "unified_houses_2"  # Коллекция где cian_3.py сохраняет данные
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


def load_cian_data_from_mongo(db) -> Optional[Dict]:
    """Загружает данные из MongoDB коллекции unified_houses_2"""
    try:
        cian_col = db[CIAN_COLLECTION_NAME]
        
        # Ищем ЖК «8 NEBO» в коллекции
        query = {"building_title": BUILDING_NAME}
        building = cian_col.find_one(query, projection={"_id": 0})
        
        if building:
            print(f"✅ Найден ЖК в MongoDB ({CIAN_COLLECTION_NAME}): {BUILDING_NAME}")
            return building
        
        # Пробуем поиск по частичному совпадению
        query_regex = {"building_title": {"$regex": BUILDING_NAME.replace("«", "").replace("»", ""), "$options": "i"}}
        building = cian_col.find_one(query_regex, projection={"_id": 0})
        
        if building:
            print(f"✅ Найден ЖК в MongoDB (по частичному совпадению): {building.get('building_title', BUILDING_NAME)}")
            return building
        
        print(f"❌ ЖК '{BUILDING_NAME}' не найден в коллекции {CIAN_COLLECTION_NAME}")
        return None
    except Exception as e:
        print(f"❌ Ошибка загрузки данных из MongoDB: {e}")
        return None


def load_cian_data_from_file() -> Optional[Dict]:
    """Загружает данные из CIAN JSON файла (резервный вариант)"""
    if not CIAN_DATA_FILE.exists():
        print(f"⚠️ Файл не найден: {CIAN_DATA_FILE}")
        return None
    
    try:
        with open(CIAN_DATA_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)
        
        # Ищем ЖК «8 NEBO»
        for building in data:
            if building.get("building_title") == BUILDING_NAME:
                print(f"✅ Найден ЖК в файле: {BUILDING_NAME}")
                return building
        
        print(f"❌ ЖК '{BUILDING_NAME}' не найден в файле")
        return None
    except Exception as e:
        print(f"❌ Ошибка загрузки данных из файла: {e}")
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


def update_unified_record_with_cian(old_record: Dict, cian_building: Dict) -> Dict:
    """
    Обновляет существующую запись unified_houses данными из CIAN
    Плавная миграция: обновляет только нужные поля, сохраняя все остальное
    """
    import copy
    
    # Копируем development из старой записи, чтобы сохранить все поля
    old_dev = old_record.get("development", {})
    updated_dev = copy.deepcopy(old_dev)
    
    # Обновляем только photos в development
    cian_photos = cian_building.get("building_photos", [])
    updated_dev["photos"] = cian_photos
    print(f"📸 Обновлены фото ЖК: {len(cian_photos)} фото из CIAN (было: {len(old_dev.get('photos', []))})")
    
    # Обновляем название если оно отличается (но обычно оно одинаковое)
    cian_name = cian_building.get("building_title", "")
    if cian_name and updated_dev.get("name") != cian_name:
        updated_dev["name"] = cian_name
        print(f"📝 Обновлено название ЖК: {cian_name}")
    
    # Подготавливаем обновления
    updates = {
        "$set": {
            "updated_at": datetime.now(timezone.utc),
            "development": updated_dev  # Обновляем весь development объект, сохраняя все поля
        }
    }
    
    # Обновляем apartment_types из CIAN (полностью заменяем старые квартиры новыми)
    apartments = cian_building.get("apartments", [])
    
    print(f"📦 Обрабатываем {len(apartments)} квартир из CIAN...")
    
    # Создаем новую структуру apartment_types из данных CIAN
    apartment_types = {}
    
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
    
    # Заменяем apartment_types полностью
    old_apt_count = sum(len(apt_type_data.get("apartments", [])) 
                       for apt_type_data in old_record.get("apartment_types", {}).values())
    updates["$set"]["apartment_types"] = apartment_types
    
    # Статистика
    total_apartments = sum(len(apt_type_data.get("apartments", [])) 
                          for apt_type_data in apartment_types.values())
    print(f"✅ Обновлены квартиры: {old_apt_count} → {total_apartments} квартир в {len(apartment_types)} типах")
    
    return updates


def compare_structures(old_record: Dict, updates: Dict, cian_building: Dict) -> None:
    """Сравнивает старую и новую структуру для dry-run режима"""
    print("\n" + "="*80)
    print("📊 СРАВНЕНИЕ СТРУКТУР (DRY-RUN)")
    print("="*80)
    
    # Сравнение development
    old_dev = old_record.get("development", {})
    new_dev = updates["$set"].get("development", {})
    
    print("\n🏢 DEVELOPMENT:")
    print(f"  Название:")
    print(f"    Старое: {old_dev.get('name', 'N/A')}")
    print(f"    Новое:  {new_dev.get('name', 'N/A')}")
    print(f"    {'✅ Совпадает' if old_dev.get('name') == new_dev.get('name') else '⚠️ Изменилось'}")
    
    print(f"\n  Фото ЖК:")
    old_photos = old_dev.get('photos', [])
    new_photos = new_dev.get('photos', [])
    print(f"    Старое: {len(old_photos)} фото")
    print(f"    Новое:  {len(new_photos)} фото")
    print(f"    {'✅ Совпадает' if len(old_photos) == len(new_photos) else '🔄 Обновлено'}")
    
    print(f"\n  Адрес:")
    print(f"    Старое: {old_dev.get('address', 'N/A')}")
    print(f"    Новое:  {new_dev.get('address', 'N/A')}")
    print(f"    {'✅ Сохранен' if old_dev.get('address') == new_dev.get('address') else '⚠️ Изменилось'}")
    
    print(f"\n  Корпуса:")
    old_korpuses = old_dev.get('korpuses', [])
    new_korpuses = new_dev.get('korpuses', [])
    print(f"    Старое: {len(old_korpuses)} корпусов")
    print(f"    Новое:  {len(new_korpuses)} корпусов")
    print(f"    {'✅ Сохранены' if len(old_korpuses) == len(new_korpuses) else '⚠️ Изменилось'}")
    
    print(f"\n  Диапазон цен:")
    print(f"    Старое: {old_dev.get('price_range', 'N/A')}")
    print(f"    Новое:  {new_dev.get('price_range', 'N/A')}")
    print(f"    {'✅ Сохранен' if old_dev.get('price_range') == new_dev.get('price_range') else '⚠️ Изменилось'}")
    
    # Сравнение apartment_types
    old_apt_types = old_record.get("apartment_types", {})
    new_apt_types = updates["$set"].get("apartment_types", {})
    
    print("\n🏠 APARTMENT_TYPES:")
    old_total = sum(len(apt_type_data.get('apartments', [])) 
                   for apt_type_data in old_apt_types.values())
    new_total = sum(len(apt_type_data.get('apartments', [])) 
                   for apt_type_data in new_apt_types.values())
    
    print(f"  Всего квартир:")
    print(f"    Старое: {old_total} квартир")
    print(f"    Новое:  {new_total} квартир")
    print(f"    {'✅ Совпадает' if old_total == new_total else '🔄 Обновлено'}")
    
    print(f"\n  По типам:")
    all_types = set(old_apt_types.keys()) | set(new_apt_types.keys())
    for apt_type in sorted(all_types):
        old_count = len(old_apt_types.get(apt_type, {}).get('apartments', []))
        new_count = len(new_apt_types.get(apt_type, {}).get('apartments', []))
        status = "✅" if old_count == new_count else "🔄"
        print(f"    {status} {apt_type}-комн: {old_count} → {new_count}")
    
    # Проверка структуры квартиры
    if new_apt_types:
        first_type = list(new_apt_types.keys())[0]
        first_apt = new_apt_types[first_type].get('apartments', [])
        if first_apt:
            print(f"\n  📋 Структура квартиры (пример из {first_type}-комн):")
            example_apt = first_apt[0]
            print(f"    Поля в квартире: {', '.join(sorted(example_apt.keys()))}")
            
            # Проверяем наличие всех важных полей
            important_fields = ['title', 'rooms', 'area', 'totalArea', 'price', 'url', 
                              'images_apartment', 'decoration']
            missing_fields = [f for f in important_fields if f not in example_apt]
            if missing_fields:
                print(f"    ⚠️ Отсутствуют поля: {', '.join(missing_fields)}")
            else:
                print(f"    ✅ Все важные поля присутствуют")
    
    # Проверка сохранения других полей
    print("\n🔍 ПРОВЕРКА СОХРАНЕНИЯ ПОЛЕЙ:")
    important_fields = [
        'latitude', 'longitude', 'city', 'district', 'street', 'name',
        'address_full', 'address_city', 'address_district', 
        'address_street', 'address_house',
        'rating', 'rating_description', 'construction_progress',
        '_source_ids'
    ]
    
    preserved = []
    missing = []
    for field in important_fields:
        if field in old_record:
            preserved.append(field)
        else:
            missing.append(field)
    
    print(f"  ✅ Сохранены ({len(preserved)}): {', '.join(preserved[:10])}")
    if len(preserved) > 10:
        print(f"     ... и еще {len(preserved) - 10} полей")
    if missing:
        print(f"  ⚠️ Отсутствуют в старой записи: {', '.join(missing)}")
    
    print("\n" + "="*80)
    print("✅ ВСЕ ОСТАЛЬНЫЕ ПОЛЯ СОХРАНЯЮТСЯ БЕЗ ИЗМЕНЕНИЙ")
    print("🔄 ОБНОВЛЯЮТСЯ ТОЛЬКО: development.photos и apartment_types")
    print("="*80)


def main():
    """Основная функция миграции"""
    parser = argparse.ArgumentParser(description='Миграция данных из CIAN в unified_houses')
    parser.add_argument('--dry-run', action='store_true', 
                       help='Тестовый режим: показывает что будет обновлено, но не сохраняет в БД')
    args = parser.parse_args()
    
    if args.dry_run:
        print("🧪 DRY-RUN РЕЖИМ: изменения не будут сохранены в БД")
    
    print("🔄 Начинаем миграцию данных из CIAN в unified_houses...")
    print(f"📁 Ищем ЖК: {BUILDING_NAME}")
    
    # Подключаемся к MongoDB
    db = get_mongo_connection()
    
    # Загружаем данные из CIAN (сначала из MongoDB, потом из файла как резерв)
    cian_building = load_cian_data_from_mongo(db)
    if not cian_building:
        print("⚠️ Не найдено в MongoDB, пробую загрузить из файла...")
        cian_building = load_cian_data_from_file()
        if not cian_building:
            return
    
    # Находим существующую запись
    old_record = find_unified_record(db, BUILDING_NAME)
    if not old_record:
        return
    
    record_id = old_record.get('_id')
    print(f"📍 Запись ID: {record_id}")
    print(f"📍 Координаты: {old_record.get('latitude')}, {old_record.get('longitude')}")
    
    # Подготавливаем обновления
    print("\n🔨 Подготавливаем обновления...")
    updates = update_unified_record_with_cian(old_record, cian_building)
    
    # В dry-run режиме показываем сравнение
    if args.dry_run:
        compare_structures(old_record, updates, cian_building)
        print("\n🧪 DRY-RUN: изменения НЕ сохранены в БД")
        print("   Для реального обновления запустите скрипт без флага --dry-run")
        return
    
    # Обновляем существующую запись
    unified_col = db['unified_houses']
    result = unified_col.update_one(
        {'_id': record_id},
        updates
    )
    
    if result.modified_count > 0:
        print(f"\n✅ Запись обновлена!")
        print(f"📝 ID записи: {record_id}")
        
        # Получаем обновленную запись для статистики
        updated_record = unified_col.find_one({'_id': record_id})
        if updated_record:
            apt_count = sum(len(apt_type_data.get('apartments', [])) 
                          for apt_type_data in updated_record.get('apartment_types', {}).values())
            photos_count = len(updated_record.get('development', {}).get('photos', []))
            print(f"📊 Квартир в записи: {apt_count}")
            print(f"📸 Фото ЖК: {photos_count}")
    else:
        print(f"\n⚠️ Запись не была обновлена (возможно, данные не изменились)")


if __name__ == "__main__":
    main()

