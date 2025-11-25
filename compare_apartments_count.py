#!/usr/bin/env python3
"""
Скрипт для сравнения количества квартир между unified_houses и unified_houses_2
Показывает статистику и определяет, какие ЖК можно обновлять, а какие нет
"""

import os
from pathlib import Path
from pymongo import MongoClient
from bson import ObjectId
from dotenv import load_dotenv
from typing import Dict, List, Tuple

# Загружаем переменные окружения
PROJECT_ROOT = Path(__file__).resolve().parent
load_dotenv(dotenv_path=PROJECT_ROOT / ".env")

UNIFIED_COLLECTION = "unified_houses"
CIAN_COLLECTION = "unified_houses_2"


def get_mongo_connection():
    """Получить подключение к MongoDB"""
    MONGO_URI = os.getenv("MONGO_URI", "mongodb://root:Kfleirb_17@176.98.177.188:27017/admin")
    DB_NAME = os.getenv("DB_NAME", "houses")
    client = MongoClient(MONGO_URI)
    db = client[DB_NAME]
    return db


def count_apartments_in_unified(record: Dict) -> int:
    """Подсчитывает количество квартир в записи unified_houses"""
    apartment_types = record.get("apartment_types", {})
    total = 0
    for apt_type_data in apartment_types.values():
        apartments = apt_type_data.get("apartments", [])
        total += len(apartments)
    return total


def count_apartments_in_cian(record: Dict) -> int:
    """Подсчитывает количество квартир в записи unified_houses_2"""
    apartments = record.get("apartments", [])
    return len(apartments)


def get_building_name_from_unified(record: Dict) -> str:
    """Извлекает название ЖК из unified_houses"""
    # Пробуем разные варианты
    dev = record.get("development", {})
    if dev and dev.get("name"):
        return dev.get("name")
    
    # Пробуем верхний уровень
    if record.get("name"):
        return record.get("name")
    
    return "Без названия"


def get_building_name_from_cian(record: Dict) -> str:
    """Извлекает название ЖК из unified_houses_2"""
    return record.get("building_title", "Без названия")


def normalize_building_name(name: str) -> str:
    """Нормализует название ЖК для сравнения"""
    if not name:
        return ""
    # Убираем лишние пробелы и приводим к нижнему регистру
    return " ".join(name.lower().split())


def find_matching_buildings(unified_records: List[Dict], cian_records: List[Dict]) -> List[Dict]:
    """Находит совпадающие ЖК между unified_houses и unified_houses_2"""
    matches = []
    
    # Создаем индекс cian записей по нормализованному названию
    cian_index = {}
    for cian_record in cian_records:
        cian_name = get_building_name_from_cian(cian_record)
        normalized = normalize_building_name(cian_name)
        if normalized:
            cian_index[normalized] = cian_record
    
    # Ищем совпадения
    for unified_record in unified_records:
        unified_name = get_building_name_from_unified(unified_record)
        normalized = normalize_building_name(unified_name)
        
        if normalized in cian_index:
            cian_record = cian_index[normalized]
            matches.append({
                "unified": unified_record,
                "cian": cian_record,
                "name": unified_name
            })
    
    return matches


def compare_apartments():
    """Сравнивает количество квартир между коллекциями"""
    print("🔍 Начинаем сравнение количества квартир...")
    print(f"📊 unified_houses vs {CIAN_COLLECTION}\n")
    
    db = get_mongo_connection()
    
    # Загружаем все записи из unified_houses
    print("📥 Загружаем данные из unified_houses...")
    unified_col = db[UNIFIED_COLLECTION]
    unified_records = list(unified_col.find({}))
    print(f"✅ Загружено {len(unified_records)} записей из unified_houses")
    
    # Загружаем все записи из unified_houses_2
    print(f"📥 Загружаем данные из {CIAN_COLLECTION}...")
    cian_col = db[CIAN_COLLECTION]
    cian_records = list(cian_col.find({}))
    print(f"✅ Загружено {len(cian_records)} записей из {CIAN_COLLECTION}\n")
    
    # Находим совпадающие ЖК
    print("🔗 Ищем совпадающие ЖК...")
    matches = find_matching_buildings(unified_records, cian_records)
    print(f"✅ Найдено {len(matches)} совпадающих ЖК\n")
    
    # Сравниваем количество квартир
    print("="*100)
    print("📊 СРАВНЕНИЕ КОЛИЧЕСТВА КВАРТИР")
    print("="*100)
    
    results = []
    can_update = []
    should_skip = []
    only_unified = []
    only_cian = []
    
    for match in matches:
        unified_record = match["unified"]
        cian_record = match["cian"]
        name = match["name"]
        
        unified_count = count_apartments_in_unified(unified_record)
        cian_count = count_apartments_in_cian(cian_record)
        
        unified_id = str(unified_record.get("_id", "N/A"))
        cian_id = str(cian_record.get("_id", "N/A"))
        
        result = {
            "name": name,
            "unified_id": unified_id,
            "cian_id": cian_id,
            "unified_count": unified_count,
            "cian_count": cian_count,
            "difference": cian_count - unified_count,
            "can_update": False,
            "reason": ""
        }
        
        # Определяем, можно ли обновлять
        if unified_count > cian_count:
            result["can_update"] = False
            result["reason"] = "⚠️ В unified_houses больше квартир - НЕ ОБНОВЛЯТЬ"
            should_skip.append(result)
        elif cian_count > unified_count:
            result["can_update"] = True
            result["reason"] = "✅ В CIAN больше квартир - МОЖНО ОБНОВИТЬ"
            can_update.append(result)
        elif cian_count == unified_count:
            result["can_update"] = True
            result["reason"] = "🔄 Количество одинаковое - МОЖНО ОБНОВИТЬ (данные могут отличаться)"
            can_update.append(result)
        else:
            result["can_update"] = False
            result["reason"] = "❓ Неопределенная ситуация"
            should_skip.append(result)
        
        results.append(result)
    
    # ЖК только в unified_houses
    unified_names = {normalize_building_name(get_building_name_from_unified(r)) for r in unified_records}
    cian_names = {normalize_building_name(get_building_name_from_cian(r)) for r in cian_records}
    
    for unified_record in unified_records:
        name = get_building_name_from_unified(unified_record)
        normalized = normalize_building_name(name)
        if normalized not in cian_names:
            unified_count = count_apartments_in_unified(unified_record)
            only_unified.append({
                "name": name,
                "unified_id": str(unified_record.get("_id", "N/A")),
                "unified_count": unified_count,
                "reason": "📌 Только в unified_houses"
            })
    
    # ЖК только в unified_houses_2
    for cian_record in cian_records:
        name = get_building_name_from_cian(cian_record)
        normalized = normalize_building_name(name)
        if normalized not in unified_names:
            cian_count = count_apartments_in_cian(cian_record)
            only_cian.append({
                "name": name,
                "cian_id": str(cian_record.get("_id", "N/A")),
                "cian_count": cian_count,
                "reason": "📌 Только в unified_houses_2"
            })
    
    # Выводим результаты
    print("\n" + "="*100)
    print("✅ МОЖНО ОБНОВИТЬ (в CIAN больше или равно квартир)")
    print("="*100)
    if can_update:
        for result in sorted(can_update, key=lambda x: x["difference"], reverse=True):
            print(f"\n🏢 {result['name']}")
            print(f"   unified_houses:     {result['unified_count']:4d} квартир (ID: {result['unified_id'][:24]}...)")
            print(f"   unified_houses_2:   {result['cian_count']:4d} квартир (ID: {result['cian_id'][:24]}...)")
            print(f"   Разница:            {result['difference']:+4d} квартир")
            print(f"   {result['reason']}")
    else:
        print("   Нет ЖК для обновления")
    
    print("\n" + "="*100)
    print("⚠️ НЕ ОБНОВЛЯТЬ (в unified_houses больше квартир)")
    print("="*100)
    if should_skip:
        for result in sorted(should_skip, key=lambda x: x["unified_count"] - x["cian_count"], reverse=True):
            print(f"\n🏢 {result['name']}")
            print(f"   unified_houses:     {result['unified_count']:4d} квартир (ID: {result['unified_id'][:24]}...)")
            print(f"   unified_houses_2:   {result['cian_count']:4d} квартир (ID: {result['cian_id'][:24]}...)")
            print(f"   Разница:            {result['difference']:+4d} квартир")
            print(f"   {result['reason']}")
    else:
        print("   Нет ЖК для пропуска")
    
    print("\n" + "="*100)
    print("📌 ТОЛЬКО В unified_houses (нет в unified_houses_2)")
    print("="*100)
    if only_unified:
        for item in sorted(only_unified, key=lambda x: x["unified_count"], reverse=True):
            print(f"   {item['name']:50s} - {item['unified_count']:4d} квартир (ID: {item['unified_id'][:24]}...)")
    else:
        print("   Нет таких ЖК")
    
    print("\n" + "="*100)
    print("📌 ТОЛЬКО В unified_houses_2 (нет в unified_houses)")
    print("="*100)
    if only_cian:
        for item in sorted(only_cian, key=lambda x: x["cian_count"], reverse=True):
            print(f"   {item['name']:50s} - {item['cian_count']:4d} квартир (ID: {item['cian_id'][:24]}...)")
    else:
        print("   Нет таких ЖК")
    
    # Итоговая статистика
    print("\n" + "="*100)
    print("📈 ИТОГОВАЯ СТАТИСТИКА")
    print("="*100)
    print(f"   Всего ЖК в unified_houses:        {len(unified_records)}")
    print(f"   Всего ЖК в unified_houses_2:      {len(cian_records)}")
    print(f"   Совпадающих ЖК:                    {len(matches)}")
    print(f"   ✅ Можно обновить:                 {len(can_update)}")
    print(f"   ⚠️ Не обновлять:                  {len(should_skip)}")
    print(f"   📌 Только в unified_houses:        {len(only_unified)}")
    print(f"   📌 Только в unified_houses_2:      {len(only_cian)}")
    
    total_unified_apts = sum(count_apartments_in_unified(r) for r in unified_records)
    total_cian_apts = sum(count_apartments_in_cian(r) for r in cian_records)
    print(f"\n   Всего квартир в unified_houses:    {total_unified_apts}")
    print(f"   Всего квартир в unified_houses_2:  {total_cian_apts}")
    
    print("\n" + "="*100)


if __name__ == "__main__":
    compare_apartments()

