#!/usr/bin/env python3
"""
Сравнение квартир ЖК «Акварель» между unified_houses и unified_houses_3.
Печатает статистику по типам планировок и показывает пример расхождений.
"""

import os
from collections import defaultdict
from typing import Dict, Any

from pymongo import MongoClient
from bson import ObjectId
from dotenv import load_dotenv

PROJECT_ROOT = os.path.dirname(os.path.abspath(__file__))
load_dotenv(os.path.join(PROJECT_ROOT, ".env"))

BUILDING_NAME = "ЖК «Акварель»"
ORIGINAL_COLLECTION = "unified_houses"
MERGED_COLLECTION = "unified_houses_3"


def get_db():
    mongo_uri = os.getenv("MONGO_URI", "mongodb://root:Kfleirb_17@176.98.177.188:27017/admin")
    db_name = os.getenv("DB_NAME", "houses")
    client = MongoClient(mongo_uri)
    return client[db_name]


def fetch_building(collection, name) -> Dict[str, Any]:
    record = collection.find_one({"development.name": name})
    if not record:
        record = collection.find_one({"name": name})
    return record


def summarize_apartments(record) -> Dict[str, int]:
    result = {}
    for apt_type, data in (record or {}).get("apartment_types", {}).items():
        result[apt_type] = len(data.get("apartments", []))
    return result


def key_fields(apartment: Dict[str, Any]) -> Dict[str, Any]:
    if not apartment:
        return {}
    return {
        "title": apartment.get("title"),
        "price": apartment.get("price"),
        "pricePerSquare": apartment.get("pricePerSquare") or apartment.get("price_per_square"),
        "completionDate": apartment.get("completionDate"),
        "url": apartment.get("url"),
    }


def compare():
    db = get_db()
    orig = fetch_building(db[ORIGINAL_COLLECTION], BUILDING_NAME)
    merged = fetch_building(db[MERGED_COLLECTION], BUILDING_NAME)

    if not orig:
        print(f"❌ Не нашли запись в {ORIGINAL_COLLECTION}")
        return
    if not merged:
        print(f"❌ Не нашли запись в {MERGED_COLLECTION}")
        return

    orig_stats = summarize_apartments(orig)
    merged_stats = summarize_apartments(merged)

    print(f"🏢 {BUILDING_NAME}")
    print("------------------------------------------------------------")
    print(f"Источник: {ORIGINAL_COLLECTION}")
    for apt_type, count in sorted(orig_stats.items()):
        print(f"  {apt_type}: {count} квартир")
    print(f"Итого: {sum(orig_stats.values())}")
    print()

    print(f"Источник: {MERGED_COLLECTION}")
    for apt_type, count in sorted(merged_stats.items()):
        print(f"  {apt_type}: {count} квартир")
    print(f"Итого: {sum(merged_stats.values())}")
    print("------------------------------------------------------------\n")

    types = sorted(set(orig_stats.keys()) | set(merged_stats.keys()))
    for apt_type in types:
        orig_list = (orig.get("apartment_types", {}).get(apt_type, {}).get("apartments") or [])
        merged_list = (merged.get("apartment_types", {}).get(apt_type, {}).get("apartments") or [])
        if not orig_list and not merged_list:
            continue
        print(f"Тип: {apt_type}")
        if orig_list:
            print("  • Пример из unified_houses:")
            print(f"    {key_fields(orig_list[0])}")
        if merged_list:
            print("  • Пример из unified_houses_3:")
            print(f"    {key_fields(merged_list[0])}")
        print()


if __name__ == "__main__":
    compare()

