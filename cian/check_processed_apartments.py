#!/usr/bin/env python3
"""
Скрипт для проверки, все ли ссылки из cian_buildings.json были обработаны и сохранены в MongoDB.
"""

import json
import os
from pathlib import Path
from typing import List, Dict, Any, Set
from pymongo import MongoClient
from dotenv import load_dotenv

# Директория текущего скрипта
PROJECT_ROOT = Path(__file__).resolve().parent

# Загружаем переменные окружения из .env файла (из корня проекта)
PROJECT_ROOT_PARENT = PROJECT_ROOT.parent
load_dotenv(dotenv_path=PROJECT_ROOT_PARENT / ".env")

INPUT_FILE = PROJECT_ROOT / "cian_buildings.json"
OUTPUT_FILE = PROJECT_ROOT / "buildings_to_reprocess.json"
MONGO_COLLECTION_NAME = "unified_houses_2"


def load_buildings(path: str = str(INPUT_FILE)) -> List[Dict[str, Any]]:
    """Загружает список ЖК из JSON файла."""
    if not Path(path).exists():
        print(f"Файл не найден: {path}")
        return []

    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
        if isinstance(data, list):
            return data
        elif isinstance(data, dict) and "buildings" in data:
            return data["buildings"]
        else:
            return []
    except Exception as e:
        print(f"Ошибка при чтении файла {path}: {e}")
        return []


def get_mongo_collection():
    """Получает коллекцию MongoDB."""
    mongo_uri = os.getenv("MONGO_URI", "mongodb://root:Kfleirb_17@176.98.177.188:27017/admin")
    db_name = os.getenv("DB_NAME", "houses")
    client = MongoClient(mongo_uri)
    collection = client[db_name][MONGO_COLLECTION_NAME]
    return client, collection


def get_all_apartment_urls_from_mongo(collection) -> Set[str]:
    """Извлекает все URL квартир из MongoDB."""
    urls = set()
    try:
        for building_doc in collection.find({}, {"apartments": 1}):
            apartments = building_doc.get("apartments", [])
            for apt in apartments:
                url = apt.get("url")
                if url:
                    urls.add(url)
    except Exception as e:
        print(f"Ошибка при чтении из MongoDB: {e}")
    return urls


def main():
    print("Загружаю данные из cian_buildings.json...")
    buildings = load_buildings()
    if not buildings:
        print("Нет данных в файле cian_buildings.json")
        return

    # Собираем все ссылки из исходного файла
    expected_urls = set()
    building_stats = {}
    
    for building in buildings:
        building_title = building.get('title', 'Неизвестный ЖК')
        apartments_links = building.get('apartments', [])
        building_stats[building_title] = len(apartments_links)
        for url in apartments_links:
            expected_urls.add(url)

    print(f"\nВсего ЖК: {len(buildings)}")
    print(f"Всего ссылок на квартиры в исходном файле: {len(expected_urls)}")

    print("\nПодключаюсь к MongoDB...")
    client, collection = get_mongo_collection()

    print("Загружаю данные из MongoDB...")
    processed_urls = get_all_apartment_urls_from_mongo(collection)

    print(f"Всего ссылок в MongoDB: {len(processed_urls)}")

    # Находим пропущенные ссылки
    missing_urls = expected_urls - processed_urls
    extra_urls = processed_urls - expected_urls

    print(f"\n{'='*60}")
    print(f"СТАТИСТИКА:")
    print(f"{'='*60}")
    print(f"Ожидалось обработать: {len(expected_urls)}")
    print(f"Обработано: {len(processed_urls)}")
    print(f"Пропущено: {len(missing_urls)}")
    print(f"Лишних (не из исходного файла): {len(extra_urls)}")
    print(f"Процент обработки: {len(processed_urls) / len(expected_urls) * 100:.1f}%")

    if missing_urls:
        print(f"\n{'='*60}")
        print(f"ПРОПУЩЕННЫЕ ССЫЛКИ ({len(missing_urls)}):")
        print(f"{'='*60}")
        
        # Группируем по ЖК
        missing_by_building = {}
        for building in buildings:
            building_title = building.get('title', 'Неизвестный ЖК')
            apartments_links = building.get('apartments', [])
            for url in apartments_links:
                if url in missing_urls:
                    if building_title not in missing_by_building:
                        missing_by_building[building_title] = []
                    missing_by_building[building_title].append(url)
        
        for building_title, urls in missing_by_building.items():
            print(f"\n{building_title} ({len(urls)} пропущено):")
            for url in urls[:10]:  # Показываем первые 10
                print(f"  - {url}")
            if len(urls) > 10:
                print(f"  ... и еще {len(urls) - 10} ссылок")

    if extra_urls:
        print(f"\n{'='*60}")
        print(f"ЛИШНИЕ ССЫЛКИ (есть в MongoDB, но нет в исходном файле) ({len(extra_urls)}):")
        print(f"{'='*60}")
        for url in list(extra_urls)[:20]:  # Показываем первые 20
            print(f"  - {url}")
        if len(extra_urls) > 20:
            print(f"  ... и еще {len(extra_urls) - 20} ссылок")

    # Статистика по ЖК
    print(f"\n{'='*60}")
    print(f"СТАТИСТИКА ПО ЖК:")
    print(f"{'='*60}")
    
    buildings_to_reprocess = []
    
    for building in buildings:
        building_title = building.get('title', 'Неизвестный ЖК')
        apartments_links = building.get('apartments', [])
        expected_count = len(apartments_links)
        
        # Считаем, сколько из этих ссылок есть в MongoDB
        processed_count = sum(1 for url in apartments_links if url in processed_urls)
        missing_count = expected_count - processed_count
        
        if missing_count > 0:
            status = "⚠️"
            # Добавляем в список для повторной обработки только если есть пропущенные ссылки
            buildings_to_reprocess.append({
                "title": building_title,
                "link": building.get('link', ''),
                "expected": expected_count,
                "processed": processed_count,
                "missing": missing_count
            })
        elif processed_count == expected_count and expected_count > 0:
            status = "✓"
        else:
            status = "○"
            # НЕ добавляем ЖК с нулевым количеством ссылок - им нечего обрабатывать
        
        print(f"{status} {building_title}: {processed_count}/{expected_count} "
              f"({processed_count/expected_count*100:.1f}%)" if expected_count > 0 else f"{status} {building_title}: 0/0")

    # Сохраняем список ЖК для повторной обработки
    if buildings_to_reprocess:
        try:
            with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
                json.dump(buildings_to_reprocess, f, ensure_ascii=False, indent=2)
            print(f"\n{'='*60}")
            print(f"Сохранен список ЖК для повторной обработки: {OUTPUT_FILE}")
            print(f"Всего ЖК для доработки: {len(buildings_to_reprocess)}")
        except Exception as e:
            print(f"\n⚠️ Ошибка при сохранении списка для повторной обработки: {e}")
    else:
        print(f"\n{'='*60}")
        print("Все ЖК обработаны полностью! Нет необходимости в повторной обработке.")

    client.close()
    print(f"\n{'='*60}")
    print("Проверка завершена")


if __name__ == "__main__":
    main()

