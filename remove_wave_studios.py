#!/usr/bin/env python3
"""
Скрипт считает количество студий в ЖК «Волна» (коллекция unified_houses_2) и,
при подтверждении, удаляет их из документа.
"""

import os
from pathlib import Path
from typing import Any, Dict, List

from pymongo import MongoClient
from dotenv import load_dotenv

PROJECT_ROOT = Path(__file__).resolve().parent
load_dotenv(dotenv_path=PROJECT_ROOT / ".env")

MONGO_URI = os.getenv("MONGO_URI", "mongodb://root:Kfleirb_17@176.98.177.188:27017/admin")
DB_NAME = os.getenv("DB_NAME", "houses")
COLLECTION_NAME = "unified_houses_2"
BUILDING_TITLE = "ЖК «Волна»"


def is_studio(apartment: Dict[str, Any]) -> bool:
    """Определяет, является ли квартира студией."""
    rooms = apartment.get("rooms")
    title = (apartment.get("title") or "").lower()

    # Студии обычно записаны как 0, "0" или содержат слово "студия" в названии
    if rooms in (0, "0", "студия", "studio"):
        return True
    return "студия" in title or "studio" in title


def main() -> None:
    client = MongoClient(MONGO_URI)
    collection = client[DB_NAME][COLLECTION_NAME]

    building = collection.find_one({"building_title": BUILDING_TITLE})
    if not building:
        print(f"❌ Не найден документ с building_title = {BUILDING_TITLE}")
        client.close()
        return

    apartments: List[Dict[str, Any]] = building.get("apartments", [])
    studios = [apt for apt in apartments if is_studio(apt)]

    print(f"ЖК: {BUILDING_TITLE}")
    print(f"Всего квартир: {len(apartments)}")
    print(f"Студий найдено: {len(studios)}")

    if not studios:
        print("Удалять нечего — студий нет.")
        client.close()
        return

    answer = input("Удалить все студии из ЖК «Волна»? (yes/no): ").strip().lower()
    if answer not in ("yes", "y", "да", "д"):
        print("Отмена. Ничего не удалено.")
        client.close()
        return

    remaining_apartments = [apt for apt in apartments if not is_studio(apt)]
    result = collection.update_one(
        {"_id": building["_id"]},
        {"$set": {"apartments": remaining_apartments}}
    )

    if result.modified_count:
        print(f"✓ Удалено студий: {len(studios)}. Осталось квартир: {len(remaining_apartments)}")
    else:
        print("⚠️ Не удалось обновить документ.")

    client.close()


if __name__ == "__main__":
    main()

