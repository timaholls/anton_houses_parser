#!/usr/bin/env python3
"""
Загружает данные из cian/cian_apartments_data.json в MongoDB коллекцию unified_houses_2.

Логика:
1. Читает JSON, сформированный скриптом cian/cian_3.py.
2. Для каждого ЖК формирует документ и апсертом (replace_one) сохраняет в коллекцию unified_houses_2.
3. Совпадения определяются по полю building_link (если его нет, берется building_title).
"""

import os
import json
from pathlib import Path
from datetime import datetime, timezone
from typing import Any, Dict, List

from pymongo import MongoClient, ReplaceOne
from dotenv import load_dotenv

PROJECT_ROOT = Path(__file__).resolve().parent
DATA_FILE = PROJECT_ROOT / "cian" / "cian_apartments_data.json"
COLLECTION_NAME = "unified_houses_2"

# Загружаем .env, чтобы подтянуть параметры подключения
load_dotenv(dotenv_path=PROJECT_ROOT / ".env")


def get_mongo_collection():
    """Возвращает объект коллекции unified_houses_2."""
    mongo_uri = os.getenv("MONGO_URI", "mongodb://root:Kfleirb_17@176.98.177.188:27017/admin")
    db_name = os.getenv("DB_NAME", "houses")
    client = MongoClient(mongo_uri)
    db = client[db_name]
    return db[COLLECTION_NAME]


def load_cian_data(path: Path = DATA_FILE) -> List[Dict[str, Any]]:
    """Загружает данные из JSON файла CIAN."""
    if not path.exists():
        raise FileNotFoundError(f"Файл с данными не найден: {path}")

    with open(path, "r", encoding="utf-8") as f:
        data = json.load(f)

    if not isinstance(data, list):
        raise ValueError(f"Ожидался список зданий, получено: {type(data)}")

    return data


def prepare_documents(buildings: List[Dict[str, Any]]) -> List[ReplaceOne]:
    """Формирует bulk операции ReplaceOne для upsert по каждому ЖК."""
    operations: List[ReplaceOne] = []
    now_iso = datetime.now(timezone.utc).isoformat()

    for building in buildings:
        document = dict(building)
        document["source"] = "cian"
        document["updatedAt"] = now_iso

        # Ключ для апсерта: сначала ссылка, если нет — название
        building_link = document.get("building_link")
        building_title = document.get("building_title")
        if not building_link and not building_title:
            raise ValueError("В одном из объектов нет полей building_link / building_title — невозможно определить ключ.")

        query = {"building_link": building_link} if building_link else {"building_title": building_title}
        operations.append(ReplaceOne(query, document, upsert=True))

    return operations


def upload_to_mongo() -> None:
    """Основная функция загрузки данных в MongoDB."""
    buildings = load_cian_data()
    if not buildings:
        print("Файл пуст — нечего загружать.")
        return

    collection = get_mongo_collection()
    operations = prepare_documents(buildings)

    if not operations:
        print("Нет подготовленных документов для записи.")
        return

    result = collection.bulk_write(operations, ordered=False)

    upserted = len(result.upserted_ids) if result.upserted_ids else 0
    modified = result.modified_count

    print(f"Готово. Новых документов: {upserted}, обновлено: {modified}")


if __name__ == "__main__":
    upload_to_mongo()

