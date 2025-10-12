#!/usr/bin/env python3
"""
Скрипт для вывода совпавших названий ЖК из объединенной коллекции.
Показывает, какие названия совпали в DomRF, Avito и DomClick.
"""
import os
from pathlib import Path
from pymongo import MongoClient
from dotenv import load_dotenv

# Корневая директория проекта
PROJECT_ROOT = Path(__file__).resolve().parent

# Загружаем настройки из domrf/.env (используем те же параметры подключения)
env_path = PROJECT_ROOT / "domrf" / ".env"
load_dotenv(dotenv_path=env_path)

MONGO_URI = os.getenv("MONGO_URI")
DB_NAME = os.getenv("DB_NAME")
UNIFIED_COLLECTION_NAME = "unified_houses"


def show_unified_matches():
    """Выводит названия из трех источников"""
    print("🔍 ВЫВОД СОВПАВШИХ ЗАПИСЕЙ")
    print("=" * 80)

    try:
        client = MongoClient(MONGO_URI)
        db = client[DB_NAME]
        collection = db[UNIFIED_COLLECTION_NAME]

        records = list(collection.find({}))
        print(f"📊 Всего записей в объединенной коллекции: {len(records)}\n")

        if not records:
            print("❌ Коллекция пуста.")
            return

        for i, rec in enumerate(records, 1):
            domrf_name = rec.get("domrf", {}).get("name") or rec.get("domrf_data", {}).get("objCommercNm", "N/A")
            avito_name = rec.get("avito", {}).get("development", {}).get("name") or rec.get("avito_data", {}).get(
                "development", {}).get("name", "N/A")
            domclick_name = rec.get("domclick", {}).get("development", {}).get("complex_name") or rec.get(
                "domclick_data", {}).get("development", {}).get("complex_name", "N/A")

            print(f"📋 {i:3d}.")
            print(f"  🏠 DomRF:    {domrf_name}")
            print(f"  🏪 Avito:    {avito_name}")
            print(f"  🏢 DomClick: {domclick_name}")
            print("-" * 60)

        client.close()
        print("\n✅ Готово! Все совпавшие записи выведены.")

    except Exception as e:
        print(f"❌ Ошибка при работе с MongoDB: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    show_unified_matches()
