#!/usr/bin/env python3
"""
Скрипт для просмотра структуры коллекций DomRF, Avito и DomClick.
Выводит по одной записи из каждой коллекции для анализа структуры данных.
"""
from pymongo import MongoClient
import json
from bson import ObjectId

# Статические настройки подключения
MONGO_URI = "mongodb://root:Kfleirb_17@176.98.177.188:27017/admin"
DB_NAME = "houses"
COLLECTIONS = {
    "DomRF": "domrf",
    "Avito": "avito",
    "DomClick": "domclick"
}

class MongoJSONEncoder(json.JSONEncoder):
    """Позволяет сериализовать ObjectId и другие BSON-типы"""
    def default(self, obj):
        if isinstance(obj, ObjectId):
            return str(obj)
        return super().default(obj)

def show_sample_records():
    """Выводит по одной записи из каждой коллекции"""
    print("🔍 ПРОСМОТР СТРУКТУРЫ КОЛЛЕКЦИЙ")
    print("=" * 100)

    try:
        # Подключаемся к базе
        client = MongoClient(MONGO_URI)
        db = client[DB_NAME]

        sources = {
            "DomRF": db[COLLECTIONS["DomRF"]],
            "Avito": db[COLLECTIONS["Avito"]],
            "DomClick": db[COLLECTIONS["DomClick"]],
        }

        for name, collection in sources.items():
            print(f"\n📦 {name} ({collection.name})")
            print("-" * 100)
            record = collection.find_one({})
            if record:
                # Красиво выводим JSON с поддержкой ObjectId
                print(json.dumps(record, ensure_ascii=False, indent=2, cls=MongoJSONEncoder))
            else:
                print("❌ Коллекция пуста!")

        client.close()
        print("\n✅ Готово! Данные выведены.")

    except Exception as e:
        print(f"❌ Ошибка: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    show_sample_records()
