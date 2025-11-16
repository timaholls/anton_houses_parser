#!/usr/bin/env python3
"""
Проставляет случайный рейтинг (4 или 5) для всех записей unified_houses.
"""

import os
import random
from datetime import datetime, timezone

from pymongo import MongoClient


def get_mongo_connection():
    mongo_uri = os.getenv("MONGO_URI", "mongodb://root:Kfleirb_17@176.98.177.188:27017/admin")
    db_name = os.getenv("DB_NAME", "houses")
    client = MongoClient(mongo_uri)
    return client[db_name]


def main():
    db = get_mongo_connection()
    unified_col = db["unified_houses"]

    print("🔧 Обновляем рейтинги для unified_houses...")

    updated = 0
    now = datetime.now(timezone.utc)

    for record in unified_col.find({}, {"_id": 1, "rating_created_at": 1}):
        rating = random.choice([4, 5])
        created_at = record.get("rating_created_at") or now
        unified_col.update_one(
            {"_id": record["_id"]},
            {
                "$set": {
                    "rating": rating,
                    "rating_description": "Автогенерация",
                    "rating_created_at": created_at,
                    "rating_updated_at": now,
                }
            },
        )
        updated += 1

    print(f"✅ Обновлено записей: {updated}")


if __name__ == "__main__":
    main()


