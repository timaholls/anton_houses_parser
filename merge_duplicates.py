#!/usr/bin/env python3
"""
Утилита для объединения дублирующихся записей в коллекции DomClick.
Группирует записи по slug из URL или по названию комплекса и оставляет одну,
объединяя данные полей и удаляя дубликаты.
"""

from collections import defaultdict
from datetime import datetime
from typing import Callable, Dict, List, Optional, Tuple

from pymongo.collection import Collection

from domclick.db_manager import (
    DB_NAME,
    COLLECTION_NAME,
    compare_and_merge_data,
    extract_slug_from_url,
    get_mongo_client,
    normalize_complex_name,
)


def sort_priority(doc: Dict) -> Tuple[int, datetime]:
    """Определяет приоритет записи при выборе основной."""
    total_apartments = doc.get('total_apartments') or 0
    updated_at = doc.get('updated_at')
    if isinstance(updated_at, datetime):
        dt = updated_at
    else:
        dt = datetime.min
    return total_apartments, dt


def merge_duplicates(collection: Collection) -> None:
    total_groups = 0
    total_deleted = 0

    # Сначала объединяем по slug, затем по названию
    for label, key_func in (
        ("slug", lambda d: _slug_key(d)),
        ("Название ЖК", lambda d: _name_key(d)),
    ):
        merged, deleted = _merge_by_key(collection, key_func, label)
        total_groups += merged
        total_deleted += deleted

    print(f"\n✅ Обработано групп: {total_groups}")
    print(f"🗑️ Удалено дубликатов: {total_deleted}")


def _id_str(doc: Dict) -> str:
    return str(doc.get('_id'))


def _slug_key(doc: Dict) -> Optional[str]:
    slug = extract_slug_from_url(doc.get('url'))
    return slug.lower() if slug else None


def _name_key(doc: Dict) -> Optional[str]:
    name = doc.get('development', {}).get('complex_name')
    normalized_name = normalize_complex_name(name)
    if normalized_name:
        return normalized_name
    normalized_name = doc.get('normalized_complex_name')
    return normalized_name


def _merge_by_key(
    collection: Collection,
    key_func: Callable[[Dict], Optional[str]],
    label: str,
) -> Tuple[int, int]:
    docs = list(collection.find({}))
    groups: Dict[str, List[Dict]] = defaultdict(list)

    for doc in docs:
        key = key_func(doc)
        if key:
            groups[key].append(doc)

    merged_groups = 0
    deleted_records = 0

    for key, items in groups.items():
        if len(items) < 2:
            continue

        items_sorted = sorted(items, key=sort_priority, reverse=True)
        primary = items_sorted[0]
        print(f"🔄 [{label}] {key} — {len(items_sorted)} записей. Оставляем {_id_str(primary)}")

        for duplicate in items_sorted[1:]:
            merged_data, changes = compare_and_merge_data(primary, duplicate)
            if changes:
                collection.update_one({'_id': primary['_id']}, {'$set': merged_data})
                primary = merged_data

            collection.delete_one({'_id': duplicate['_id']})
            deleted_records += 1
            print(f"   ✂️ Удалён дубликат {_id_str(duplicate)}")

        merged_groups += 1

    return merged_groups, deleted_records


def main():
    client = get_mongo_client()
    if not client:
        print("❌ Не удалось подключиться к MongoDB")
        return

    try:
        collection = client[DB_NAME][COLLECTION_NAME]
        merge_duplicates(collection)
    finally:
        client.close()


if __name__ == "__main__":
    main()

