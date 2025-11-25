#!/usr/bin/env python3
"""
Создает коллекцию unified_houses_3, объединяя квартиры из unified_houses и unified_houses_2.
Основная логика:
    * Базой служит запись из unified_houses (сохраняем все поля без изменений)
    * Квартиры из unified_houses_2 добавляем только в те типы, где они реально нужны
    * Проверяем количество квартир по типам (Студии, 1-комн и т.д.)
    * Не трогаем тип, если в unified_houses уже больше квартир и нет явной нехватки
    * Если ЖК отсутствует в unified_houses_2 — просто копируем оригинальную запись
"""

import argparse
import copy
import os
import re
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from dotenv import load_dotenv
from pymongo import MongoClient

PROJECT_ROOT = Path(__file__).resolve().parent
load_dotenv(PROJECT_ROOT / ".env")

UNIFIED_COLLECTION = "unified_houses"
CIAN_COLLECTION = "unified_houses_2"
TARGET_COLLECTION = "unified_houses_3"
DEFAULT_THRESHOLD = 15

# ЖК, для которых всегда делаем полную замену квартир данными из unified_houses_2
FORCED_REPLACE_NAMES = {
    "жк 8 nebo",
    "жк 8 марта",
    "жк atlantis atlantis",
    "жк акварель",
    "жк зубово life garden",
    "жк квартал родина парк",
    "жк космос",
    "жк новый империал",
    "жк семейный",
    "жк экогород яркий",
}

# ЖК, которые копируем как есть из unified_houses (без объединения и без замены)
COPY_ONLY_NAMES = {
    "жк холмогоры",
    "жк цветы башкирии",
}

def get_mongo_connection():
    """Получает подключение к MongoDB, используя .env или значения по умолчанию."""
    mongo_uri = os.getenv("MONGO_URI", "mongodb://root:Kfleirb_17@176.98.177.188:27017/admin")
    db_name = os.getenv("DB_NAME", "houses")
    client = MongoClient(mongo_uri)
    return client[db_name]
# mongodump --uri="mongodb://root:Kfleirb_17@176.98.177.188:27017/houses?authSource=admin" --out="/home/art/Документы/mongo_bac/houses-$(date +%F)"

def normalize_name(name: Optional[str]) -> str:
    """Нормализует название ЖК для поиска соответствий."""
    if not name:
        return ""
    cleaned = name.lower()
    cleaned = cleaned.replace("«", "").replace("»", "")
    cleaned = re.sub(r"[^a-zа-я0-9]+", " ", cleaned)
    return " ".join(cleaned.split())


def parse_rooms_from_title(title: str) -> Optional[int]:
    """Извлекает количество комнат из названия квартиры."""
    if not title:
        return None

    title_lower = title.lower()
    if "студия" in title_lower or "studio" in title_lower:
        return 0

    patterns = [
        r"(\d+)[-\s]*комн",
        r"(\d+)[-\s]*к\.",
        r"(\d+)[-\s]*ком",
        r"^(\d+)[-\s]*комн",
    ]

    for pattern in patterns:
        match = re.search(pattern, title_lower)
        if match:
            try:
                rooms = int(match.group(1))
                if 1 <= rooms <= 10:
                    return rooms
            except ValueError:
                continue
    return None


def parse_floor_info(floor_str: str) -> Tuple[Optional[int], Optional[int]]:
    """Парсит строку этажа: '12 из 32', '14/27', '5-10', '12'."""
    if not floor_str:
        return None, None

    patterns = [
        r"(\d+)\s+из\s+(\d+)",
        r"(\d+)/(\d+)",
        r"(\d+)-(\d+)",
        r"(\d+)",
    ]

    for pattern in patterns:
        match = re.search(pattern, floor_str)
        if match:
            try:
                numbers = [int(group) for group in match.groups()]
                if len(numbers) == 2:
                    return numbers[0], numbers[1]
                return numbers[0], numbers[0]
            except ValueError:
                continue

    return None, None


def parse_area_from_string(area_str: str) -> Tuple[Optional[str], Optional[float]]:
    """Парсит площадь вида '57,03 м²'."""
    if not area_str:
        return None, None

    match = re.search(r"(\d+[,.]?\d*)\s*м²", area_str)
    if not match:
        return None, None

    value = match.group(1).replace(",", ".")
    try:
        return value, float(value)
    except ValueError:
        return None, None


def extract_factoid_value(factoids: List[Dict[str, Any]], label: str) -> Optional[str]:
    """Возвращает значение из factoids по label."""
    for factoid in factoids or []:
        if factoid.get("label") == label:
            return factoid.get("value")
    return None


def extract_summary_value(summary_info: List[Dict[str, Any]], label: str) -> Optional[str]:
    """Возвращает значение из summary_info по label."""
    for item in summary_info or []:
        if item.get("label") == label:
            return item.get("value")
    return None


def map_rooms_to_type_label(rooms: int) -> str:
    """Преобразует количество комнат в ключ apartment_types."""
    if rooms <= 0:
        return "Студия"
    if rooms >= 5:
        return "5-комн"
    return str(rooms)


def convert_cian_apartment(cian_apt: Dict[str, Any]) -> Optional[Tuple[str, Dict[str, Any]]]:
    """Конвертирует квартиру CIAN в формат unified_houses и возвращает (тип, данные)."""
    title = cian_apt.get("title", "")
    rooms = parse_rooms_from_title(title)
    if rooms is None:
        return None

    main_photo = cian_apt.get("main_photo")
    if not main_photo:
        return None

    apartment: Dict[str, Any] = {
        "title": title,
        "url": cian_apt.get("url"),
        "price": cian_apt.get("price"),
        "pricePerSquare": cian_apt.get("price_per_square"),
        "image": [main_photo],
        "images_apartment": [main_photo],  # Для совместимости с новым интерфейсом
    }

    factoids = cian_apt.get("factoids", [])
    area_str = extract_factoid_value(factoids, "Общая площадь")
    if area_str:
        area_value, area_float = parse_area_from_string(area_str)
        if area_value:
            apartment["area"] = area_value
        if area_float is not None:
            apartment["totalArea"] = area_float

    completion = extract_factoid_value(factoids, "Год сдачи")
    if completion:
        apartment["completionDate"] = completion

    floor_str = extract_factoid_value(factoids, "Этаж")
    if floor_str:
        floor_min, floor_max = parse_floor_info(floor_str)
        if floor_min is not None:
            apartment["floorMin"] = floor_min
        if floor_max is not None:
            apartment["floorMax"] = floor_max

    living_area = extract_factoid_value(factoids, "Жилая площадь")
    if living_area:
        apartment["livingArea"] = living_area.replace(" м²", "").replace(",", ".")

    kitchen_area = extract_factoid_value(factoids, "Площадь кухни")
    if kitchen_area:
        apartment["kitchenArea"] = kitchen_area.replace(" м²", "").replace(",", ".")

    decoration_type = extract_factoid_value(factoids, "Отделка")
    if decoration_type:
        apartment["decorationType"] = decoration_type

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

    decoration = cian_apt.get("decoration", {})
    if decoration:
        decor_obj = {
            "description": decoration.get("description", ""),
            "photos": decoration.get("photos", []),
        }
        if decor_obj["description"] or decor_obj["photos"]:
            apartment["decoration"] = decor_obj

    return map_rooms_to_type_label(rooms), apartment


def build_type_completion_defaults(record: Dict[str, Any]) -> Dict[str, str]:
    """
    Собирает стандартные completionDate для каждого типа планировок из базовой записи.
    Берем первое непустое значение внутри соответствующего apartment_types.<type>.apartments.
    """
    defaults: Dict[str, str] = {}
    apartment_types = record.get("apartment_types", {})
    for apt_type, data in apartment_types.items():
        for apartment in data.get("apartments", []):
            completion = (
                apartment.get("completionDate")
                or apartment.get("completion_date")
                or apartment.get("completion_date_range")
            )
            if completion:
                defaults.setdefault(apt_type, completion)
                break
    return defaults


def ensure_completion_date(apartment: Dict[str, Any], fallback: Optional[str]) -> None:
    """Всегда предпочитаем оригинальный срок сдачи (fallback) если он есть."""
    if fallback:
        apartment["completionDate"] = fallback
        return

    current = (
        apartment.get("completionDate")
        or apartment.get("completion_date")
        or apartment.get("completion_date_range")
    )
    if current:
        apartment["completionDate"] = current


def build_cian_groups(cian_record: Dict[str, Any]) -> Dict[str, List[Dict[str, Any]]]:
    """Группирует квартиры CIAN по типам (Студия, 1, 2...)."""
    grouped: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
    for raw_apartment in cian_record.get("apartments", []):
        converted = convert_cian_apartment(raw_apartment)
        if not converted:
            continue
        apt_type, apt_data = converted
        grouped[apt_type].append(apt_data)
    return grouped


def replace_apartments(
    base_record: Dict[str, Any],
    cian_record: Dict[str, Any],
) -> Tuple[int, int, List[str]]:
    """
    Полностью заменяет apartment_types данными из CIAN.
    Используется для ЖК из "принудительного" списка.
    """
    logs: List[str] = []
    old_total = sum(
        len(data.get("apartments", []))
        for data in base_record.get("apartment_types", {}).values()
    )
    cian_groups = build_cian_groups(cian_record)
    completion_defaults = build_type_completion_defaults(base_record)

    new_apartment_types = {}
    for apt_type, apartments in cian_groups.items():
        fallback = completion_defaults.get(apt_type)
        for apt in apartments:
            ensure_completion_date(apt, fallback)
        new_apartment_types[apt_type] = {"apartments": apartments}

    base_record["apartment_types"] = new_apartment_types

    total_after = sum(
        len(data.get("apartments", []))
        for data in base_record.get("apartment_types", {}).values()
    )
    base_record["updated_at"] = datetime.now(timezone.utc)

    logs.append(f"  🔄 Замена квартир: было {old_total}, стало {total_after}")

    added = max(total_after - old_total, 0)
    return added, total_after, logs


def should_merge_type(unified_count: int, cian_count: int, threshold: int) -> bool:
    """
    Определяет, стоит ли объединять конкретный тип квартир.
    Логика:
        * если новых квартир нет — ничего не делаем
        * если в unified_houses нет квартир — добавляем
        * если в обеих записях мало квартир (<= threshold) — объединяем
        * если в CIAN больше квартир — дополняем
        * иначе (в unified_houses уже больше) — пропускаем
    """
    if cian_count == 0:
        return False
    if unified_count == 0:
        return True
    if unified_count <= threshold and cian_count <= threshold:
        return True
    if cian_count > unified_count:
        return True
    return False


def merge_apartments(
    base_record: Dict[str, Any],
    cian_record: Dict[str, Any],
    threshold: int,
) -> Tuple[int, int, List[str]]:
    """
    Объединяет квартиры в base_record (unified_houses) с CIAN-данными.
    Возвращает:
        (кол-во добавленных квартир, итоговое количество квартир, список логов)
    """
    logs: List[str] = []
    base_types = copy.deepcopy(base_record.get("apartment_types", {}))
    cian_groups = build_cian_groups(cian_record)
    type_completion_defaults = build_type_completion_defaults(base_record)

    added_total = 0
    for apt_type, cian_apartments in cian_groups.items():
        cian_count = len(cian_apartments)
        unified_apartments = base_types.get(apt_type, {}).get("apartments", [])
        unified_count = len(unified_apartments)

        if not type_completion_defaults.get(apt_type) and unified_apartments:
            existing_completion = next(
                (apt.get("completionDate") for apt in unified_apartments if apt.get("completionDate")),
                None,
            )
            if existing_completion:
                type_completion_defaults[apt_type] = existing_completion

        if not should_merge_type(unified_count, cian_count, threshold):
            logs.append(
                f"  ✋ {apt_type}: пропускаю (unified={unified_count}, cian={cian_count})"
            )
            continue

        urls_in_unified = {
            apt.get("url") for apt in unified_apartments if apt.get("url")
        }
        added_here = 0
        for apartment in cian_apartments:
            url = apartment.get("url")
            ensure_completion_date(apartment, type_completion_defaults.get(apt_type))
            if url and url in urls_in_unified:
                continue
            unified_apartments.append(apartment)
            if url:
                urls_in_unified.add(url)
            added_here += 1

        if added_here:
            base_types.setdefault(apt_type, {"apartments": []})
            base_types[apt_type]["apartments"] = unified_apartments
            logs.append(
                f"  ➕ {apt_type}: добавлено {added_here} (было {unified_count}, стало {len(unified_apartments)})"
            )
        else:
            logs.append(
                f"  ⚖️ {apt_type}: новые квартиры уже есть (дубликаты), всего {unified_count}"
            )
        added_total += added_here

    base_record["apartment_types"] = base_types
    total_after = sum(
        len(data.get("apartments", [])) for data in base_types.values()
    )
    if added_total:
        base_record["updated_at"] = datetime.now(timezone.utc)
    return added_total, total_after, logs


def load_replace_targets(args) -> Tuple[set, set]:
    """
    Возвращает два множества:
        * replace_targets — ЖК, где нужно полностью заменить квартиры
        * copy_only_targets — ЖК, которые копируем без изменений
    """
    replace_targets = set(FORCED_REPLACE_NAMES)
    copy_only_targets = set(COPY_ONLY_NAMES)

    def add_to_set(names_iterable, target_set):
        for name in names_iterable or []:
            normalized = normalize_name(name)
            if normalized:
                target_set.add(normalized)

    # Дополнительные значения из аргументов (если всё же нужно)
    add_to_set(getattr(args, "replace", None), replace_targets)

    replace_file = getattr(args, "replace_file", None)
    if replace_file:
        path = Path(replace_file)
        if path.exists():
            lines = [
                line.strip()
                for line in path.read_text(encoding="utf-8").splitlines()
                if line.strip() and not line.startswith("#")
            ]
            add_to_set(lines, replace_targets)
        else:
            print(f"⚠️ Файл со списком для замены не найден: {path}")

    return replace_targets, copy_only_targets


def build_cian_index(cian_records: List[Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
    """Создает индекс CIAN-записей по названию ЖК."""
    index: Dict[str, Dict[str, Any]] = {}
    for record in cian_records:
        title = record.get("building_title") or record.get("development", {}).get("name")
        normalized = normalize_name(title)
        if normalized:
            index[normalized] = record
    return index


def process_records(args) -> None:
    """Основной процесс слияния коллекций."""
    db = get_mongo_connection()
    unified_col = db[UNIFIED_COLLECTION]
    cian_col = db[CIAN_COLLECTION]
    target_col = db[TARGET_COLLECTION]

    unified_filter = {}
    if args.building:
        unified_filter["development.name"] = {"$regex": args.building, "$options": "i"}

    unified_records = list(unified_col.find(unified_filter))
    cian_records = list(cian_col.find({}))
    cian_index = build_cian_index(cian_records)

    replace_targets, copy_only_targets = load_replace_targets(args)

    stats = {
        "processed": 0,
        "copied": 0,
        "merged": 0,
        "replaced": 0,
        "added_apartments": 0,
        "skipped": 0,
    }

    print(f"🔁 Обрабатываем {len(unified_records)} записей из {UNIFIED_COLLECTION}")
    print(f"📦 Всего записей в {CIAN_COLLECTION}: {len(cian_records)}")
    print(f"🎯 Порог 'мало квартир' = {args.threshold}")
    if args.dry_run:
        print("🧪 DRY-RUN: изменения НЕ записываются в базу")
    print("-" * 100)

    for record in unified_records:
        stats["processed"] += 1
        base_name = (
            record.get("development", {}).get("name")
            or record.get("name")
            or "Без названия"
        )
        normalized = normalize_name(base_name)
        cian_record = None
        action_note = ""
        if normalized in copy_only_targets:
            action_note = " (copy-only)"
        else:
            cian_record = cian_index.get(normalized)
        base_copy = copy.deepcopy(record)

        replace_mode = normalized in replace_targets

        if not cian_record:
            stats["copied"] += 1
            action = "📋 Копирую без изменений" + action_note
        else:
            if replace_mode:
                added, total_after, logs = replace_apartments(base_copy, cian_record)
                stats["added_apartments"] += added
                stats["replaced"] += 1
                action = f"♻️ Полная замена: {total_after} квартир"
            else:
                added, total_after, logs = merge_apartments(base_copy, cian_record, args.threshold)
                stats["added_apartments"] += added
                if added:
                    stats["merged"] += 1
                    action = f"🔗 Объединено, добавлено {added} квартир (итого {total_after})"
                else:
                    stats["skipped"] += 1
                    action = "⚠️ Совпало по названию, но добавлять нечего"

        print(f"🏢 {base_name} — {action}")
        if cian_record and logs:
            for line in logs:
                print(line)

        if not args.dry_run:
            target_col.replace_one({"_id": base_copy["_id"]}, base_copy, upsert=True)

    print("-" * 100)
    print("📈 ИТОГ:")
    print(f"  Всего обработано:       {stats['processed']}")
    print(f"  Скопировано как есть:   {stats['copied']}")
    print(f"  Объединено:             {stats['merged']}")
    print(f"  Заменено по списку:     {stats['replaced']}")
    print(f"  Добавлено квартир:      {stats['added_apartments']}")
    print(f"  Совпадений без изменений: {stats['skipped']}")
    if args.dry_run:
        print("🧪 DRY-RUN завершен, коллекция не изменялась")
    else:
        print(f"✅ Данные записаны в коллекцию {TARGET_COLLECTION}")


def parse_args():
    parser = argparse.ArgumentParser(
        description="Объединяет unified_houses и unified_houses_2 в unified_houses_3"
    )
    parser.add_argument(
        "--building",
        help="Фильтр по названию ЖК (регулярное выражение)",
    )
    parser.add_argument(
        "--threshold",
        type=int,
        default=DEFAULT_THRESHOLD,
        help="Порог 'мало квартир' для объединения типов (по умолчанию 10)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Запустить без записи в базу",
    )
    parser.add_argument(
        "--replace",
        action="append",
        help="Название ЖК (можно несколько флагов), для которых квартиры нужно заменить целиком",
    )
    parser.add_argument(
        "--replace-file",
        help="Путь к файлу со списком ЖК для полной замены (по одному названию в строке)",
    )
    return parser.parse_args()


def main():
    args = parse_args()
    process_records(args)


if __name__ == "__main__":
    main()

