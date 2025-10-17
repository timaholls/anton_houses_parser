#!/usr/bin/env python3
"""
Скрипт для инкрементального обновления объединенных записей unified_houses
Обновляет только те записи, где исходные данные изменились после последнего объединения
"""

import os
import sys
from datetime import datetime, timezone
from bson import ObjectId
from pymongo import MongoClient


def get_mongo_connection():
    """Получить подключение к MongoDB"""
    MONGO_URI = os.getenv("MONGO_URI", "mongodb://root:Kfleirb_17@176.98.177.188:27017/admin")
    DB_NAME = os.getenv("DB_NAME", "houses")
    client = MongoClient(MONGO_URI)
    db = client[DB_NAME]
    return db


def normalize_datetime(dt):
    """Нормализует datetime к UTC для корректного сравнения"""
    if dt is None:
        return None

    # Если datetime без timezone, добавляем UTC
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)

    # Если datetime с timezone, конвертируем в UTC
    return dt.astimezone(timezone.utc)


def get_source_timestamp(record):
    """Получает максимальную дату модификации из исходных записей"""
    max_timestamp = None
    updated_records = []

    # Проверяем DomRF
    if record.get('_source_ids', {}).get('domrf'):
        try:
            domrf_id = ObjectId(record['_source_ids']['domrf'])
            db = get_mongo_connection()
            domrf_record = db['domrf'].find_one({'_id': domrf_id})
            if domrf_record:
                if domrf_record.get('updated_at'):
                    normalized_dt = normalize_datetime(domrf_record['updated_at'])
                    if not max_timestamp or normalized_dt > max_timestamp:
                        max_timestamp = normalized_dt
                else:
                    # Добавляем updated_at если его нет
                    print(f"🔄 Добавляем updated_at для DomRF {domrf_id}")
                    current_time = datetime.now(timezone.utc)
                    db['domrf'].update_one(
                        {'_id': domrf_id},
                        {'$set': {'updated_at': current_time}}
                    )
                    updated_records.append(f"DomRF {domrf_id}")
                    if not max_timestamp or current_time > max_timestamp:
                        max_timestamp = current_time
        except Exception as e:
            print(f"Ошибка получения DomRF {domrf_id}: {e}")

    # Проверяем Avito
    if record.get('_source_ids', {}).get('avito'):
        try:
            avito_id = ObjectId(record['_source_ids']['avito'])
            db = get_mongo_connection()
            avito_record = db['avito'].find_one({'_id': avito_id})
            if avito_record:
                if avito_record.get('updated_at'):
                    normalized_dt = normalize_datetime(avito_record['updated_at'])
                    if not max_timestamp or normalized_dt > max_timestamp:
                        max_timestamp = normalized_dt
                else:
                    # Добавляем updated_at если его нет
                    print(f"🔄 Добавляем updated_at для Avito {avito_id}")
                    current_time = datetime.now(timezone.utc)
                    db['avito'].update_one(
                        {'_id': avito_id},
                        {'$set': {'updated_at': current_time}}
                    )
                    updated_records.append(f"Avito {avito_id}")
                    if not max_timestamp or current_time > max_timestamp:
                        max_timestamp = current_time
        except Exception as e:
            print(f"Ошибка получения Avito {avito_id}: {e}")

    # Проверяем DomClick
    if record.get('_source_ids', {}).get('domclick'):
        try:
            domclick_id = ObjectId(record['_source_ids']['domclick'])
            db = get_mongo_connection()
            domclick_record = db['domclick'].find_one({'_id': domclick_id})
            if domclick_record:
                if domclick_record.get('updated_at'):
                    normalized_dt = normalize_datetime(domclick_record['updated_at'])
                    if not max_timestamp or normalized_dt > max_timestamp:
                        max_timestamp = normalized_dt
                else:
                    # Добавляем updated_at если его нет
                    print(f"🔄 Добавляем updated_at для DomClick {domclick_id}")
                    current_time = datetime.now(timezone.utc)
                    db['domclick'].update_one(
                        {'_id': domclick_id},
                        {'$set': {'updated_at': current_time}}
                    )
                    updated_records.append(f"DomClick {domclick_id}")
                    if not max_timestamp or current_time > max_timestamp:
                        max_timestamp = current_time
        except Exception as e:
            print(f"Ошибка получения DomClick {domclick_id}: {e}")

    if updated_records:
        print(f"✅ Обновлены записи: {', '.join(updated_records)}")

    return max_timestamp


def rebuild_unified_record(unified_record):
    """Пересоздает объединенную запись ТОЧНО ПО ЛОГИКЕ save_manual_match"""
    db = get_mongo_connection()

    # Получаем исходные записи
    source_ids = unified_record.get('_source_ids', {})
    print(f"🔍 Source IDs: {source_ids}")

    domrf_record = None
    if source_ids.get('domrf'):
        try:
            domrf_record = db['domrf'].find_one({'_id': ObjectId(source_ids['domrf'])})
            print(f"📄 DomRF record found: {bool(domrf_record)}")
        except Exception as e:
            print(f"❌ Error getting DomRF: {e}")

    avito_record = None
    if source_ids.get('avito'):
        try:
            avito_record = db['avito'].find_one({'_id': ObjectId(source_ids['avito'])})
            print(f"📄 Avito record found: {bool(avito_record)}")
        except Exception as e:
            print(f"❌ Error getting Avito: {e}")

    domclick_record = None
    if source_ids.get('domclick'):
        try:
            domclick_record = db['domclick'].find_one({'_id': ObjectId(source_ids['domclick'])})
            print(f"📄 DomClick record found: {bool(domclick_record)}")
        except Exception as e:
            print(f"❌ Error getting DomClick: {e}")

    # Проверяем, что у нас есть хотя бы одна запись
    if not avito_record and not domclick_record:
        print(f"❌ Нет исходных записей для unified_record {unified_record['_id']}")
        return None

    # Определяем координаты (приоритет: DomRF > Avito > DomClick > существующие в unified_record)
    latitude = None
    longitude = None

    if domrf_record:
        latitude = domrf_record.get('latitude')
        longitude = domrf_record.get('longitude')
    elif avito_record:
        latitude = avito_record.get('latitude')
        longitude = avito_record.get('longitude')
    elif domclick_record:
        latitude = domclick_record.get('latitude')
        longitude = domclick_record.get('longitude')

    # Если координаты не найдены в исходных записях, берем из существующей unified_record
    if not latitude or not longitude:
        latitude = unified_record.get('latitude')
        longitude = unified_record.get('longitude')
        if latitude and longitude:
            print(f"📍 Используем существующие координаты из unified_record: ({latitude}, {longitude})")

    if not latitude or not longitude:
        print(f"⚠️ Нет координат ни в исходных записях, ни в unified_record {unified_record['_id']}")
        return None

    # === ПЕРЕСОЗДАЕМ ЗАПИСЬ С НУЛЯ ПО ТОЧНОЙ ЛОГИКЕ save_manual_match ===

    # Сохраняем старые данные для сравнения
    old_dev = unified_record.get('development', {})
    old_apt_types = unified_record.get('apartment_types', {})
    old_apt_counts = {}
    total_old_apartments = 0
    for apt_type, apt_data in old_apt_types.items():
        count = len(apt_data.get('apartments', []))
        old_apt_counts[apt_type] = count
        total_old_apartments += count

    changes = []

    # 1. Создаем НОВУЮ запись (как в save_manual_match)
    new_record = {
        'latitude': latitude,
        'longitude': longitude,
        'source': 'manual',
        'created_by': 'manual',
        'is_featured': unified_record.get('is_featured', False),
        'agent_id': unified_record.get('agent_id'),
        'updated_at': datetime.now(timezone.utc)
    }

    # Проверяем изменения координат
    if unified_record.get('latitude') != latitude or unified_record.get('longitude') != longitude:
        changes.append(
            f"📍 Координаты: ({unified_record.get('latitude')}, {unified_record.get('longitude')}) → ({latitude}, {longitude})")

    # 2. Development из Avito + photos из DomClick (ТОЧНАЯ ЛОГИКА)
    if avito_record:
        avito_dev = avito_record.get('development', {})
        if isinstance(avito_dev, dict):
            new_name = avito_dev.get('name', '')
            new_address = avito_dev.get('address', '')
            new_price = avito_dev.get('price_range', '')
            new_korpuses = avito_dev.get('korpuses', [])

            # Проверяем изменения
            if old_dev.get('name') != new_name:
                changes.append(f"🏢 Название: '{old_dev.get('name', '')}' → '{new_name}'")
            if old_dev.get('address') != new_address:
                changes.append(f"📫 Адрес: '{old_dev.get('address', '')}' → '{new_address}'")
            if old_dev.get('price_range') != new_price:
                changes.append(f"💰 Цены: '{old_dev.get('price_range', '')}' → '{new_price}'")
            if len(old_dev.get('korpuses', [])) != len(new_korpuses):
                changes.append(f"🏗️ Корпусов: {len(old_dev.get('korpuses', []))} → {len(new_korpuses)}")

            new_record['development'] = {
                'name': new_name,
                'address': new_address,
                'price_range': new_price,
                'parameters': avito_dev.get('parameters', {}),
                'korpuses': new_korpuses,
                'photos': []  # Будет заполнено из DomClick
            }

            # Добавляем фото ЖК из DomClick
            if domclick_record:
                domclick_dev = domclick_record.get('development', {})
                dev_photos = domclick_dev.get('photos', [])
                new_record['development']['photos'] = dev_photos

                old_photos_count = len(old_dev.get('photos', []))
                new_photos_count = len(dev_photos)
                if old_photos_count != new_photos_count:
                    changes.append(f"📸 Фото ЖК: {old_photos_count} → {new_photos_count}")

    # 3. Объединяем apartment_types (ТОЧНАЯ ЛОГИКА из save_manual_match)
    new_record['apartment_types'] = {}

    if avito_record and domclick_record:
        avito_apt_types = avito_record.get('apartment_types', {})
        domclick_apt_types = domclick_record.get('apartment_types', {})

        # Маппинг старых названий на новые упрощенные (ТОЧНО КАК В save_manual_match)
        name_mapping = {
            # Студия
            'Студия': 'Студия',
            # 1-комнатные (разные варианты названий из Avito и DomClick)
            '1 ком.': '1',
            '1-комн': '1',
            '1-комн.': '1',
            # 2-комнатные
            '2': '2',
            '2-комн': '2',
            '2-комн.': '2',
            # 3-комнатные
            '3': '3',
            '3-комн': '3',
            '3-комн.': '3',
            # 4-комнатные
            '4': '4',
            '4-комн': '4',
            '4-комн.': '4',
            '4-комн.+': '4',
            '4-комн+': '4'
        }

        # Сначала обрабатываем все типы из DomClick (чтобы не пропустить 1-комнатные)
        processed_types = set()
        new_apt_counts = {}

        for dc_type_name, dc_type_data in domclick_apt_types.items():
            # Упрощаем название типа
            simplified_name = name_mapping.get(dc_type_name, dc_type_name)

            # Пропускаем если уже обработали этот упрощенный тип
            if simplified_name in processed_types:
                continue
            processed_types.add(simplified_name)

            # Получаем квартиры из DomClick
            dc_apartments = dc_type_data.get('apartments', [])
            if not dc_apartments:
                continue

            # Ищем соответствующий тип в Avito
            avito_apartments = []
            for avito_type_name, avito_data in avito_apt_types.items():
                avito_simplified = name_mapping.get(avito_type_name, avito_type_name)
                if avito_simplified == simplified_name:
                    avito_apartments = avito_data.get('apartments', [])
                    break

            # Объединяем: количество квартир = количество квартир в DomClick
            combined_apartments = []

            for i, dc_apt in enumerate(dc_apartments):
                # Получаем ВСЕ фото этой квартиры из DomClick как МАССИВ
                apartment_photos = dc_apt.get('photos', [])

                # Если фото нет - пропускаем эту квартиру
                if not apartment_photos:
                    continue

                # Берем соответствующую квартиру из Avito (циклически)
                avito_apt = avito_apartments[i % len(avito_apartments)] if avito_apartments else {}

                combined_apartments.append({
                    'title': avito_apt.get('title', ''),
                    'price': avito_apt.get('price', ''),
                    'pricePerSquare': avito_apt.get('pricePerSquare', ''),
                    'completionDate': avito_apt.get('completionDate', ''),
                    'url': avito_apt.get('urlPath', ''),
                    'image': apartment_photos  # МАССИВ всех фото этой планировки!
                })

            # Добавляем в результат только если есть квартиры с фото
            if combined_apartments:
                new_record['apartment_types'][simplified_name] = {
                    'apartments': combined_apartments
                }
                new_apt_counts[simplified_name] = len(combined_apartments)

        # Логируем изменения в количестве квартир
        total_new_apartments = sum(new_apt_counts.values())
        if total_old_apartments != total_new_apartments:
            changes.append(f"🏠 Всего квартир: {total_old_apartments} → {total_new_apartments}")

        # Детализируем по типам
        all_types = set(old_apt_counts.keys()) | set(new_apt_counts.keys())
        for apt_type in sorted(all_types):
            old_count = old_apt_counts.get(apt_type, 0)
            new_count = new_apt_counts.get(apt_type, 0)
            if old_count != new_count:
                if old_count == 0:
                    changes.append(f"  ➕ {apt_type}-комн: добавлено {new_count} квартир")
                elif new_count == 0:
                    changes.append(f"  ➖ {apt_type}-комн: удалено {old_count} квартир")
                else:
                    changes.append(f"  📊 {apt_type}-комн: {old_count} → {new_count} квартир")

    # 4. Сохраняем ссылки на исходные записи
    new_record['_source_ids'] = source_ids

    # Выводим все изменения
    if changes:
        print(f"\n📝 ИЗМЕНЕНИЯ В ЗАПИСИ:")
        for change in changes:
            print(f"   {change}")
        print()
    else:
        print(f"✅ Данные актуальны, изменений нет\n")

    return new_record


def main():
    """Основная функция обновления"""
    print("🔄 Начинаем инкрементальное обновление unified_houses...")

    db = get_mongo_connection()
    unified_col = db['unified_houses']

    # Получаем все объединенные записи
    unified_records = list(unified_col.find({}))
    total_records = len(unified_records)

    print(f"📊 Найдено {total_records} объединенных записей")

    updated_count = 0
    skipped_count = 0
    error_count = 0

    for i, record in enumerate(unified_records, 1):
        try:
            print(f"\n[{i}/{total_records}] Обрабатываем: {record.get('development', {}).get('name', 'Без названия')}")

            # Получаем дату последнего обновления исходных записей
            source_timestamp = get_source_timestamp(record)

            if not source_timestamp:
                print(f"⚠️ Нет информации о дате обновления исходных записей")
                skipped_count += 1
                continue

            # Получаем дату последнего обновления объединенной записи
            unified_timestamp = record.get('updated_at', record.get('_id').generation_time)
            unified_timestamp = normalize_datetime(unified_timestamp)

            # Сравниваем даты
            if source_timestamp <= unified_timestamp:
                print(f"✅ Запись актуальна (исходные: {source_timestamp}, объединенная: {unified_timestamp})")
                skipped_count += 1
                continue

            print(f"🔄 Обновляем (исходные: {source_timestamp}, объединенная: {unified_timestamp})")

            # Пересоздаем запись
            new_record = rebuild_unified_record(record)

            if new_record:
                # Обновляем запись в базе
                result = unified_col.replace_one(
                    {'_id': record['_id']},
                    new_record
                )

                if result.modified_count > 0:
                    print(f"✅ Запись обновлена")
                    updated_count += 1
                else:
                    print(f"⚠️ Запись не изменилась")
                    skipped_count += 1
            else:
                print(f"❌ Не удалось пересоздать запись")
                error_count += 1

        except Exception as e:
            print(f"❌ Ошибка обработки записи: {e}")
            error_count += 1

    print(f"\n📊 Результаты обновления:")
    print(f"✅ Обновлено: {updated_count}")
    print(f"⏭️ Пропущено: {skipped_count}")
    print(f"❌ Ошибок: {error_count}")
    print(f"📈 Всего обработано: {updated_count + skipped_count + error_count}")


if __name__ == "__main__":
    main()
