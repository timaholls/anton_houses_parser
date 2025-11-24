#!/usr/bin/env python3
"""
Скрипт для сравнения старой и новой записей unified_houses
Проверяет, что все критичные поля перенесены при миграции
"""

import os
import json
from pathlib import Path
from typing import Dict, Set, Any
from pymongo import MongoClient
from bson import ObjectId
from dotenv import load_dotenv

# Загружаем переменные окружения
PROJECT_ROOT = Path(__file__).resolve().parent
load_dotenv(dotenv_path=PROJECT_ROOT / ".env")

OLD_RECORD_ID = "68f076bbb02e8b1ca002f1fb"
NEW_RECORD_ID = "6923e8527526e3b8a616bb18"

# Поля, которые должны быть заменены данными из CIAN (не сравниваем)
FIELDS_TO_IGNORE = {
    "_id",
    "updated_at",
    "development.name",
    "development.photos",
    "apartment_types",
}

# Поля, которые должны быть одинаковыми (критичные)
CRITICAL_FIELDS = {
    "latitude",
    "longitude",
    "address_full",
    "address_city",
    "address_district",
    "address_street",
    "address_house",
    "rating",
    "rating_description",
    "rating_created_at",
    "rating_updated_at",
    "is_featured",
    "agent_id",
    "source",
    "created_by",
    "_source_ids",
}


def get_mongo_connection():
    """Получить подключение к MongoDB"""
    MONGO_URI = os.getenv("MONGO_URI", "mongodb://root:Kfleirb_17@176.98.177.188:27017/admin")
    DB_NAME = os.getenv("DB_NAME", "houses")
    client = MongoClient(MONGO_URI)
    db = client[DB_NAME]
    return db


def get_all_keys(obj: Any, prefix: str = "") -> Set[str]:
    """Рекурсивно получает все ключи из объекта с путями"""
    keys = set()
    
    if isinstance(obj, dict):
        for key, value in obj.items():
            full_key = f"{prefix}.{key}" if prefix else key
            keys.add(full_key)
            
            if isinstance(value, (dict, list)):
                keys.update(get_all_keys(value, full_key))
    elif isinstance(obj, list):
        for i, item in enumerate(obj):
            if isinstance(item, (dict, list)):
                keys.update(get_all_keys(item, f"{prefix}[{i}]"))
    
    return keys


def get_nested_value(obj: Any, path: str) -> Any:
    """Получает значение по пути вида 'development.name' или 'apartment_types.1.apartments[0].title'"""
    parts = path.split(".")
    current = obj
    
    for part in parts:
        if "[" in part:
            # Обработка массивов: apartment_types[0] или apartments[0]
            key, index = part.split("[")
            index = int(index.rstrip("]"))
            if isinstance(current, dict) and key in current:
                current = current[key]
                if isinstance(current, list) and 0 <= index < len(current):
                    current = current[index]
                else:
                    return None
            else:
                return None
        else:
            if isinstance(current, dict) and part in current:
                current = current[part]
            else:
                return None
    
    return current


def compare_values(old_val: Any, new_val: Any, path: str) -> Dict:
    """Сравнивает два значения и возвращает результат сравнения"""
    result = {
        "path": path,
        "old_exists": old_val is not None,
        "new_exists": new_val is not None,
        "old_value": old_val,
        "new_value": new_val,
        "equal": old_val == new_val,
    }
    
    # Для критичных полей проверяем равенство
    if any(path.startswith(cf) for cf in CRITICAL_FIELDS):
        result["is_critical"] = True
        if not result["equal"]:
            result["status"] = "❌ КРИТИЧНО: значения отличаются!"
        else:
            result["status"] = "✅ Критичное поле совпадает"
    else:
        result["is_critical"] = False
        if not result["equal"]:
            result["status"] = "⚠️ Значения отличаются"
        else:
            result["status"] = "✅ Совпадает"
    
    return result


def compare_records(old_record: Dict, new_record: Dict) -> Dict:
    """Сравнивает две записи и возвращает детальный отчет"""
    # Получаем все ключи из обеих записей
    old_keys = get_all_keys(old_record)
    new_keys = get_all_keys(new_record)
    
    # Исключаем поля, которые должны отличаться
    old_keys_filtered = {k for k in old_keys if not any(k.startswith(ignore) for ignore in FIELDS_TO_IGNORE)}
    new_keys_filtered = {k for k in new_keys if not any(k.startswith(ignore) for ignore in FIELDS_TO_IGNORE)}
    
    all_keys = old_keys_filtered | new_keys_filtered
    
    results = []
    missing_in_new = []
    missing_in_old = []
    different_values = []
    critical_issues = []
    
    for key in sorted(all_keys):
        # Пропускаем поля, которые должны быть заменены
        if any(key.startswith(ignore) for ignore in FIELDS_TO_IGNORE):
            continue
        
        old_val = get_nested_value(old_record, key)
        new_val = get_nested_value(new_record, key)
        
        comparison = compare_values(old_val, new_val, key)
        results.append(comparison)
        
        if not comparison["old_exists"] and comparison["new_exists"]:
            missing_in_old.append(key)
        elif comparison["old_exists"] and not comparison["new_exists"]:
            missing_in_new.append(key)
            if comparison["is_critical"]:
                critical_issues.append(key)
        elif not comparison["equal"]:
            different_values.append(key)
            if comparison["is_critical"]:
                critical_issues.append(key)
    
    return {
        "total_fields": len(all_keys),
        "results": results,
        "missing_in_new": missing_in_new,
        "missing_in_old": missing_in_old,
        "different_values": different_values,
        "critical_issues": critical_issues,
        "summary": {
            "total_compared": len(results),
            "equal": len([r for r in results if r["equal"]]),
            "different": len(different_values),
            "missing_in_new": len(missing_in_new),
            "missing_in_old": len(missing_in_old),
            "critical_issues": len(critical_issues),
        }
    }


def format_value(value: Any, max_length: int = 100) -> str:
    """Форматирует значение для вывода"""
    if value is None:
        return "None"
    
    if isinstance(value, (dict, list)):
        value_str = json.dumps(value, ensure_ascii=False, indent=2)
        if len(value_str) > max_length:
            return value_str[:max_length] + "..."
        return value_str
    
    value_str = str(value)
    if len(value_str) > max_length:
        return value_str[:max_length] + "..."
    return value_str


def print_comparison_report(comparison: Dict, old_record: Dict, new_record: Dict):
    """Выводит детальный отчет о сравнении"""
    print("\n" + "="*80)
    print("📊 ОТЧЕТ О СРАВНЕНИИ ЗАПИСЕЙ")
    print("="*80)
    
    print(f"\n📝 Старая запись ID: {old_record.get('_id')}")
    print(f"📝 Новая запись ID: {new_record.get('_id')}")
    
    print(f"\n📈 СТАТИСТИКА:")
    summary = comparison["summary"]
    print(f"  Всего полей для сравнения: {summary['total_compared']}")
    print(f"  ✅ Совпадающих: {summary['equal']}")
    print(f"  ⚠️ Отличающихся: {summary['different']}")
    print(f"  ❌ Отсутствующих в новой записи: {summary['missing_in_new']}")
    print(f"  ➕ Новых полей в новой записи: {summary['missing_in_old']}")
    print(f"  🔴 Критичных проблем: {summary['critical_issues']}")
    
    # Критичные проблемы
    if comparison["critical_issues"]:
        print(f"\n🔴 КРИТИЧНЫЕ ПРОБЛЕМЫ ({len(comparison['critical_issues'])}):")
        for issue in comparison["critical_issues"]:
            old_val = get_nested_value(old_record, issue)
            new_val = get_nested_value(new_record, issue)
            print(f"  ❌ {issue}")
            print(f"     Старое: {format_value(old_val, 80)}")
            print(f"     Новое:  {format_value(new_val, 80)}")
    
    # Отсутствующие в новой записи
    if comparison["missing_in_new"]:
        print(f"\n❌ ОТСУТСТВУЮТ В НОВОЙ ЗАПИСИ ({len(comparison['missing_in_new'])}):")
        for key in comparison["missing_in_new"][:20]:  # Показываем первые 20
            old_val = get_nested_value(old_record, key)
            print(f"  - {key}: {format_value(old_val, 60)}")
        if len(comparison["missing_in_new"]) > 20:
            print(f"  ... и еще {len(comparison['missing_in_new']) - 20} полей")
    
    # Новые поля в новой записи
    if comparison["missing_in_old"]:
        print(f"\n➕ НОВЫЕ ПОЛЯ В НОВОЙ ЗАПИСИ ({len(comparison['missing_in_old'])}):")
        for key in comparison["missing_in_old"][:20]:  # Показываем первые 20
            new_val = get_nested_value(new_record, key)
            print(f"  + {key}: {format_value(new_val, 60)}")
        if len(comparison["missing_in_old"]) > 20:
            print(f"  ... и еще {len(comparison['missing_in_old']) - 20} полей")
    
    # Отличающиеся значения (не критичные)
    non_critical_different = [k for k in comparison["different_values"] 
                              if k not in comparison["critical_issues"]]
    if non_critical_different:
        print(f"\n⚠️ ОТЛИЧАЮЩИЕСЯ ЗНАЧЕНИЯ (не критичные) ({len(non_critical_different)}):")
        for key in non_critical_different[:10]:  # Показываем первые 10
            old_val = get_nested_value(old_record, key)
            new_val = get_nested_value(new_record, key)
            print(f"  ⚠️ {key}")
            print(f"     Старое: {format_value(old_val, 60)}")
            print(f"     Новое:  {format_value(new_val, 60)}")
        if len(non_critical_different) > 10:
            print(f"  ... и еще {len(non_critical_different) - 10} полей")
    
    # Проверка критичных полей
    print(f"\n✅ ПРОВЕРКА КРИТИЧНЫХ ПОЛЕЙ:")
    for field in sorted(CRITICAL_FIELDS):
        old_val = get_nested_value(old_record, field)
        new_val = get_nested_value(new_record, field)
        if old_val == new_val:
            print(f"  ✅ {field}: совпадает")
        else:
            print(f"  ❌ {field}: ОТЛИЧАЕТСЯ!")
            print(f"     Старое: {format_value(old_val, 60)}")
            print(f"     Новое:  {format_value(new_val, 60)}")
    
    # Проверка замененных полей
    print(f"\n🔄 ЗАМЕНЕННЫЕ ПОЛЯ (из CIAN):")
    print(f"  📝 development.name:")
    print(f"     Старое: {old_record.get('development', {}).get('name', 'N/A')}")
    print(f"     Новое:  {new_record.get('development', {}).get('name', 'N/A')}")
    
    old_photos_count = len(old_record.get('development', {}).get('photos', []))
    new_photos_count = len(new_record.get('development', {}).get('photos', []))
    print(f"  📸 development.photos:")
    print(f"     Старое: {old_photos_count} фото")
    print(f"     Новое:  {new_photos_count} фото")
    
    old_apt_count = sum(
        len(apt_type.get('apartments', []))
        for apt_type in old_record.get('apartment_types', {}).values()
    )
    new_apt_count = sum(
        len(apt_type.get('apartments', []))
        for apt_type in new_record.get('apartment_types', {}).values()
    )
    print(f"  🏠 apartment_types:")
    print(f"     Старое: {old_apt_count} квартир")
    print(f"     Новое:  {new_apt_count} квартир")
    
    print("\n" + "="*80)


def main():
    """Основная функция сравнения"""
    print("🔍 Начинаем сравнение записей unified_houses...")
    print(f"📝 Старая запись ID: {OLD_RECORD_ID}")
    print(f"📝 Новая запись ID: {NEW_RECORD_ID}")
    
    # Подключаемся к MongoDB
    db = get_mongo_connection()
    unified_col = db['unified_houses']
    
    # Получаем записи
    try:
        old_record = unified_col.find_one({'_id': ObjectId(OLD_RECORD_ID)})
        if not old_record:
            print(f"❌ Старая запись с ID {OLD_RECORD_ID} не найдена")
            return
        
        new_record = unified_col.find_one({'_id': ObjectId(NEW_RECORD_ID)})
        if not new_record:
            print(f"❌ Новая запись с ID {NEW_RECORD_ID} не найдена")
            return
        
        print(f"✅ Обе записи найдены")
        
    except Exception as e:
        print(f"❌ Ошибка получения записей: {e}")
        return
    
    # Сравниваем записи
    print("\n🔍 Сравниваем записи...")
    comparison = compare_records(old_record, new_record)
    
    # Выводим отчет
    print_comparison_report(comparison, old_record, new_record)
    
    # Итоговый вердикт
    print("\n" + "="*80)
    if comparison["summary"]["critical_issues"] > 0:
        print("❌ ОБНАРУЖЕНЫ КРИТИЧНЫЕ ПРОБЛЕМЫ!")
        print("   Некоторые критичные поля не совпадают или отсутствуют в новой записи")
    elif comparison["summary"]["missing_in_new"] > 0:
        print("⚠️ ВНИМАНИЕ: В новой записи отсутствуют некоторые поля из старой")
        print("   Проверьте, не являются ли они важными")
    else:
        print("✅ ВСЕ КРИТИЧНЫЕ ПОЛЯ ПЕРЕНЕСЕНЫ КОРРЕКТНО!")
    print("="*80)


if __name__ == "__main__":
    main()

