#!/usr/bin/env python3
"""
Улучшенная версия скрипта объединения коллекций (DomRF + Avito + DomClick)
Добавлено:
- Учет уже сопоставленных записей (удаляются из списка для последующих итераций)
- Вывод оставшихся несопоставленных записей в каждой коллекции
- Используется точная модель intfloat/multilingual-e5-large
- Добавлена нормализация названий (нижний регистр, удаление спецсимволов, стоп-слов, множественных пробелов)
"""
import os
import re
from pathlib import Path
from typing import Dict, List, Any, Optional
from pymongo import MongoClient
from dotenv import load_dotenv
from sentence_transformers import SentenceTransformer, util

os.environ['HF_HUB_DISABLE_SYMLINKS_WARNING'] = '1'

PROJECT_ROOT = Path(__file__).resolve().parent
UNIFIED_COLLECTION_NAME = "unified_houses"
_semantic_model = None


# ---------------------- МОДЕЛЬ ---------------------- #
def get_semantic_model():
    global _semantic_model
    if _semantic_model is None:
        print("🤖 Загружаем точную многоязычную модель...")
        _semantic_model = SentenceTransformer('intfloat/multilingual-e5-large')
        print("✅ Модель загружена!")
    return _semantic_model


# ---------------------- НОРМАЛИЗАЦИЯ ---------------------- #
PREFIXES = [
    'жк', 'ток', 'жилой комплекс', 'комплекс жилых апартаментов', 'комплекс апартаментов',
    'комплекс высотных домов', 'клубный дом', 'клубная резиденция', 'микрорайон',
    'семейный квартал', 'знаковый квартал', 'красочный квартал', 'жилой квартал',
    'комплекс', 'апартаменты', 'жилой', 'квартал', 'дом'
]
STOP_WORDS = [
    'литер', 'литера', 'секция', 'секции', 'этап', 'очередь',
    'паркинг', 'корпус', 'строение', 'номер', '№'
]


def normalize_name(name: str) -> str:
    if not name:
        return ''
    
    # Приводим к нижнему регистру
    name = name.lower().strip()
    
    # Убираем кавычки и спецсимволы
    name = re.sub(r'[«»"\'`\(\)\.,;:!?\-]', ' ', name)
    
    # Убираем префиксы
    for prefix in PREFIXES:
        if name.startswith(prefix + ' '):
            name = name[len(prefix):].strip()
    
    # Убираем стоп-слова
    for word in STOP_WORDS:
        pattern = r'\b' + re.escape(word) + r'\b\s*'
        name = re.sub(pattern, '', name)
    
    # Убираем множественные пробелы
    name = re.sub(r'\s+', ' ', name).strip()
    
    return name


# ---------------------- СХОЖЕСТЬ ---------------------- #
def semantic_similarity(a: str, b: str, model) -> float:
    if not a or not b:
        return 0.0
    a, b = normalize_name(a), normalize_name(b)
    if not a or not b:
        return 0.0
    try:
        emb1, emb2 = model.encode([a, b], convert_to_tensor=True)
        score = util.cos_sim(emb1, emb2).item()
        return float(score)
    except Exception as e:
        print(f"⚠️ Ошибка при сравнении: {e}")
        return 0.0


# ---------------------- СОПОСТАВЛЕНИЕ ---------------------- #
def find_best_match(source_name: str, target_records: List[Dict], get_name_func, model, threshold: float = 0.8):
    best_match, best_score = None, 0.0
    for rec in target_records:
        name = get_name_func(rec)
        if not name:
            continue
        score = semantic_similarity(source_name, name, model)
        if score > best_score:
            best_score = score
            best_match = rec
    if best_score >= threshold:
        print(f"✅ Лучшее совпадение: {get_name_func(best_match)} (схожесть {best_score:.2f})")
        return best_match
    print(f"❌ Совпадений не найдено (макс. {best_score:.2f})")
    return None


# ---------------------- ОСНОВНАЯ ЛОГИКА ---------------------- #
def create_unified_record(domrf: Dict, avito: Optional[Dict], domclick: Optional[Dict]) -> Dict:
    return {
        'source': 'unified',
        'domrf': {
            'name': domrf.get('objCommercNm'),
            'latitude': domrf.get('latitude'),
            'longitude': domrf.get('longitude')
        },
        'avito': avito,
        'domclick': domclick
    }


def load_env(parser_name: str) -> Dict[str, str]:
    env_path = PROJECT_ROOT / parser_name / '.env'
    if not env_path.exists():
        return {}
    load_dotenv(env_path, override=True)
    return {
        'MONGO_URI': os.getenv('MONGO_URI'),
        'DB_NAME': os.getenv('DB_NAME'),
        'COLLECTION_NAME': os.getenv('COLLECTION_NAME')
    }


def create_unified_collection():
    print("🚀 СОЗДАНИЕ ОБЪЕДИНЕННОЙ КОЛЛЕКЦИИ (E5-Large + нормализация)")
    print("=" * 80)

    cfg = {p: load_env(p) for p in ['domrf', 'avito', 'domclick']}
    if not all(cfg[p] for p in cfg):
        print("❌ Не удалось загрузить конфигурации .env")
        return

    client = MongoClient(cfg['domrf']['MONGO_URI'])
    db = client[cfg['domrf']['DB_NAME']]

    domrf_col = db[cfg['domrf']['COLLECTION_NAME']]
    avito_col = db[cfg['avito']['COLLECTION_NAME']]
    domclick_col = db[cfg['domclick']['COLLECTION_NAME']]

    unified_col = db[UNIFIED_COLLECTION_NAME]
    unified_col.drop()

    model = get_semantic_model()

    domrf_records = list(domrf_col.find())
    avito_records = list(avito_col.find())
    domclick_records = list(domclick_col.find())

    matched_avito_ids, matched_domclick_ids = set(), set()

    for i, domrf in enumerate(domrf_records, 1):
        name = domrf.get('objCommercNm')
        if not name:
            continue
        print(f"\n🔍 {i}/{len(domrf_records)} DomRF: {name}")

        avito_match = find_best_match(name, [r for r in avito_records if r['_id'] not in matched_avito_ids],
                                      lambda r: r.get('development', {}).get('name', ''), model)
        if not avito_match:
            continue
        matched_avito_ids.add(avito_match['_id'])

        domclick_match = find_best_match(name, [r for r in domclick_records if r['_id'] not in matched_domclick_ids],
                                         lambda r: r.get('development', {}).get('complex_name', ''), model)
        if not domclick_match:
            continue
        matched_domclick_ids.add(domclick_match['_id'])

        unified_col.insert_one(create_unified_record(domrf, avito_match, domclick_match))

    print(f"\n✅ Коллекция создана: {unified_col.count_documents({})} записей")

    unmatched_avito = [r for r in avito_records if r['_id'] not in matched_avito_ids]
    unmatched_domclick = [r for r in domclick_records if r['_id'] not in matched_domclick_ids]

    print("\n📊 Итог несопоставленных записей:")
    print(f"  • Avito: {len(unmatched_avito)}")
    print(f"  • DomClick: {len(unmatched_domclick)}")

    if unmatched_avito:
        print("\n📋 Несопоставленные Avito:")
        for r in unmatched_avito[:20]:
            print(f"   - {r.get('development', {}).get('name', '')}")
    if unmatched_domclick:
        print("\n📋 Несопоставленные DomClick:")
        for r in unmatched_domclick[:20]:
            print(f"   - {r.get('development', {}).get('complex_name', '')}")

    client.close()


if __name__ == "__main__":
    create_unified_collection()
