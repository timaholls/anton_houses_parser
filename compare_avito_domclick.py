#!/usr/bin/env python3
"""
Скрипт для сравнения данных между коллекциями Avito и DomClick без DomRF.
Показывает совпадения в консоль без сохранения в базу данных.
"""
import json
import os
import re
from pathlib import Path
from pymongo import MongoClient
from dotenv import load_dotenv
from typing import Dict, List, Any, Optional

# Корневая директория проекта
PROJECT_ROOT = Path(__file__).resolve().parent


def load_env_from_parser(parser_name):
    """Загружает переменные окружения из папки парсера"""
    env_path = PROJECT_ROOT / parser_name / ".env"
    if env_path.exists():
        # Сохраняем текущие переменные окружения
        original_env = dict(os.environ)
        
        # Очищаем переменные окружения
        for key in ['DB_NAME', 'COLLECTION_NAME', 'MONGO_URI']:
            if key in os.environ:
                del os.environ[key]
        
        # Загружаем переменные из конкретного .env файла
        load_dotenv(dotenv_path=env_path, override=True)
        
        # Получаем значения
        config = {
            "DB_NAME": os.getenv("DB_NAME"),
            "COLLECTION_NAME": os.getenv("COLLECTION_NAME"),
            "MONGO_URI": os.getenv("MONGO_URI")
        }
        
        # Восстанавливаем оригинальные переменные окружения
        os.environ.clear()
        os.environ.update(original_env)
        
        return config
    return None


def transliterate_russian_to_latin(text: str) -> str:
    """Транслитерирует русский текст в латиницу"""
    translit_dict = {
        'а': 'a', 'б': 'b', 'в': 'v', 'г': 'g', 'д': 'd', 'е': 'e', 'ё': 'yo',
        'ж': 'zh', 'з': 'z', 'и': 'i', 'й': 'y', 'к': 'k', 'л': 'l', 'м': 'm',
        'н': 'n', 'о': 'o', 'п': 'p', 'р': 'r', 'с': 's', 'т': 't', 'у': 'u',
        'ф': 'f', 'х': 'h', 'ц': 'ts', 'ч': 'ch', 'ш': 'sh', 'щ': 'sch',
        'ъ': '', 'ы': 'y', 'ь': '', 'э': 'e', 'ю': 'yu', 'я': 'ya',
        'А': 'A', 'Б': 'B', 'В': 'V', 'Г': 'G', 'Д': 'D', 'Е': 'E', 'Ё': 'Yo',
        'Ж': 'Zh', 'З': 'Z', 'И': 'I', 'Й': 'Y', 'К': 'K', 'Л': 'L', 'М': 'M',
        'Н': 'N', 'О': 'O', 'П': 'P', 'Р': 'R', 'С': 'S', 'Т': 'T', 'У': 'U',
        'Ф': 'F', 'Х': 'H', 'Ц': 'Ts', 'Ч': 'Ch', 'Ш': 'Sh', 'Щ': 'Sch',
        'Ъ': '', 'Ы': 'Y', 'Ь': '', 'Э': 'E', 'Ю': 'Yu', 'Я': 'Ya'
    }
    
    result = ""
    for char in text:
        result += translit_dict.get(char, char)
    return result


def normalize_name(name: str) -> str:
    """Нормализует название ЖК для поиска с поддержкой транслитерации"""
    if not name:
        return ""
    
    # Приводим к нижнему регистру
    normalized = name.lower()
    
    # Убираем содержимое в скобках (часто там транслитерации)
    normalized = re.sub(r'\([^)]*\)', '', normalized)
    
    # Убираем лишние символы и пробелы
    normalized = re.sub(r'[^\w\s]', '', normalized)
    normalized = re.sub(r'\s+', ' ', normalized).strip()
    
    # Убираем общие слова (но НЕ убираем значимые слова типа "village", "park")
    common_words = ['жк', 'жилой', 'комплекс', 'дома', 'квартиры', 'поселок', 
                   'литер', 'литера', 'секции', 'этап', 'очередь',
                   'клубный', 'микрорайон', 'семейный', 'красочный',
                   'апартаментов', 'апартаменты', 'высотных', 'экогород',
                   'клубная', 'резиденция', 'группа', 'компаний', 'комплекса']
    
    # Значимые слова, которые НЕ должны удаляться
    significant_words = {'village', 'виллидж', 'park', 'парк', 'city', 'сити', 
                        'town', 'таун', 'garden', 'гарден', 'house', 'хаус',
                        'collection', 'коллекшн', 'квартал', 'premiere', 'премьер',
                        'умный', 'smart', 'дом', 'the', 'prime'}
    
    for word in common_words:
        # Убираем слово только если оно отдельное (не часть другого слова) и не значимое
        if word not in significant_words:
            normalized = re.sub(r'\b' + word + r'\b', '', normalized)
    
    # Убираем отдельно стоящие цифры и короткие буквы (номера литеров, этапов, секций)
    # Но оставляем цифры, которые являются частью названия (8 марта, 535)
    words = normalized.split()
    filtered_words = []
    for i, word in enumerate(words):
        # Если это одиночная цифра или 1-2 буквы после основного названия (но не значимые слова)
        if len(filtered_words) > 0 and (word.isdigit() or (len(word) <= 2 and word.isalpha() and word not in significant_words)):
            # Пропускаем номера литеров/секций после названия
            continue
        filtered_words.append(word)
    
    normalized = ' '.join(filtered_words)
    
    # Убираем лишние пробелы после удаления слов
    normalized = re.sub(r'\s+', ' ', normalized).strip()
    
    # Транслитерируем русский текст в латиницу для лучшего сопоставления
    transliterated = transliterate_russian_to_latin(normalized)
    
    # Возвращаем оба варианта через пробел для сравнения
    if normalized != transliterated:
        return f"{normalized} {transliterated}"
    else:
        return normalized


def calculate_similarity(name1: str, name2: str) -> float:
    """
    Вычисляет схожесть между двумя названиями ЖК с поддержкой транслитерации.
    Ключевая логика:
    - Если основное название совпадает, но есть дополнительные транслитерации - высокая схожесть
    - Если есть значимые различия (village, park и т.д.) - низкая схожесть
    """
    if not name1 or not name2:
        return 0.0
    
    # Точное совпадение
    if name1 == name2:
        return 1.0
    
    # Разбиваем на слова
    words1 = set(name1.split())
    words2 = set(name2.split())
    
    if not words1 or not words2:
        return 0.0
    
    # Список значимых слов, которые ОБЯЗАТЕЛЬНО должны совпадать
    significant_words = {'village', 'виллидж', 'park', 'парк', 'city', 'сити', 
                        'town', 'таун', 'garden', 'гарден', 'house', 'хаус',
                        'collection', 'коллекшн', 'квартал', 'premiere', 'премьер',
                        'умный', 'smart', 'дом', 'the', 'prime'}
    
    # Проверяем наличие значимых слов
    significant_in_1 = words1.intersection(significant_words)
    significant_in_2 = words2.intersection(significant_words)
    
    # Если в одном есть значимое слово, а в другом нет - это разные ЖК
    if significant_in_1 != significant_in_2:
        # Даже если основная часть похожа, это разные комплексы
        return 0.6  # Не достигнет порога 0.8
    
    # Служебные слова (не влияют на совпадение, но и не критичны)
    stop_words = {'новый', 'старый', 'большой', 'маленький'}
    
    # Убираем служебные слова
    filtered_words1 = {word for word in words1 if word not in stop_words}
    filtered_words2 = {word for word in words2 if word not in stop_words}
    
    # Если после фильтрации не осталось слов
    if not filtered_words1 or not filtered_words2:
        return 0.0
    
    # Одно содержит другое полностью
    if filtered_words1 == filtered_words2:
        return 1.0
    
    # Проверяем пересечения с учетом транслитерации
    # Ищем пересечения между оригинальными словами и транслитерациями
    all_words1 = filtered_words1.copy()
    all_words2 = filtered_words2.copy()
    
    # Добавляем транслитерации для каждого слова
    for word in list(filtered_words1):
        if any(ord(c) > 127 for c in word):  # Если есть кириллические символы
            transliterated = transliterate_russian_to_latin(word)
            if transliterated != word:
                all_words1.add(transliterated)
    
    for word in list(filtered_words2):
        if any(ord(c) > 127 for c in word):  # Если есть кириллические символы
            transliterated = transliterate_russian_to_latin(word)
            if transliterated != word:
                all_words2.add(transliterated)
    
    # Проверяем пересечения с учетом транслитерации
    common_words = all_words1.intersection(all_words2)
    
    if common_words:
        # Если есть общие слова (включая транслитерации), проверяем процент совпадения
        similarity1 = len(common_words) / len(filtered_words1)
        similarity2 = len(common_words) / len(filtered_words2)
        
        # Берем среднее значение для более мягкого подхода
        avg_similarity = (similarity1 + similarity2) / 2
        
        # Бонус за совпадение транслитераций
        if avg_similarity >= 0.7:
            return min(0.95, avg_similarity + 0.1)  # Бонус за хорошее совпадение
        elif avg_similarity >= 0.5:
            return avg_similarity * 0.95
        else:
            return avg_similarity * 0.8
    
    # Если нет прямых пересечений, проверяем вложенность с учетом транслитерации
    if filtered_words1.issubset(all_words2):
        extra_words = all_words2 - filtered_words1
        if len(extra_words) <= 4:  # Увеличиваем лимит для транслитераций
            return 0.9
        return 0.8
    
    if filtered_words2.issubset(all_words1):
        extra_words = all_words1 - filtered_words2
        if len(extra_words) <= 4:  # Увеличиваем лимит для транслитераций
            return 0.9
        return 0.8
    
    # Финальная проверка: есть ли хотя бы одно слово, которое совпадает по транслитерации
    for word1 in filtered_words1:
        for word2 in filtered_words2:
            # Проверяем прямые совпадения
            if word1 == word2:
                return 0.85  # Хорошее совпадение хотя бы одного слова
            
            # Проверяем транслитерацию
            if len(word1) > 3 and len(word2) > 3:  # Только для достаточно длинных слов
                translit1 = transliterate_russian_to_latin(word1)
                translit2 = transliterate_russian_to_latin(word2)
                
                if translit1 == word2 or word1 == translit2 or translit1 == translit2:
                    return 0.9  # Отличное совпадение по транслитерации
    
    return 0.0


def find_matching_domclick_record(avito_name: str, domclick_collection) -> Optional[Dict]:
    """Ищет запись в DomClick по названию ЖК из Avito"""
    if not avito_name:
        return None
    
    # Нормализуем название из Avito
    normalized_avito = normalize_name(avito_name)
    print(f"🔍 Avito: '{avito_name}' → нормализовано: '{normalized_avito}'")
    
    # Получаем все записи из DomClick
    domclick_records = list(domclick_collection.find())
    
    best_match = None
    best_score = 0
    comparison_count = 0
    
    for record in domclick_records:
        # Извлекаем название из development.complex_name
        development = record.get('development', {})
        domclick_name = development.get('complex_name', '')
        
        if not domclick_name:
            continue
        
        # Нормализуем название из DomClick
        normalized_domclick = normalize_name(domclick_name)
        
        # Вычисляем схожесть
        similarity_score = calculate_similarity(normalized_avito, normalized_domclick)
        comparison_count += 1
        
        # Показываем только если схожесть > 0.3 (чтобы видеть потенциальные совпадения)
        if similarity_score > 0.3:
            print(f"  📋 DomClick: '{domclick_name}' → нормализовано: '{normalized_domclick}' | Схожесть: {similarity_score:.2f}")
        
        if similarity_score > best_score and similarity_score > 0.8:  # Минимальный порог
            print(f"    ✅ НОВОЕ ЛУЧШЕЕ СОВПАДЕНИЕ! Схожесть: {similarity_score:.2f}")
            best_score = similarity_score
            best_match = record
        elif similarity_score > 0.5:  # Показываем близкие совпадения
            print(f"    ⚠️  Близкое совпадение, но недостаточно (нужно >0.8)")
    
    print(f"📊 Сравнено с {comparison_count} записями из DomClick")
    if best_match:
        development = best_match.get('development', {})
        domclick_name = development.get('complex_name', '')
        print(f"🏆 ЛУЧШЕЕ СОВПАДЕНИЕ: '{domclick_name}' (схожесть: {best_score:.2f})")
    else:
        print(f"❌ Совпадений не найдено (порог: 0.8)")
    
    print()
    return best_match


def compare_avito_domclick():
    """Сравнивает данные между Avito и DomClick"""
    print("🔍 СРАВНЕНИЕ AVITO ↔ DOMCLICK")
    print("="*80)
    
    # Загружаем конфигурации
    avito_config = load_env_from_parser('avito')
    domclick_config = load_env_from_parser('domclick')
    
    if not all([avito_config, domclick_config]):
        print("❌ Не удалось загрузить конфигурации")
        return
    
    try:
        # Подключаемся к MongoDB (используем конфигурацию Avito как основную)
        client = MongoClient(avito_config['MONGO_URI'])
        db = client[avito_config['DB_NAME']]
        
        # Получаем коллекции
        avito_collection = db[avito_config['COLLECTION_NAME']]
        domclick_collection = db[domclick_config['COLLECTION_NAME']]
        
        print(f"📊 База данных: {avito_config['DB_NAME']}")
        print(f"📊 Коллекция Avito: {avito_config['COLLECTION_NAME']}")
        print(f"📊 Коллекция DomClick: {domclick_config['COLLECTION_NAME']}")
        
        # Получаем все записи из Avito
        print(f"\n📥 Загружаем записи из Avito...")
        avito_records = list(avito_collection.find())
        print(f"✅ Загружено {len(avito_records)} записей из Avito")
        
        # Статистика
        total_processed = 0
        total_matched = 0
        total_skipped = 0
        matches_details = []
        
        print(f"\n🔄 ОБРАБОТКА ЗАПИСЕЙ:")
        print("="*80)
        
        for i, avito_record in enumerate(avito_records):
            if i % 5 == 0:
                print(f"\n📋 Обрабатываем запись {i+1}/{len(avito_records)}")
            
            # Извлекаем название из development.name
            development = avito_record.get('development', {})
            avito_name = development.get('name', '')
            
            if not avito_name:
                total_skipped += 1
                continue
            
            # Ищем совпадения в DomClick
            domclick_match = find_matching_domclick_record(avito_name, domclick_collection)
            
            total_processed += 1
            
            if domclick_match:
                total_matched += 1
                domclick_development = domclick_match.get('development', {})
                domclick_name = domclick_development.get('complex_name', '')
                
                match_info = {
                    'avito_name': avito_name,
                    'domclick_name': domclick_name,
                    'avito_id': str(avito_record.get('_id')),
                    'domclick_id': str(domclick_match.get('_id'))
                }
                matches_details.append(match_info)
        
        # Выводим итоговую статистику
        print(f"\n{'='*80}")
        print("📈 ИТОГОВАЯ СТАТИСТИКА:")
        print("="*80)
        print(f"  • Всего записей в Avito: {len(avito_records)}")
        print(f"  • Обработано записей: {total_processed}")
        print(f"  • Пропущено (нет названия): {total_skipped}")
        print(f"  • Найдено совпадений: {total_matched}")
        print(f"  • Процент совпадений: {(total_matched/total_processed*100):.1f}%" if total_processed > 0 else "  • Процент совпадений: 0.0%")
        
        # Показываем детали совпадений
        if matches_details:
            print(f"\n🏆 ДЕТАЛИ СОВПАДЕНИЙ:")
            print("-" * 80)
            for i, match in enumerate(matches_details[:10], 1):  # Показываем первые 10
                print(f"{i:2d}. Avito: '{match['avito_name']}'")
                print(f"    DomClick: '{match['domclick_name']}'")
                print(f"    IDs: Avito({match['avito_id'][:12]}...), DomClick({match['domclick_id'][:12]}...)")
                print()
            
            if len(matches_details) > 10:
                print(f"    ... и ещё {len(matches_details) - 10} совпадений")
        
        # Сохраняем результаты в файл
        results_file = PROJECT_ROOT / "avito_domclick_comparison.json"
        results = {
            'statistics': {
                'total_avito_records': len(avito_records),
                'processed_records': total_processed,
                'skipped_records': total_skipped,
                'matched_records': total_matched,
                'match_percentage': (total_matched/total_processed*100) if total_processed > 0 else 0
            },
            'matches': matches_details
        }
        
        with open(results_file, 'w', encoding='utf-8') as f:
            json.dump(results, f, ensure_ascii=False, indent=2)
        
        print(f"📄 Результаты сохранены в: {results_file}")
        
        client.close()
        
        print(f"\n{'='*80}")
        print("✅ СРАВНЕНИЕ ЗАВЕРШЕНО")
        print(f"{'='*80}")
        
    except Exception as e:
        print(f"❌ Ошибка: {e}")
        import traceback
        traceback.print_exc()


def main():
    compare_avito_domclick()


if __name__ == "__main__":
    main()
