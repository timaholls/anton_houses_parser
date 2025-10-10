#!/usr/bin/env python3
"""
Скрипт для создания объединенной коллекции из данных DomRF, Avito и DomClick.
Логика:
1. Берем запись из DomRF
2. Ищем по названию в Avito
3. Ищем фотографии в DomClick по названию
4. Создаем объединенную запись и сохраняем в новую коллекцию
"""
import json
import os
import re
from pathlib import Path
from pymongo import MongoClient
from dotenv import load_dotenv
from typing import Dict, List, Any, Optional
from rapidfuzz import fuzz

# Корневая директория проекта
PROJECT_ROOT = Path(__file__).resolve().parent

# Название новой объединенной коллекции
UNIFIED_COLLECTION_NAME = "unified_houses"


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


def normalize_name_simple(name: str) -> str:
    """Простая нормализация названия для rapidfuzz"""
    if not name:
        return ""

    # Приводим к нижнему регистру
    name = name.lower().strip()

    # Убираем содержимое в скобках (транслитерации, пояснения)
    name = re.sub(r'\([^)]*\)', '', name)

    # Убираем кавычки и лишние символы (включая точки, тире, подчеркивания)
    name = re.sub(r'[«»""\[\].—–\-_&]', ' ', name)

    # Убираем префиксы
    prefixes = ['жк', 'ток', 'комплекс жилых апартаментов', 'комплекс апартаментов',
                'комплекс высотных домов', 'жилой комплекс', 'клубный дом',
                'клубная резиденция', 'микрорайон', 'семейный квартал',
                'знаковый квартал', 'красочный квартал', 'квартал']

    for prefix in prefixes:
        pattern = r'\b' + re.escape(prefix) + r'\b\s*'
        name = re.sub(pattern, '', name, flags=re.IGNORECASE)

    # Убираем множественные пробелы
    name = re.sub(r'\s+', ' ', name).strip()

    return name


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
                    'клубный', 'микрорайон', 'красочный',
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
        # Пропускаем слова, которые являются номерами литеров/секций/этапов
        if (word.isdigit() or  # Одиночные цифры
                (len(word) <= 3 and word.isalpha() and word not in significant_words) or  # Короткие буквы
                word in ['литер', 'литера', 'секции', 'секция', 'этап', 'очередь', 'паркинг']):  # Служебные слова
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


def find_matching_avito_record(domrf_record: Dict, avito_collection, used_avito_ids: set) -> Optional[Dict]:
    """Ищет запись в Avito по названию ЖК, исключая уже использованные записи"""
    domrf_name = domrf_record.get('objCommercNm')
    if not domrf_name:
        return None

    # Используем уже вычисленное нормализованное название из DomRF записи
    normalized_domrf = domrf_record.get('normalized_name')
    if not normalized_domrf:
        # Если поле normalized_name отсутствует, вычисляем его
        normalized_domrf = normalize_name(domrf_name)

    print(f"🔍 DomRF: '{domrf_name}' → нормализовано: '{normalized_domrf}'")

    # Получаем все записи из Avito, исключая уже использованные
    avito_records = list(avito_collection.find({'_id': {'$nin': list(used_avito_ids)}}))

    best_match = None
    best_score = 0
    comparison_count = 0

    for record in avito_records:
        # Извлекаем название из development.name
        development = record.get('development', {})
        avito_name = development.get('name', '')

        if not avito_name:
            continue

        # Нормализуем название из Avito
        normalized_avito = normalize_name(avito_name)

        # Вычисляем схожесть используя rapidfuzz
        similarity_score = calculate_similarity_rapidfuzz(domrf_name, avito_name)
        comparison_count += 1

        # Показываем только если схожесть > 0.3 (чтобы видеть потенциальные совпадения)
        if similarity_score > 0.3:
            print(f"  📋 Avito: '{avito_name}' → нормализовано: '{normalized_avito}' | Схожесть: {similarity_score:.2f}")

        if similarity_score > best_score and similarity_score > 0.60:  # Повышенный минимальный порог для точности
            print(f"    ✅ НОВОЕ ЛУЧШЕЕ СОВПАДЕНИЕ! Схожесть: {similarity_score:.2f}")
            best_score = similarity_score
            best_match = record
        elif similarity_score > 0.5:  # Показываем близкие совпадения
            print(f"    ⚠️  Близкое совпадение, но недостаточно (нужно >0.60)")

    print(f"📊 Сравнено с {comparison_count} записями из Avito (доступно: {len(avito_records)})")
    if best_match:
        development = best_match.get('development', {})
        avito_name = development.get('name', '')
        print(f"🏆 ЛУЧШЕЕ СОВПАДЕНИЕ: '{avito_name}' (схожесть: {best_score:.2f})")
    else:
        print(f"❌ Совпадений не найдено (порог: 0.60)")

    print()
    return best_match


def find_matching_domclick_record(domrf_record: Dict, domclick_collection, used_domclick_ids: set) -> Optional[Dict]:
    """Ищет запись в DomClick по названию ЖК, исключая уже использованные записи"""
    domrf_name = domrf_record.get('objCommercNm')
    if not domrf_name:
        return None

    # Используем уже вычисленное нормализованное название из DomRF записи
    normalized_domrf = domrf_record.get('normalized_name')
    if not normalized_domrf:
        # Если поле normalized_name отсутствует, вычисляем его
        normalized_domrf = normalize_name(domrf_name)

    print(f"🔍 DomRF: '{domrf_name}' → нормализовано: '{normalized_domrf}'")

    # Получаем все записи из DomClick, исключая уже использованные
    domclick_records = list(domclick_collection.find({'_id': {'$nin': list(used_domclick_ids)}}))

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

        # Вычисляем схожесть используя rapidfuzz
        similarity_score = calculate_similarity_rapidfuzz(domrf_name, domclick_name)
        comparison_count += 1

        # Показываем только если схожесть > 0.3 (чтобы видеть потенциальные совпадения)
        if similarity_score > 0.3:
            print(
                f"  📋 DomClick: '{domclick_name}' → нормализовано: '{normalized_domclick}' | Схожесть: {similarity_score:.2f}")

        if similarity_score > best_score and similarity_score > 0.60:  # Повышенный минимальный порог для точности
            print(f"    ✅ НОВОЕ ЛУЧШЕЕ СОВПАДЕНИЕ! Схожесть: {similarity_score:.2f}")
            best_score = similarity_score
            best_match = record
        elif similarity_score > 0.5:  # Показываем близкие совпадения
            print(f"    ⚠️  Близкое совпадение, но недостаточно (нужно >0.60)")

    print(f"📊 Сравнено с {comparison_count} записями из DomClick (доступно: {len(domclick_records)})")
    if best_match:
        development = best_match.get('development', {})
        domclick_name = development.get('complex_name', '')
        print(f"🏆 ЛУЧШЕЕ СОВПАДЕНИЕ: '{domclick_name}' (схожесть: {best_score:.2f})")
    else:
        print(f"❌ Совпадений не найдено (порог: 0.60)")

    print()
    return best_match


def split_compound_word(word: str, other_words: set) -> set:
    """Пытается разбить слитое слово на части, основываясь на других словах"""
    if len(word) < 6:  # Слишком короткое для разбивки
        return {word}

    # Ищем возможные разделения
    parts = set()
    parts.add(word)  # Добавляем исходное слово

    # Пробуем найти части слова в других словах
    for other_word in other_words:
        if other_word in word:
            # Найдено вхождение - добавляем части
            remaining = word.replace(other_word, '')
            if remaining:
                parts.add(other_word)
                parts.add(remaining)
                # Рекурсивно разбиваем остаток
                remaining_parts = split_compound_word(remaining, other_words - {other_word})
                parts.update(remaining_parts)

    # Также пробуем разбить по длине (эвристика)
    if len(word) > 8:
        # Пробуем разделить пополам
        mid = len(word) // 2
        part1 = word[:mid]
        part2 = word[mid:]
        if len(part1) >= 3 and len(part2) >= 3:
            parts.add(part1)
            parts.add(part2)

    return parts


def calculate_similarity_rapidfuzz(name1: str, name2: str) -> float:
    """Вычисляет схожесть между двумя названиями ЖК используя rapidfuzz с поддержкой транслитерации"""
    if not name1 or not name2:
        return 0.0

    # Нормализуем названия
    norm1 = normalize_name_simple(name1)
    norm2 = normalize_name_simple(name2)

    # Вариант 1: Прямое сравнение (оба на кириллице или оба на латинице)
    ratio_direct = fuzz.token_sort_ratio(norm1, norm2, processor=str.lower)

    # Вариант 2: Транслитерируем оба в латиницу и сравниваем
    trans1 = transliterate_russian_to_latin(norm1)
    trans2 = transliterate_russian_to_latin(norm2)
    ratio_transliterated = fuzz.token_sort_ratio(trans1, trans2, processor=str.lower)

    # Вариант 3: Сравниваем norm1 с trans2 (для случаев когда один уже на латинице)
    ratio_cross1 = fuzz.token_sort_ratio(norm1, trans2, processor=str.lower)

    # Вариант 4: Сравниваем trans1 с norm2 (для случаев когда второй уже на латинице)
    ratio_cross2 = fuzz.token_sort_ratio(trans1, norm2, processor=str.lower)

    # Берем максимальный результат из всех вариантов
    max_ratio = max(ratio_direct, ratio_transliterated, ratio_cross1, ratio_cross2)

    # Конвертируем в 0-1 диапазон
    return max_ratio / 100.0


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
    # Группируем по парам (русское, английское)
    significant_pairs = [
        ('village', 'виллидж'), ('park', 'парк'), ('city', 'сити'),
        ('town', 'таун'), ('garden', 'гарден'), ('house', 'хаус'),
        ('collection', 'коллекшн'), ('premiere', 'премьер'),
        ('smart', 'умный'), ('prime', 'прайм')
    ]

    # Создаем множества для быстрого поиска
    significant_words = set()
    for eng, rus in significant_pairs:
        significant_words.add(eng)
        significant_words.add(rus)

    # Добавляем слова без пар
    significant_words.update({'квартал', 'дом', 'the'})

    # Проверяем наличие значимых слов с учетом пар
    def get_significant_concepts(words):
        """Возвращает концепции значимых слов (учитывая пары)"""
        concepts = set()
        for word in words:
            if word in significant_words:
                # Ищем пару для этого слова
                found_pair = False
                for eng, rus in significant_pairs:
                    if word in (eng, rus):
                        concepts.add((eng, rus))
                        found_pair = True
                        break
                if not found_pair:
                    concepts.add((word,))  # Слово без пары
            else:
                # Проверяем, содержится ли значимое слово внутри этого слова
                for eng, rus in significant_pairs:
                    if eng in word or rus in word:
                        concepts.add((eng, rus))
        return concepts

    concepts_1 = get_significant_concepts(words1)
    concepts_2 = get_significant_concepts(words2)

    # Если концепции не совпадают - это разные ЖК
    if concepts_1 != concepts_2 and (concepts_1 or concepts_2):
        # Даже если основная часть похожа, это разные комплексы
        return 0.6  # Не достигнет порога 0.8

    # Если концепции совпадают, даем бонус
    concept_bonus = 0.0
    if concepts_1 == concepts_2 and concepts_1:
        concept_bonus = 0.3  # Бонус за совпадающие концепции

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

    # Проверяем пересечения с учетом транслитерации и разбивки слитых слов
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

    # Пытаемся разбить длинные слитые слова
    for word in list(all_words1):
        if len(word) > 8:  # Длинные слова могут быть слитыми
            parts = split_compound_word(word, all_words2)
            all_words1.update(parts)

    for word in list(all_words2):
        if len(word) > 8:  # Длинные слова могут быть слитыми
            parts = split_compound_word(word, all_words1)
            all_words2.update(parts)

    # Проверяем пересечения с учетом транслитерации
    common_words = all_words1.intersection(all_words2)

    if common_words:
        # Если есть общие слова (включая транслитерации), проверяем процент совпадения
        similarity1 = len(common_words) / len(filtered_words1)
        similarity2 = len(common_words) / len(filtered_words2)

        # Берем среднее значение для более мягкого подхода
        avg_similarity = (similarity1 + similarity2) / 2

        # Применяем бонус за совпадающие концепции
        avg_similarity += concept_bonus

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
            return min(0.95, 0.9 + concept_bonus)
        return min(0.95, 0.8 + concept_bonus)

    if filtered_words2.issubset(all_words1):
        extra_words = all_words1 - filtered_words2
        if len(extra_words) <= 4:  # Увеличиваем лимит для транслитераций
            return min(0.95, 0.9 + concept_bonus)
        return min(0.95, 0.8 + concept_bonus)

    # Финальная проверка: есть ли хотя бы одно слово, которое совпадает по транслитерации
    for word1 in filtered_words1:
        for word2 in filtered_words2:
            # Проверяем прямые совпадения
            if word1 == word2:
                return min(0.95, 0.85 + concept_bonus)  # Хорошее совпадение хотя бы одного слова

            # Проверяем, содержится ли одно слово в другом (для случаев как "zorgepremer" vs "zorge")
            if len(word1) > 6 and len(word2) > 3:  # word1 длинное, word2 короткое
                if word2 in word1:
                    remaining = word1.replace(word2, '')
                    if len(remaining) <= 4:  # Осталось мало символов
                        return min(0.95, 0.9 + concept_bonus)
            elif len(word2) > 6 and len(word1) > 3:  # word2 длинное, word1 короткое
                if word1 in word2:
                    remaining = word2.replace(word1, '')
                    if len(remaining) <= 4:  # Осталось мало символов
                        return min(0.95, 0.9 + concept_bonus)

            # Проверяем транслитерацию
            if len(word1) > 3 and len(word2) > 3:  # Только для достаточно длинных слов
                translit1 = transliterate_russian_to_latin(word1)
                translit2 = transliterate_russian_to_latin(word2)

                if translit1 == word2 or word1 == translit2 or translit1 == translit2:
                    return min(0.95, 0.9 + concept_bonus)  # Отличное совпадение по транслитерации

                # Проверяем вложенность после транслитерации
                if len(translit1) > 6 and len(word2) > 3:
                    if word2 in translit1:
                        remaining = translit1.replace(word2, '')
                        if len(remaining) <= 4:
                            return min(0.95, 0.9 + concept_bonus)
                elif len(translit2) > 6 and len(word1) > 3:
                    if word1 in translit2:
                        remaining = translit2.replace(word1, '')
                        if len(remaining) <= 4:
                            return min(0.95, 0.9 + concept_bonus)

    return 0.0


def extract_photos_from_domclick(domclick_record: Dict) -> List[str]:
    """Извлекает фотографии из записи DomClick"""
    photos = []

    if not domclick_record:
        return photos

    # Ищем фотографии в разных местах структуры
    development = domclick_record.get('development', {})

    # Фотографии из development
    if 'photos' in development:
        photos.extend(development['photos'])

    # Фотографии из apartment_types
    apartment_types = domclick_record.get('apartment_types', {})
    for apt_type, apt_data in apartment_types.items():
        if isinstance(apt_data, dict) and 'photos' in apt_data:
            photos.extend(apt_data['photos'])

    # Убираем дубликаты
    photos = list(set(photos))

    return photos


def create_unified_record(domrf_record: Dict, avito_record: Optional[Dict],
                          domclick_record: Optional[Dict]) -> Dict:
    """Создает объединенную запись из данных трех источников"""

    # Базовые данные из DomRF (только нужные поля)
    unified_record = {
        '_id': domrf_record.get('_id'),
        'source': 'unified',
        'created_at': domrf_record.get('details_extracted_at'),
        'domrf_data': {
            'latitude': domrf_record.get('latitude'),
            'longitude': domrf_record.get('longitude'),
            'object_details': domrf_record.get('object_details', {}),
            'developer': domrf_record.get('developer', {}),
            'objCommercNm': domrf_record.get('objCommercNm')
        }
    }

    # Данные из Avito (все поля)
    if avito_record:
        unified_record['avito_data'] = avito_record

    # Данные из DomClick (apartment_types и development)
    if domclick_record:
        unified_record['domclick_data'] = {
            'apartment_types': domclick_record.get('apartment_types', {}),
            'development': domclick_record.get('development', {})
        }

    return unified_record


def create_unified_collection():
    """Создает объединенную коллекцию"""
    print("🚀 СОЗДАНИЕ ОБЪЕДИНЕННОЙ КОЛЛЕКЦИИ")
    print("=" * 80)

    # Загружаем конфигурации
    domrf_config = load_env_from_parser('domrf')
    avito_config = load_env_from_parser('avito')
    domclick_config = load_env_from_parser('domclick')

    if not all([domrf_config, avito_config, domclick_config]):
        print("❌ Не удалось загрузить все конфигурации")
        return

    try:
        # Подключаемся к MongoDB (используем конфигурацию DomRF как основную)
        client = MongoClient(domrf_config['MONGO_URI'])
        db = client[domrf_config['DB_NAME']]

        # Получаем коллекции
        domrf_collection = db[domrf_config['COLLECTION_NAME']]
        avito_collection = db[avito_config['COLLECTION_NAME']]
        domclick_collection = db[domclick_config['COLLECTION_NAME']]

        # Создаем или очищаем объединенную коллекцию
        unified_collection = db[UNIFIED_COLLECTION_NAME]
        unified_collection.drop()  # Очищаем существующую коллекцию

        print(f"📊 Подключились к базе: {domrf_config['DB_NAME']}")
        print(f"📊 Коллекция DomRF: {domrf_config['COLLECTION_NAME']}")
        print(f"📊 Коллекция Avito: {avito_config['COLLECTION_NAME']}")
        print(f"📊 Коллекция DomClick: {domclick_config['COLLECTION_NAME']}")
        print(f"📊 Новая коллекция: {UNIFIED_COLLECTION_NAME}")

        # Получаем все записи из DomRF
        print(f"\n📥 Загружаем записи из DomRF...")
        domrf_records = list(domrf_collection.find())
        print(f"✅ Загружено {len(domrf_records)} записей из DomRF")

        # Наборы для отслеживания использованных записей
        used_avito_ids = set()
        used_domclick_ids = set()
        unmatched_domrf = []

        # Обрабатываем каждую запись
        processed_count = 0
        matched_avito = 0
        matched_domclick = 0
        skipped_no_avito = 0
        skipped_no_domclick = 0

        for i, domrf_record in enumerate(domrf_records):
            if i % 10 == 0:
                print(f"  Обработано: {i}/{len(domrf_records)}")

            # Получаем название ЖК из DomRF
            domrf_name = domrf_record.get('objCommercNm')
            if not domrf_name:
                continue

            # Ищем совпадения в Avito (исключая уже использованные)
            avito_match = find_matching_avito_record(domrf_record, avito_collection, used_avito_ids)
            if avito_match:
                matched_avito += 1
                # Добавляем в список использованных
                used_avito_ids.add(avito_match['_id'])
            else:
                # Если не нашли в Avito - пропускаем эту запись
                skipped_no_avito += 1
                unmatched_domrf.append({
                    'name': domrf_name,
                    'objId': domrf_record.get('objId'),
                    'reason': 'Не найдено в Avito'
                })
                continue

            # Ищем совпадения в DomClick (только если нашли в Avito, исключая уже использованные)
            domclick_match = find_matching_domclick_record(domrf_record, domclick_collection, used_domclick_ids)
            if domclick_match:
                matched_domclick += 1
                # Добавляем в список использованных
                used_domclick_ids.add(domclick_match['_id'])
            else:
                # Если не нашли в DomClick - тоже пропускаем
                skipped_no_domclick += 1
                unmatched_domrf.append({
                    'name': domrf_name,
                    'objId': domrf_record.get('objId'),
                    'reason': 'Не найдено в DomClick'
                })
                continue

            # Создаем объединенную запись (только если нашли и в Avito, и в DomClick)
            unified_record = create_unified_record(domrf_record, avito_match, domclick_match)

            # Сохраняем в объединенную коллекцию
            unified_collection.insert_one(unified_record)
            processed_count += 1

        # Выводим статистику
        print(f"\n📈 СТАТИСТИКА ОБРАБОТКИ:")
        print(f"  • Всего записей DomRF: {len(domrf_records)}")
        print(f"  • Обработано записей: {processed_count}")
        print(f"  • Пропущено (нет в Avito): {skipped_no_avito}")
        print(f"  • Пропущено (нет в DomClick): {skipped_no_domclick}")
        print(f"  • Найдено совпадений в Avito: {matched_avito}")
        print(f"  • Найдено совпадений в DomClick: {matched_domclick}")
        print(f"  • Записей в объединенной коллекции: {unified_collection.count_documents({})}")

        # Создаем индексы для быстрого поиска
        print(f"\n🔍 Создаем индексы...")
        unified_collection.create_index("domrf_data.objCommercNm")
        unified_collection.create_index("domrf_data.latitude")
        unified_collection.create_index("domrf_data.longitude")

        print(f"✅ Индексы созданы")

        # Получаем список несопоставленных записей из Avito и DomClick
        print(f"\n📊 АНАЛИЗ НЕСОПОСТАВЛЕННЫХ ЗАПИСЕЙ...")

        # Несопоставленные записи из Avito
        unmatched_avito = list(avito_collection.find({'_id': {'$nin': list(used_avito_ids)}}))

        # Несопоставленные записи из DomClick
        unmatched_domclick = list(domclick_collection.find({'_id': {'$nin': list(used_domclick_ids)}}))

        # Выводим таблицу несопоставленных записей DomRF
        if unmatched_domrf:
            print(f"\n{'=' * 120}")
            print(f"📋 НЕСОПОСТАВЛЕННЫЕ ЗАПИСИ ИЗ DOMRF ({len(unmatched_domrf)} шт.)")
            print(f"{'=' * 120}")
            print(f"{'№':<5} {'Название ЖК':<50} {'objId':<15} {'Причина':<40}")
            print(f"{'-' * 120}")
            for idx, record in enumerate(unmatched_domrf[:50], 1):  # Показываем первые 50
                name = record['name'][:48] if len(record['name']) > 48 else record['name']
                obj_id = str(record['objId'])[:13] if record['objId'] else 'N/A'
                reason = record['reason'][:38] if len(record['reason']) > 38 else record['reason']
                print(f"{idx:<5} {name:<50} {obj_id:<15} {reason:<40}")
            if len(unmatched_domrf) > 50:
                print(f"... и еще {len(unmatched_domrf) - 50} записей")
        else:
            print(f"\n✅ Все записи из DomRF сопоставлены!")

        # Выводим таблицу несопоставленных записей Avito
        if unmatched_avito:
            print(f"\n{'=' * 120}")
            print(f"📋 НЕСОПОСТАВЛЕННЫЕ ЗАПИСИ ИЗ AVITO ({len(unmatched_avito)} шт.)")
            print(f"{'=' * 120}")
            print(f"{'№':<5} {'Название ЖК':<80} {'ID':<35}")
            print(f"{'-' * 120}")
            for idx, record in enumerate(unmatched_avito[:50], 1):  # Показываем первые 50
                development = record.get('development', {})
                name = development.get('name') or 'N/A'
                name = name[:78] if name and len(name) > 78 else name
                record_id = str(record['_id'])[:33]
                print(f"{idx:<5} {name:<80} {record_id:<35}")
            if len(unmatched_avito) > 50:
                print(f"... и еще {len(unmatched_avito) - 50} записей")
        else:
            print(f"\n✅ Все записи из Avito сопоставлены!")

        # Выводим таблицу несопоставленных записей DomClick
        if unmatched_domclick:
            print(f"\n{'=' * 120}")
            print(f"📋 НЕСОПОСТАВЛЕННЫЕ ЗАПИСИ ИЗ DOMCLICK ({len(unmatched_domclick)} шт.)")
            print(f"{'=' * 120}")
            print(f"{'№':<5} {'Название ЖК':<80} {'ID':<35}")
            print(f"{'-' * 120}")
            for idx, record in enumerate(unmatched_domclick[:50], 1):  # Показываем первые 50
                development = record.get('development', {})
                name = development.get('complex_name') or 'N/A'
                name = name[:78] if name and len(name) > 78 else name
                record_id = str(record['_id'])[:33]
                print(f"{idx:<5} {name:<80} {record_id:<35}")
            if len(unmatched_domclick) > 50:
                print(f"... и еще {len(unmatched_domclick) - 50} записей")
        else:
            print(f"\n✅ Все записи из DomClick сопоставлены!")

        client.close()

        print(f"\n{'=' * 80}")
        print(f"✅ ОБЪЕДИНЕННАЯ КОЛЛЕКЦИЯ СОЗДАНА: {UNIFIED_COLLECTION_NAME}")
        print(f"{'=' * 80}")

    except Exception as e:
        print(f"❌ Ошибка: {e}")
        import traceback
        traceback.print_exc()


def main():
    print("🔄 ЛОГИКА ОБРАБОТКИ:")
    print("1. Берем запись из DomRF")
    print("2. Ищем совпадение в Avito")
    print("3. Если НЕ нашли в Avito → пропускаем запись")
    print("4. Если нашли в Avito → ищем в DomClick")
    print("5. Если НЕ нашли в DomClick → тоже пропускаем запись")
    print("6. Создаем объединенную запись (только при полной цепочке)")
    print()

    create_unified_collection()


if __name__ == "__main__":
    main()
