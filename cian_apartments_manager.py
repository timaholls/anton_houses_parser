#!/usr/bin/env python3
"""
Веб-интерфейс для управления квартирами объединенной коллекции (unified_houses_3).
Позволяет просматривать квартиры, видеть фото и параметры, удалять проблемные.
"""

import os
import copy
from pathlib import Path
from flask import Flask, render_template_string, request, redirect, url_for
from pymongo import MongoClient
from bson import ObjectId
from dotenv import load_dotenv

# Загружаем переменные окружения
PROJECT_ROOT = Path(__file__).resolve().parent
load_dotenv(dotenv_path=PROJECT_ROOT / ".env")

app = Flask(__name__)
COLLECTION_NAME = "unified_houses_3"
TYPE_SUGGESTIONS = [
    "Студия", "1", "2", "3", "4", "5", "5-комн", "6", "7", "8"
]


def get_mongo_connection():
    """Получить подключение к MongoDB"""
    MONGO_URI = os.getenv("MONGO_URI", "mongodb://root:Kfleirb_17@176.98.177.188:27017/admin")
    DB_NAME = os.getenv("DB_NAME", "houses")
    client = MongoClient(MONGO_URI)
    db = client[DB_NAME]
    return db


def get_buildings_list():
    """Получает список всех ЖК из unified_houses_2"""
    db = get_mongo_connection()
    collection = db[COLLECTION_NAME]
    
    buildings = collection.find(
        {},
        projection={"development.name": 1, "name": 1, "_id": 1}
    ).sort("development.name", 1)
    
    result = []
    for building in buildings:
        dev_name = building.get("development", {}).get("name")
        fallback_name = building.get("name")
        display_name = dev_name or fallback_name or "Без названия"
        result.append({
            "_id": building["_id"],
            "display_name": display_name
        })
    return result


def get_building_name(building: dict) -> str:
    """Возвращает удобочитаемое название ЖК"""
    if not building:
        return "Без названия"
    return (
        building.get("development", {}).get("name")
        or building.get("name")
        or building.get("building_title")
        or "Без названия"
    )


def build_type_options(building: dict) -> list[str]:
    """Формирует список доступных категорий квартир для подсказки."""
    options = set(TYPE_SUGGESTIONS)
    if building:
        for apt_type in (building.get("apartment_types") or {}).keys():
            options.add(apt_type)
    return sorted(options, key=lambda x: (len(x), x))


def normalize_title_for_type(original_title: str | None, target_type: str) -> str:
    """Обновляет title под выбранный тип (заменяет первую часть до запятой)."""
    title = (original_title or "").strip()
    suffix = ""
    if "," in title:
        parts = title.split(",", 1)
        suffix = parts[1].strip()
    prefix = "Студия" if target_type.lower().startswith("ст") else f"{target_type}-комн"
    return f"{prefix}{(', ' + suffix) if suffix else ''}"


def get_building_apartments(building_id: str):
    """Получает все квартиры (flatten) для указанного ЖК"""
    db = get_mongo_connection()
    collection = db[COLLECTION_NAME]
    
    building = collection.find_one({"_id": ObjectId(building_id)})
    if not building:
        return None, []

    apartments = []
    apartment_types = building.get("apartment_types", {})
    for apt_type, data in apartment_types.items():
        for idx, apt in enumerate(data.get("apartments", [])):
            apt_copy = copy.deepcopy(apt)
            apt_copy["_type"] = apt_type
            apt_copy["_type_index"] = idx
            gallery = apt_copy.get("images_apartment") or apt_copy.get("image") or []
            if isinstance(gallery, str):
                gallery = [gallery]
            apt_copy["_gallery"] = gallery
            apartments.append(apt_copy)
    return building, apartments


def delete_apartment_from_building(
    building_id: str,
    apartment_type: str,
    apartment_url: str | None = None,
    apartment_index: int | None = None,
):
    """Удаляет квартиру из указанного типа"""
    db = get_mongo_connection()
    collection = db[COLLECTION_NAME]

    field_path = f"apartment_types.{apartment_type}.apartments"
    query = {"_id": ObjectId(building_id)}

    if apartment_url:
        result = collection.update_one(
            query,
            {"$pull": {field_path: {"url": apartment_url}}}
        )
        if result.modified_count > 0:
            return True

    if apartment_index is not None:
        unset_res = collection.update_one(
            query,
            {"$unset": {f"{field_path}.{apartment_index}": 1}}
        )
        if unset_res.modified_count > 0:
            collection.update_one(
                query,
                {"$pull": {field_path: None}}
            )
            return True

    return False


def move_apartment_between_types(
    building_id: str,
    source_type: str,
    apartment_index: int,
    target_type: str,
    new_title: str | None = None,
) -> bool:
    """Перемещает квартиру в другую категорию, при необходимости изменяет title."""
    db = get_mongo_connection()
    collection = db[COLLECTION_NAME]
    building = collection.find_one({"_id": ObjectId(building_id)})
    if not building:
        return False

    apartment_types = copy.deepcopy(building.get("apartment_types", {}))
    source_list = apartment_types.get(source_type, {}).get("apartments", [])

    if apartment_index < 0 or apartment_index >= len(source_list):
        return False

    apartment = source_list.pop(apartment_index)
    if new_title is not None and new_title.strip():
        apartment["title"] = new_title.strip()

    target_type = target_type.strip() if target_type else source_type
    target_entry = apartment_types.setdefault(target_type, {"apartments": []})
    target_list = target_entry.setdefault("apartments", [])
    target_list.append(apartment)

    updates = {
        f"apartment_types.{source_type}.apartments": source_list,
        f"apartment_types.{target_type}.apartments": target_list,
    }

    collection.update_one({"_id": ObjectId(building_id)}, {"$set": updates})
    return True


HTML_TEMPLATE = """
<!DOCTYPE html>
<html lang="ru">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Управление квартирами unified_houses_3</title>
    <style>
        * {
            margin: 0;
            padding: 0;
            box-sizing: border-box;
        }
        
        body {
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, Oxygen, Ubuntu, Cantarell, sans-serif;
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            min-height: 100vh;
            padding: 20px;
        }
        
        .container {
            max-width: 1200px;
            margin: 0 auto;
            background: white;
            border-radius: 20px;
            box-shadow: 0 20px 60px rgba(0,0,0,0.3);
            overflow: hidden;
        }
        
        .header {
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            color: white;
            padding: 30px;
            text-align: center;
        }
        
        .header h1 {
            font-size: 28px;
            margin-bottom: 10px;
        }
        
        .controls {
            padding: 30px;
            background: #f8f9fa;
            border-bottom: 2px solid #e9ecef;
        }
        
        .building-select {
            width: 100%;
            padding: 15px;
            font-size: 16px;
            border: 2px solid #dee2e6;
            border-radius: 10px;
            background: white;
            cursor: pointer;
            transition: all 0.3s;
        }
        
        .building-select:hover {
            border-color: #667eea;
        }
        
        .building-select:focus {
            outline: none;
            border-color: #667eea;
            box-shadow: 0 0 0 3px rgba(102, 126, 234, 0.1);
        }
        
        .apartment-container {
            padding: 30px;
        }
        
        .apartment-card {
            background: white;
            border: 2px solid #e9ecef;
            border-radius: 15px;
            padding: 25px;
            padding-bottom: 120px;  /* Место для фиксированных кнопок */
            margin-bottom: 20px;
            transition: all 0.3s;
            position: relative;
        }
        
        .apartment-card:hover {
            box-shadow: 0 5px 20px rgba(0,0,0,0.1);
            transform: translateY(-2px);
        }
        
        .apartment-header {
            display: flex;
            justify-content: space-between;
            align-items: start;
            margin-bottom: 20px;
            flex-wrap: wrap;
            gap: 15px;
        }
        
        .apartment-title {
            font-size: 20px;
            font-weight: 600;
            color: #212529;
            flex: 1;
        }

        .apartment-subtitle {
            font-size: 16px;
            color: #6c757d;
            margin-top: 5px;
        }
        
        .apartment-url {
            color: #6c757d;
            font-size: 14px;
            word-break: break-all;
            margin-top: 5px;
        }
        
        .apartment-content {
            display: grid;
            grid-template-columns: 300px 1fr;
            gap: 25px;
            margin-bottom: 20px;
        }
        
        @media (max-width: 768px) {
            .apartment-content {
                grid-template-columns: 1fr;
            }
        }
        
        .apartment-photos {
            display: flex;
            flex-direction: column;
            gap: 10px;
        }
        
        .photo-item {
            width: 100%;
            border-radius: 10px;
            overflow: hidden;
            box-shadow: 0 2px 10px rgba(0,0,0,0.1);
        }
        
        .photo-item img {
            width: 100%;
            height: auto;
            display: block;
        }
        
        .photo-label {
            background: #f8f9fa;
            padding: 12px 16px;
            font-size: 18px;
            font-weight: 600;
            color: #343a40;
            text-align: center;
        }
        
        .apartment-info {
            display: flex;
            flex-direction: column;
            gap: 15px;
        }
        
        .info-section {
            background: #f8f9fa;
            padding: 15px;
            border-radius: 10px;
        }
        
        .info-section h3 {
            font-size: 16px;
            color: #495057;
            margin-bottom: 10px;
            border-bottom: 2px solid #dee2e6;
            padding-bottom: 5px;
        }
        
        .info-row {
            display: grid;
            grid-template-columns: 150px 1fr;
            gap: 10px;
            padding: 8px 0;
            border-bottom: 1px solid #e9ecef;
        }
        
        .info-row:last-child {
            border-bottom: none;
        }
        
        .info-label {
            font-weight: 600;
            color: #6c757d;
            font-size: 14px;
        }
        
        .info-value {
            color: #212529;
            font-size: 14px;
        }

        .edit-panel {
            margin-top: 20px;
            background: #fff3cd;
            border: 2px solid #ffeeba;
            border-radius: 12px;
            padding: 20px;
        }

        .edit-panel h3 {
            margin-bottom: 15px;
            color: #856404;
        }

        .edit-form {
            display: grid;
            grid-template-columns: 1fr 200px;
            gap: 15px;
            align-items: end;
        }

        .quick-actions {
            display: flex;
            gap: 10px;
            flex-wrap: wrap;
            margin-top: 10px;
        }

        .btn-quick {
            background: #6c63ff;
            color: white;
            padding: 8px 16px;
            border-radius: 20px;
            font-size: 14px;
        }

        .btn-quick:hover {
            background: #574bcb;
        }

        .edit-form .field-group {
            display: flex;
            flex-direction: column;
            gap: 8px;
        }

        .edit-form label {
            font-weight: 600;
            color: #6c757d;
        }

        .edit-form input[type="text"] {
            padding: 10px 12px;
            border: 1px solid #ced4da;
            border-radius: 8px;
            font-size: 15px;
        }

        .btn-move {
            background: #17a2b8;
            color: white;
        }

        .btn-move:hover {
            background: #138496;
        }
        
        .factoids-list, .summary-list {
            display: flex;
            flex-direction: column;
            gap: 8px;
        }
        
        .factoid-item, .summary-item {
            display: flex;
            gap: 10px;
            padding: 8px;
            background: white;
            border-radius: 5px;
        }
        
        .factoid-label, .summary-label {
            font-weight: 600;
            color: #495057;
            min-width: 120px;
        }
        
        .factoid-value, .summary-value {
            color: #212529;
        }
        
        .decoration-section {
            margin-top: 10px;
        }
        
        .decoration-description {
            background: white;
            padding: 10px;
            border-radius: 5px;
            margin-bottom: 10px;
            color: #495057;
            font-style: italic;
        }
        
        .actions {
            display: flex;
            gap: 15px;
            justify-content: flex-end;
            position: fixed;
            bottom: 20px;
            right: 20px;
            background: white;
            padding: 15px 20px;
            border-radius: 15px;
            box-shadow: 0 5px 25px rgba(0,0,0,0.2);
            border: 2px solid #e9ecef;
            z-index: 1000;
        }
        
        @media (max-width: 768px) {
            .actions {
                left: 20px;
                right: 20px;
                justify-content: space-between;
            }
        }
        
        .btn {
            padding: 16px 40px;
            font-size: 18px;
            font-weight: 700;
            border: none;
            border-radius: 14px;
            cursor: pointer;
            transition: all 0.3s;
            text-decoration: none;
            display: inline-block;
        }
        
        .btn-delete {
            background: #dc3545;
            color: white;
        }
        
        .btn-delete:hover {
            background: #c82333;
            transform: translateY(-2px);
            box-shadow: 0 5px 15px rgba(220, 53, 69, 0.3);
        }
        
        .btn-next {
            background: #28a745;
            color: white;
        }
        
        .btn-next:hover {
            background: #218838;
            transform: translateY(-2px);
            box-shadow: 0 5px 15px rgba(40, 167, 69, 0.3);
        }
        
        .stats {
            background: #e7f3ff;
            padding: 15px;
            border-radius: 10px;
            margin-bottom: 20px;
            text-align: center;
        }
        
        .stats-text {
            font-size: 16px;
            color: #0056b3;
            font-weight: 600;
        }
        
        .no-apartments {
            text-align: center;
            padding: 60px 20px;
            color: #6c757d;
        }
        
        .no-apartments h2 {
            font-size: 24px;
            margin-bottom: 10px;
        }
        
        .loading {
            text-align: center;
            padding: 40px;
            color: #6c757d;
        }
    </style>
</head>
<body>
    <datalist id="type-suggestions">
        {% for option in type_options %}
        <option value="{{ option }}">
        {% endfor %}
    </datalist>
    <div class="container">
        <div class="header">
            <h1>🏠 Управление квартирами unified_houses_3</h1>
            <p>Просмотр и очистка данных после объединения источников</p>
        </div>
        
        <div class="controls">
            <form method="GET" action="/">
                <label for="building_id" style="display: block; margin-bottom: 10px; font-weight: 600; color: #495057;">
                    Выберите ЖК:
                </label>
                <select name="building_id" id="building_id" class="building-select" onchange="this.form.submit()">
                    <option value="">-- Выберите ЖК --</option>
                    {% for building in buildings %}
                    <option value="{{ building._id }}" {% if current_building_id == building._id|string %}selected{% endif %}>
                        {{ building.display_name }}
                    </option>
                    {% endfor %}
                </select>
            </form>
        </div>
        
        {% if building %}
        <div class="apartment-container">
            <div class="stats">
                <div class="stats-text">
                    🏢 {{ building_name }}
                </div>
                <div class="stats-text">
                    📊 Всего квартир: {{ total_apartments }} | 
                    Текущая: {{ total_apartments and (current_index + 1) or 0 }} / {{ total_apartments }}
                </div>
            </div>
            
            {% if apartment %}
            <div class="apartment-card">
                <div class="apartment-header">
                    <div>
                        <div class="apartment-title">{{ apartment.title or 'Без названия' }}</div>
                            <div class="apartment-subtitle">Тип планировки: {{ apartment._type }}</div>
                        <div class="apartment-url">{{ apartment.url }}</div>
                    </div>
                </div>
                
                <div class="apartment-content">
                    <div class="apartment-photos">
                        {% if apartment._gallery %}
                        {% for photo in apartment._gallery %}
                        <div class="photo-item">
                            <img src="{{ photo }}" alt="Фото планировки" onerror="this.style.display='none'">
                            <div class="photo-label">Планировка {{ loop.index }}</div>
                        </div>
                        {% endfor %}
                        {% endif %}
                        
                        {% if apartment.decoration and apartment.decoration.photos %}
                        {% for photo in apartment.decoration.photos %}
                        <div class="photo-item">
                            <img src="{{ photo }}" alt="Фото отделки" onerror="this.style.display='none'">
                            <div class="photo-label">Отделка {{ loop.index }}</div>
                        </div>
                        {% endfor %}
                        {% endif %}
                        
                        {% if not apartment._gallery and (not apartment.decoration or not apartment.decoration.photos) %}
                        <div class="photo-item">
                            <div class="photo-label" style="padding: 40px; text-align: center; color: #6c757d;">
                                ⚠️ Нет фотографий
                            </div>
                        </div>
                        {% endif %}
                    </div>
                    
                    <div class="apartment-info">
                        <div class="info-section">
                            <h3>💰 Цена</h3>
                            <div class="info-row">
                                <span class="info-label">Цена:</span>
                                <span class="info-value">{{ apartment.price or 'Не указана' }}</span>
                            </div>
                            <div class="info-row">
                                <span class="info-label">Цена за м²:</span>
                                <span class="info-value">{{ apartment.pricePerSquare or apartment.price_per_square or 'Не указана' }}</span>
                            </div>
                        </div>

                        <div class="info-section">
                            <h3>📐 Планировка</h3>
                            <div class="info-row">
                                <span class="info-label">Площадь:</span>
                                <span class="info-value">
                                    {% if apartment.totalArea %}
                                        {{ apartment.totalArea }} м²
                                    {% elif apartment.area %}
                                        {{ apartment.area }} м²
                                    {% else %}
                                        Не указана
                                    {% endif %}
                                </span>
                            </div>
                            <div class="info-row">
                                <span class="info-label">Комнат:</span>
                                <span class="info-value">{{ apartment.rooms or apartment._type or 'Не указано' }}</span>
                            </div>
                            <div class="info-row">
                                <span class="info-label">Этаж:</span>
                                <span class="info-value">
                                    {% if apartment.floorMin and apartment.floorMax and apartment.floorMin != apartment.floorMax %}
                                        {{ apartment.floorMin }} - {{ apartment.floorMax }}
                                    {% elif apartment.floorMin %}
                                        {{ apartment.floorMin }}
                                    {% else %}
                                        Не указан
                                    {% endif %}
                                </span>
                            </div>
                        </div>
                        
                        {% if apartment.summary_info %}
                        <div class="info-section">
                            <h3>ℹ️ Дополнительная информация</h3>
                            <div class="summary-list">
                                {% for item in apartment.summary_info %}
                                <div class="summary-item">
                                    <span class="summary-label">{{ item.label }}:</span>
                                    <span class="summary-value">{{ item.value }}</span>
                                </div>
                                {% endfor %}
                            </div>
                        </div>
                        {% endif %}
                        
                        {% if apartment.decoration and apartment.decoration.description %}
                        <div class="info-section decoration-section">
                            <h3>🎨 Отделка</h3>
                            <div class="decoration-description">
                                {{ apartment.decoration.description }}
                            </div>
                        </div>
                        {% endif %}
                        
                        {% if apartment.description %}
                        <div class="info-section">
                            <h3>📝 Описание</h3>
                            <div style="color: #495057; line-height: 1.6;">
                                {{ apartment.description }}
                            </div>
                        </div>
                        {% endif %}
                    </div>
                </div>

                <div class="edit-panel">
                    <h3>✏️ Исправить планировку / категорию</h3>
                    <form method="POST" action="/move" class="edit-form">
                        <input type="hidden" name="building_id" value="{{ current_building_id }}">
                        <input type="hidden" name="source_type" value="{{ apartment._type }}">
                        <input type="hidden" name="apartment_index" value="{{ apartment._type_index }}">
                        <input type="hidden" name="current_index" value="{{ current_index }}">
                        <input type="hidden" name="original_title" value="{{ apartment.title }}">

                        <div class="field-group">
                            <label for="new_title">Новое название</label>
                            <input type="text" name="new_title" id="new_title" value="{{ apartment.title }}">
                        </div>

                        <div class="field-group">
                            <label for="target_type">Целевая категория</label>
                            <input type="text" name="target_type" id="target_type" list="type-suggestions" value="{{ apartment._type }}">
                        </div>

                        <button type="submit" class="btn btn-move">Перенести</button>
                    </form>

                    <div class="quick-actions">
                        <span style="font-weight:600; color:#856404;">Быстрые действия:</span>
                        <form method="POST" action="/move">
                            <input type="hidden" name="building_id" value="{{ current_building_id }}">
                            <input type="hidden" name="source_type" value="{{ apartment._type }}">
                            <input type="hidden" name="apartment_index" value="{{ apartment._type_index }}">
                            <input type="hidden" name="current_index" value="{{ current_index }}">
                            <input type="hidden" name="original_title" value="{{ apartment.title }}">
                            <button type="submit" name="quick_type" value="Студия" class="btn btn-quick">Студия</button>
                        </form>
                        {% for quick in ['1', '2', '3', '4', '5'] %}
                        <form method="POST" action="/move">
                            <input type="hidden" name="building_id" value="{{ current_building_id }}">
                            <input type="hidden" name="source_type" value="{{ apartment._type }}">
                            <input type="hidden" name="apartment_index" value="{{ apartment._type_index }}">
                            <input type="hidden" name="current_index" value="{{ current_index }}">
                            <input type="hidden" name="original_title" value="{{ apartment.title }}">
                            <button type="submit" name="quick_type" value="{{ quick }}" class="btn btn-quick">{{ quick }}</button>
                        </form>
                        {% endfor %}
                    </div>
                </div>

            </div>
            
            <div class="actions">
                <form method="POST" action="/delete" style="display: inline;">
                    <input type="hidden" name="building_id" value="{{ current_building_id }}">
                    <input type="hidden" name="apartment_url" value="{{ apartment.url }}">
                    <input type="hidden" name="apartment_type" value="{{ apartment._type }}">
                    <input type="hidden" name="apartment_index" value="{{ apartment._type_index }}">
                    <input type="hidden" name="current_index" value="{{ current_index }}">
                    <button type="submit" class="btn btn-delete" 
                            onclick="return confirm('Вы уверены, что хотите удалить эту квартиру?');">
                        🗑️ Удалить
                    </button>
                </form>
                
                <form method="GET" action="/" style="display: inline;">
                    <input type="hidden" name="building_id" value="{{ current_building_id }}">
                    <input type="hidden" name="index" value="{{ next_index }}">
                    <button type="submit" class="btn btn-next">
                        ➡️ Далее
                    </button>
                </form>
            </div>
            {% else %}
            <div class="no-apartments">
                <h2>✅ Все квартиры просмотрены</h2>
                <p>Вы просмотрели все квартиры в этом ЖК</p>
            </div>
            {% endif %}
        </div>
        {% else %}
        <div class="apartment-container">
            <div class="no-apartments">
                <h2>👆 Выберите ЖК из списка выше</h2>
                <p>Выберите ЖК для просмотра квартир</p>
            </div>
        </div>
        {% endif %}
    </div>
    <script>
        // Keyboard navigation
        document.addEventListener('keydown', function(event) {
            // Right arrow key - go to next apartment
            if (event.key === 'ArrowRight') {
                const nextButton = document.querySelector('.btn-next');
                if (nextButton) {
                    event.preventDefault();
                    nextButton.click();
                }
            }
        });
    </script>
</body>
</html>
"""


@app.route('/')
def index():
    """Главная страница с выбором ЖК и просмотром квартир"""
    buildings = get_buildings_list()
    
    building_id = request.args.get('building_id', '')
    index_param = request.args.get('index', '0')
    
    try:
        current_index = int(index_param)
    except ValueError:
        current_index = 0
    
    building = None
    building_name = "ЖК не выбран"
    apartment = None
    total_apartments = 0
    type_options = sorted(TYPE_SUGGESTIONS, key=lambda x: (len(x), x))
    
    if building_id:
        try:
            building, apartments = get_building_apartments(building_id)
            building_name = get_building_name(building)
            type_options = build_type_options(building)
            total_apartments = len(apartments)
            
            if total_apartments > 0 and current_index >= total_apartments:
                current_index = 0
            
            if 0 <= current_index < total_apartments:
                apartment = apartments[current_index]
        except Exception as e:
            print(f"Ошибка загрузки данных: {e}")
    
    return render_template_string(
        HTML_TEMPLATE,
        buildings=buildings,
        building=building,
        apartment=apartment,
        building_name=building_name,
        current_building_id=building_id,
        current_index=current_index,
        total_apartments=total_apartments,
        next_index=(current_index + 1) % total_apartments if total_apartments else 0,
        type_options=type_options
    )


@app.route('/delete', methods=['POST'])
def delete_apartment():
    """Удаляет квартиру из базы"""
    building_id = request.form.get('building_id')
    apartment_url = request.form.get('apartment_url')
    apartment_type = request.form.get('apartment_type')
    apartment_index = request.form.get('apartment_index')
    current_index = request.form.get('current_index', '0')
    
    if not building_id or not apartment_type:
        return redirect(url_for('index'))

    try:
        apt_index_value = int(apartment_index) if apartment_index not in (None, '', 'None') else None
    except ValueError:
        apt_index_value = None
    
    try:
        success = delete_apartment_from_building(
            building_id,
            apartment_type,
            apartment_url,
            apt_index_value
        )
        if success:
            print(f"✅ Квартира удалена: {apartment_url}")
        else:
            print(f"⚠️ Квартира не найдена для удаления: {apartment_url}")
    except Exception as e:
        print(f"❌ Ошибка удаления квартиры: {e}")
    
    # Перенаправляем на следующую квартиру (остаемся на том же индексе, так как список сдвинулся)
    return redirect(url_for('index', building_id=building_id, index=current_index))


@app.route('/move', methods=['POST'])
def move_apartment():
    """Переносит квартиру в другую категорию и/или переименовывает."""
    building_id = request.form.get('building_id')
    source_type = (request.form.get('source_type') or '').strip()
    target_type = (request.form.get('target_type') or '').strip()
    new_title = request.form.get('new_title')
    apartment_index = request.form.get('apartment_index')
    current_index = request.form.get('current_index', '0')

    if not building_id or not source_type:
        return redirect(url_for('index'))

    try:
        apt_index_value = int(apartment_index)
    except (TypeError, ValueError):
        apt_index_value = -1

    if apt_index_value < 0:
        return redirect(url_for('index', building_id=building_id, index=current_index))

    quick_type = request.form.get('quick_type')
    if quick_type:
        target_type = quick_type

    if not target_type:
        target_type = source_type

    if quick_type:
        original_title = request.form.get('original_title')
        new_title = normalize_title_for_type(original_title, quick_type)

    try:
        success = move_apartment_between_types(
            building_id,
            source_type,
            apt_index_value,
            target_type,
            new_title
        )
        if success:
            print(f"🔁 Квартира перенесена в '{target_type}'")
        else:
            print("⚠️ Не удалось перенести квартиру")
    except Exception as e:
        print(f"❌ Ошибка переноса квартиры: {e}")

    return redirect(url_for('index', building_id=building_id, index=current_index))


if __name__ == '__main__':
    print("🌐 Запуск веб-интерфейса управления квартирами...")
    print("📡 Откройте в браузере: http://localhost:5000")
    app.run(host='0.0.0.0', port=5250, debug=True)

