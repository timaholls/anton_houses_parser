#!/usr/bin/env python3
"""
Устойчивый скрипт для извлечения данных о квартирах с Avito
Использует текстовые селекторы для обхода меняющихся классов
"""
import asyncio
import json
import os
import pyppeteer
import random
from datetime import datetime
from typing import List, Dict
import time
from urllib.parse import urlparse, parse_qs
from pathlib import Path

# Директория текущего скрипта
PROJECT_ROOT = Path(__file__).resolve().parent

# Импорт функций работы с MongoDB
from db_manager import get_mongo_client, save_to_mongodb, DB_NAME, COLLECTION_NAME

# Импорт функций работы с браузером
from browser_manager import create_browser, create_browser_page, restart_browser

# Настройки скрипта
TIMEOUT = 30000
PROXY_ERROR_PAUSE = 5

# Файлы для отслеживания прогресса
PROGRESS_FILE = PROJECT_ROOT / "parsing_progress.json"
FAILED_URLS_FILE = PROJECT_ROOT / "failed_urls.json"
MAX_API_RETRIES = 3  # Количество попыток для API запросов

# Глобальное хранилище данных для текущего URL
CURRENT_URL_DATA = {
    'development': {},
    'apartment_types': {},
    'total_apartments': 0,
    'url': '',
    'scraped_at': ''
}


def load_progress():
    """Загружает прогресс парсинга"""
    if os.path.exists(PROGRESS_FILE):
        with open(PROGRESS_FILE, 'r', encoding='utf-8') as f:
            return json.load(f)
    return {'processed_urls': []}


def save_progress(url):
    """Сохраняет прогресс обработки URL"""
    progress = load_progress()
    if url not in progress['processed_urls']:
        progress['processed_urls'].append(url)
    with open(PROGRESS_FILE, 'w', encoding='utf-8') as f:
        json.dump(progress, f, ensure_ascii=False, indent=2)


def load_failed_urls():
    """Загружает список неудачных URL"""
    if os.path.exists(FAILED_URLS_FILE):
        with open(FAILED_URLS_FILE, 'r', encoding='utf-8') as f:
            return json.load(f)
    return []


def save_failed_url(url, error):
    """Сохраняет неудачный URL"""
    failed = load_failed_urls()
    failed.append({
        'url': url,
        'error': str(error),
        'timestamp': datetime.now().isoformat()
    })
    with open(FAILED_URLS_FILE, 'w', encoding='utf-8') as f:
        json.dump(failed, f, ensure_ascii=False, indent=2)
    print(f"📝 URL сохранен в список ошибочных: {FAILED_URLS_FILE}")


def is_proxy_error(error_message: str) -> bool:
    """Проверяет, является ли ошибка связанной с прокси"""
    proxy_errors = [
        "ERR_TUNNEL_CONNECTION_FAILED",
        "ERR_PROXY_CONNECTION_FAILED",
        "ERR_CONNECTION_REFUSED",
        "net::ERR_TUNNEL_CONNECTION_FAILED",
        "net::ERR_PROXY_CONNECTION_FAILED",
        "net::ERR_CONNECTION_REFUSED",
        "timeout",
        "connection failed"
    ]
    error_lower = error_message.lower()
    return any(proxy_err.lower() in error_lower for proxy_err in proxy_errors)


async def check_ip_blocked(page) -> bool:
    """Проверяет, заблокирован ли IP по тексту на странице"""
    try:
        # Получаем текст страницы
        page_content = await page.evaluate('''() => {
            return document.body.textContent || '';
        }''')

        # Проверяем наличие текста о блокировке IP
        blocked_texts = [
            "Доступ ограничен: проблема с IP",
            "доступ ограничен",
            "проблема с ip",
            "blocked",
            "forbidden",
            "access denied"
        ]

        page_content_lower = page_content.lower()
        for blocked_text in blocked_texts:
            if blocked_text.lower() in page_content_lower:
                print(f"🚫 Обнаружена блокировка IP: найдено '{blocked_text}'")
                return True

        return False

    except Exception as e:
        print(f"❌ Ошибка проверки блокировки IP: {e}")
        return False


async def extract_development_info(page, url: str) -> Dict:
    """Извлекает информацию о ЖК из заголовка страницы"""
    try:
        print("📋 Извлекаем информацию о ЖК...")
        
        development_info = await page.evaluate('''
            () => {
                try {
                    const header = document.querySelector('[data-marker="development-view/header"]');
                    if (!header) {
                        return null;
                    }
                    
                    const info = {
                        name: '',
                        price_range: '',
                        completion_date: '',
                        price_per_m2: '',
                        address: ''
                    };
                    
                    // Получить текст контейнера без кнопок/CTA
                    const getContainerPlainText = (container) => {
                        if (!container) return '';
                        const cloned = container.cloneNode(true);
                        const ctaSelectors = [
                            'button',
                            '[role="button"]',
                            'a[href^="tel:"]',
                            '[data-marker*="phone"]',
                            '[data-marker*="call"]',
                            '[class*="button"]',
                            '[class*="Button"]'
                        ];
                        cloned.querySelectorAll(ctaSelectors.join(',')).forEach(el => el.remove());
                        return (cloned.textContent || '')
                            .replace(/Телефон\s+застройщика/gi, '')
                            .replace(/Заказать\s+звонок/gi, '')
                            .replace(/\s{2,}/g, ' ')
                            .trim();
                    };
                    
                    // Название ЖК
                    const nameElement = header.querySelector('h1, [class*="title"], [class*="Title"]');
                    if (nameElement) {
                        info.name = nameElement.textContent.trim();
                    }
                    
                    // Получаем весь текст из заголовка без CTA
                    const allText = getContainerPlainText(header);
                    
                    // Ищем диапазон цен (например: "От 5,51 до 24,46 млн ₽")
                    const priceMatch = allText.match(/От\\s+[\\d,]+\\s+до\\s+[\\d,]+\\s+млн\\s+₽/);
                    if (priceMatch) {
                        info.price_range = priceMatch[0];
                    }
                    
                    // Ищем срок сдачи (например: "Сдача в 3 кв. 2027 — 2 кв. 2039")
                    const completionMatch = allText.match(/Сдача\\s+в\\s+[^∙]+/);
                    if (completionMatch) {
                        info.completion_date = completionMatch[0].trim();
                    }
                    
                    // Ищем цену за м² (например: "От 136,50 до 210,2 тыс. ₽ за м²")
                    const pricePerM2Match = allText.match(/От\\s+[\\d,]+\\s+до\\s+[\\d,]+\\s+тыс\\.\\s+₽\\s+за\\s+м²/);
                    if (pricePerM2Match) {
                        info.price_per_m2 = pricePerM2Match[0];
                    }
                    
                    // Адрес
                    const addressElement = header.querySelector('[class*="address"], [class*="Address"]');
                    if (addressElement) {
                        info.address = getContainerPlainText(addressElement);
                    } else {
                        // Ищем адрес в тексте (например: "ул. Лесотехникума, ЖК «8 NEBO»")
                        const addressMatch = allText.match(/ул\\.\\s+[^∙\\n]+/);
                        if (addressMatch) {
                            info.address = addressMatch[0].trim();
                        }
                    }
                    // Финальная очистка адреса на всякий случай
                    if (info.address) {
                        info.address = info.address
                            .replace(/Телефон\s+застройщика/gi, '')
                            .replace(/Заказать\s+звонок/gi, '')
                            .replace(/\s{2,}/g, ' ')
                            .trim();
                    }
                    
                    return info;
                } catch (error) {
                    console.error('Ошибка извлечения информации о ЖК:', error);
                    return null;
                }
            }
        ''')
        
        if development_info:
            print(f"✅ Информация о ЖК: {development_info.get('name', 'Неизвестно')}")
            development_info['url'] = url
            return development_info
        else:
            print(f"⚠️ Не удалось извлечь информацию о ЖК")
            return {'url': url}
            
    except Exception as e:
        print(f"❌ Ошибка извлечения информации о ЖК: {e}")
        return {'url': url}


async def extract_development_tabs_data(page) -> Dict:
    """Извлекает данные из вкладок 'Параметры' и 'Сдача корпусов'"""
    try:
        print("📋 Извлекаем данные из вкладок...")
        
        tabs_data = await page.evaluate('''
            async () => {
                try {
                    const result = {
                        parameters: {},
                        korpuses: []
                    };
                    
                    // Находим контейнер с вкладками
                    const tabsContainer = document.querySelector('[data-marker="about-development-tabs"]');
                    
                    // Если нет контейнера с вкладками, ищем данные прямо в about-development
                    if (!tabsContainer) {
                        console.log('Нет контейнера с вкладками, ищем данные прямо в about-development');
                        const developmentBlock = document.querySelector('[data-marker="about-development"]');
                        if (developmentBlock) {
                            // Ищем все строки с параметрами прямо в about-development
                            const items = developmentBlock.querySelectorAll('*');
                            const processedTexts = new Set();
                            items.forEach(item => {
                                const text = item.textContent.trim();
                                
                                // Пропускаем пустые тексты, слишком длинные или уже обработанные
                                if (!text || text.length > 100 || processedTexts.has(text)) {
                                    return;
                                }
                                
                                // Парсим строки вида "Название: Значение"
                                const match = text.match(/^([^:]+):\\s*(.+)$/);
                                if (match) {
                                    const key = match[1].trim();
                                    const value = match[2].trim();
                                    
                                    // Пропускаем если ключ слишком короткий или длинный
                                    if (key.length < 2 || key.length > 50) {
                                        return;
                                    }
                                    
                                    result.parameters[key] = value;
                                    processedTexts.add(text);
                                    
                                    // Ищем корпусы в параметрах (если содержит ключевые слова)
                                    if (key.includes('корпус') || key.includes('секция') || key.includes('очередь')) {
                                        // Пытаемся извлечь квартал и год из значения
                                        const dateMatch = value.match(/(\\d+)\\s*кв\\.?\\s*(\\d{4})/);
                                        if (dateMatch) {
                                            result.korpuses.push({
                                                name: key,
                                                quarter: dateMatch[1],
                                                year: dateMatch[2]
                                            });
                                        } else {
                                            result.korpuses.push({
                                                name: key,
                                                info: value
                                            });
                                        }
                                    }
                                }
                            });
                        }
                        return result;
                    }
                    
                    // Проверяем наличие вкладки "Сдача корпусов"
                    const korpusesTab = document.querySelector('[data-marker="about-development-tabs/tab(korpuses)"]');
                    const hasKorpusesTab = !!korpusesTab;
                    
                    // Ищем вкладку "Параметры"
                    const parametersTab = document.querySelector('[data-marker="about-development-tabs/tab(parameters)"]');
                    if (parametersTab) {
                        // Кликаем по вкладке
                        parametersTab.click();
                        // Ждем немного для загрузки данных
                        await new Promise(resolve => setTimeout(resolve, 1000));
                        
                        const developmentBlock = document.querySelector('[data-marker="about-development"]');
                        if (developmentBlock) {
                            // Ищем все строки с параметрами
                            const items = developmentBlock.querySelectorAll('[class*="item"], li, div');
                            items.forEach(item => {
                                const text = item.textContent.trim();
                                // Парсим строки вида "Название: Значение"
                                const match = text.match(/^([^:]+):\\s*(.+)$/);
                                if (match) {
                                    const key = match[1].trim();
                                    const value = match[2].trim();
                                    result.parameters[key] = value;
                                    
                                    // Если нет отдельной вкладки "Сдача корпусов", 
                                    // ищем корпусы в параметрах
                                    if (!hasKorpusesTab && (key.includes('корпус') || key.includes('секция') || key.includes('очередь'))) {
                                        // Пытаемся извлечь квартал и год из значения
                                        const dateMatch = value.match(/(\\d+)\\s*кв\\.?\\s*(\\d{4})/);
                                        if (dateMatch) {
                                            result.korpuses.push({
                                                name: key,
                                                quarter: dateMatch[1],
                                                year: dateMatch[2]
                                            });
                                        } else {
                                            result.korpuses.push({
                                                name: key,
                                                info: value
                                            });
                                        }
                                    }
                                }
                            });
                        }
                    }
                    
                    // Ищем вкладку "Сдача корпусов" только если она есть
                    if (korpusesTab) {
                        // Кликаем по вкладке
                        korpusesTab.click();
                        // Ждем немного для загрузки данных
                        await new Promise(resolve => setTimeout(resolve, 1000));
                        
                        const developmentBlock = document.querySelector('[data-marker="about-development"]');
                        if (developmentBlock) {
                            // Ищем все строки с корпусами
                            const items = developmentBlock.querySelectorAll('[class*="item"], li, div');
                            items.forEach(item => {
                                const text = item.textContent.trim();
                                // Парсим строки вида "Литер 11: 3 кв. 2027"
                                const match = text.match(/^(Литер\\s+\\d+|Корпус\\s+\\d+|[^:]+):\\s*(.+)$/);
                                if (match) {
                                    const korpusName = match[1].trim();
                                    const korpusData = match[2].trim();
                                    
                                    // Пытаемся извлечь квартал и год
                                    const dateMatch = korpusData.match(/(\\d+)\\s*кв\\.?\\s*(\\d{4})/);
                                    if (dateMatch) {
                                        result.korpuses.push({
                                            name: korpusName,
                                            quarter: dateMatch[1],
                                            year: dateMatch[2]
                                        });
                                    } else {
                                        result.korpuses.push({
                                            name: korpusName,
                                            info: korpusData
                                        });
                                    }
                                }
                            });
                        }
                    }
                    
                    return result;
                } catch (error) {
                    console.error('Ошибка извлечения данных вкладок:', error);
                    return { parameters: {}, korpuses: [] };
                }
            }
        ''')
        
        if tabs_data:
            parameters_count = len(tabs_data.get('parameters', {}))
            korpuses_count = len(tabs_data.get('korpuses', []))
            
            if parameters_count > 0:
                print(f"✅ Данные 'Параметры' извлечены: {parameters_count} параметров")
            if korpuses_count > 0:
                # Проверяем есть ли отдельная вкладка "Сдача корпусов"
                has_separate_korpuses_tab = any(key.lower() in ['сдача корпусов', 'корпусы', 'сдача'] for key in tabs_data.get('parameters', {}).keys())
                if has_separate_korpuses_tab:
                    print(f"✅ Данные 'Сдача корпусов' извлечены из отдельной вкладки: {korpuses_count} корпусов")
                else:
                    print(f"✅ Данные 'Сдача корпусов' извлечены из параметров: {korpuses_count} корпусов")
            
            # Проверяем откуда извлекались данные
            if parameters_count > 0 or korpuses_count > 0:
                print(f"📋 Источник данных: {'прямо из about-development' if parameters_count > 0 else 'из вкладок'}")
            
            return tabs_data
        else:
            print(f"⚠️ Не удалось извлечь данные вкладок")
            return {'parameters': {}, 'korpuses': []}
            
    except Exception as e:
        print(f"❌ Ошибка извлечения данных вкладок: {e}")
        return {'parameters': {}, 'korpuses': []}


async def find_apartment_type_buttons(page) -> List[Dict]:
    """Находит кнопки типов квартир через header_marker"""
    try:
        print("🔍 Ищем элементы с header_marker...")

        # Проверяем, что страница загружена
        try:
            page_title = await page.evaluate('() => document.title')
            print(f"📄 Заголовок страницы: {page_title[:50]}...")
        except Exception as e:
            print(f"❌ Ошибка получения заголовка: {e}")
            return []

        # ВАЖНО: Ждем появления header_marker элементов (они загружаются динамически!)
        print("⏳ Ожидаем появления header_marker элементов...")
        try:
            await page.waitForSelector('[data-marker*="header_marker"]', timeout=10000)
            print("✅ Header_marker элементы появились")
            # Дополнительная пауза для полной загрузки
            await asyncio.sleep(2)
        except Exception as wait_error:
            print(f"⚠️ Timeout ожидания header_marker элементов: {wait_error}")
            print("   Попробуем найти элементы без ожидания...")

        # Ищем все room-filter/option элементы для получения ID параметров
        print("🔍 Ищем room-filter/option элементы...")
        room_filter_options = await page.evaluate('''
            () => {
                try {
                    console.log('Ищем room-filter/option элементы...');
                    const options = [];
                    
                    const roomFilterElements = document.querySelectorAll('[data-marker*="room-filter/option"]');
                    console.log('Найдено room-filter/option элементов:', roomFilterElements.length);
                    
                    roomFilterElements.forEach((element, index) => {
                        const marker = element.getAttribute('data-marker');
                        const text = element.textContent ? element.textContent.trim() : '';
                        
                        // Извлекаем ID из маркера (например, "room-filter/option(3266975)" -> "3266975")
                        let optionId = '';
                        if (marker && marker.includes('room-filter/option(')) {
                            const match = marker.match(/room-filter\\/option\\((\\d+)\\)/);
                            if (match) {
                                optionId = match[1];
                            }
                        }
                        
                        if (optionId) {
                            options.push({
                                index: index,
                                marker: marker,
                                optionId: optionId,
                                text: text,
                                tagName: element.tagName,
                                className: element.className || 'no-class',
                                isClickable: true
                            });
                            console.log('Найден room-filter/option:', marker, 'ID:', optionId, 'Текст:', text);
                        }
                    });
                    
                    console.log('Возвращаем room-filter/option:', options.length);
                    return options;
                } catch (error) {
                    console.error('Ошибка в JavaScript поиска room-filter/option:', error);
                    return null;
                }
            }
        ''')
        
        if room_filter_options:
            print(f"✅ Найдено {len(room_filter_options)} room-filter/option элементов:")
        else:
            print("⚠️ Room-filter/option элементы не найдены")
        
        # Ищем все header_marker элементы (БЕЗ возврата DOM элементов!)
        buttons = await page.evaluate('''
            () => {
                try {
                    console.log('Ищем header_marker элементы...');
                const buttons = [];
                
                    // Ищем все элементы с data-marker содержащим "header_marker"
                    const headerElements = document.querySelectorAll('[data-marker*="header_marker"]');
                    console.log('Найдено header_marker элементов:', headerElements.length);
                    
                    headerElements.forEach((element, index) => {
                        const marker = element.getAttribute('data-marker');
                        const text = element.textContent ? element.textContent.trim() : '';
                        
                        // Извлекаем тип квартиры из текста
                        let apartmentType = '';
                        if (text.includes('Студии')) apartmentType = 'Студии';
                        else if (text.includes('1-комнатные')) apartmentType = '1-комнатные';
                        else if (text.includes('2-комнатные')) apartmentType = '2-комнатные';
                        else if (text.includes('3-комнатные')) apartmentType = '3-комнатные';
                        else if (text.includes('4-комнатные')) apartmentType = '4-комнатные';
                        
                        if (apartmentType) {
                            // НЕ включаем DOM элемент в результат!
                        buttons.push({
                            index: index,
                                marker: marker,
                            text: text,
                                apartmentType: apartmentType,
                            tagName: element.tagName,
                                className: element.className || 'no-class',
                                isClickable: true
                                // element: element <- УБРАЛИ ЭТО!
                        });
                            console.log('Найден header_marker:', marker, apartmentType);
                    }
                });
                
                    console.log('Возвращаем buttons:', buttons.length);
                return buttons;
                } catch (error) {
                    console.error('Ошибка в JavaScript:', error);
                    return null;
                }
            }
        ''')
        
        # Проверяем, что результат не None
        if buttons is None:
            print("❌ page.evaluate() вернул None - JavaScript ошибка или проблема с сериализацией")
            print("🔍 Пробуем получить консольные логи...")
            return []

        # Проверяем, что это список
        if not isinstance(buttons, list):
            print(f"❌ Неожиданный тип результата: {type(buttons)}")
            print(f"   Результат: {buttons}")
            return []

        button_list = buttons

        print(f"✅ Найдено {len(button_list)} header_marker элементов:")

        if not button_list:
            print("⚠️ Список кнопок пуст - возможно, страница не полностью загружена или изменилась структура")

        # Возвращаем и header_marker кнопки, и room-filter/option данные
        return {
            'header_markers': button_list,
            'room_filter_options': room_filter_options if room_filter_options else []
        }
        
    except Exception as e:
        print(f"❌ Ошибка поиска кнопок типов квартир: {e}")
        return {'header_markers': [], 'room_filter_options': []}


async def click_apartment_type_button(page, button_info: Dict, apartment_type: str, room_filter_options: List[Dict]) -> str:
    """Кликает по header_marker элементу и перехватывает API запрос (если есть key)"""
    try:
        marker = button_info['marker']
        print(f"   🖱️  Кликаем по элементу с маркером: {marker}")

        # Настраиваем перехват сетевых запросов
        captured_key = None

        def handle_request(request):
            nonlocal captured_key
            url = request.url
            # Проверяем наличие key в URL
            if 'newDevelopmentsCatalog/development/items' in url:
                if 'key=' in url:
                    # Извлекаем key из URL (старый формат)
                    try:
                        parsed_url = urlparse(url)
                        query_params = parse_qs(parsed_url.query)
                        if 'key' in query_params:
                            captured_key = query_params['key'][0]
                    except Exception as e:
                        print(f"❌ Ошибка парсинга URL: {e}")

        # Включаем перехват запросов
        page.on('request', handle_request)

        # Используем page.click() для клика по селектору
        try:
            await page.click(f'[data-marker="{marker}"]')
            print(f"   ✅ Клик по типу квартиры '{apartment_type}' выполнен")

            # Ждем 10 секунд для загрузки данных и перехвата API
            print(f"   ⏳ Ожидание 10 секунд для загрузки данных и перехвата API...")
            await asyncio.sleep(10)

            # Выводим результат перехвата
            if captured_key:
                print(f"✅ Key получен: {captured_key}")
                return captured_key
            else:
                print(f"⚠️ Key не найден - возможно, используется новый формат API без ключа")
                return "NO_KEY"  # Специальное значение, указывающее что ключа нет

        except Exception as click_error:
            print(f"   ❌ Ошибка клика: {click_error}")
            # Пробуем альтернативный способ через JavaScript
            print(f"   🔄 Пробуем альтернативный клик через JavaScript...")
            success = await page.evaluate('''
                (marker) => {
                    const element = document.querySelector(`[data-marker="${marker}"]`);
                if (element) {
                    element.click();
                    return true;
                }
                return false;
            }
            ''', marker)
            
            if success:
                print(f"   ✅ Альтернативный клик успешен")
                await asyncio.sleep(10)
                if captured_key:
                    return captured_key
                else:
                    return "NO_KEY"
            
            return None
        
    except Exception as e:
        print(f"   ❌ Ошибка клика: {e}")
        return None


async def make_api_requests_for_type(page, key: str, apartment_type: str, room_type_id: str, development_info: Dict = None):
    """Делает серию API запросов для получения всех квартир определенного типа"""
    try:
        
        # Делаем первый запрос для получения общего количества с повторными попытками
        first_result = None
        for attempt in range(MAX_API_RETRIES):
            first_result = await fetch_apartment_data(page, key, room_type_id, apartment_type, limit=100, offset=0)
            if first_result['success']:
                break
            else:
                print(f"   ⚠️ Попытка {attempt + 1}/{MAX_API_RETRIES} API запроса неудачна: {first_result.get('error', 'Неизвестная ошибка')}")
                if attempt < MAX_API_RETRIES - 1:
                    print(f"   ⏳ Ожидание 2 секунды перед повтором...")
                    await asyncio.sleep(2)
        
        if not first_result or not first_result['success']:
            print(f"   ❌ Все попытки API запроса исчерпаны")
            raise Exception(f"Failed to fetch data for {apartment_type} after {MAX_API_RETRIES} attempts")
        
        items_count = first_result.get('itemsCount', 0)
        
        if items_count == 0:
            print(f"   ⚠️ Нет квартир")
            return
        
        # Собираем все результаты
        all_apartments = []
        if first_result['data'] and 'items' in first_result['data']:
            all_apartments.extend(first_result['data']['items'])
        
        # Делаем дополнительные запросы если нужно
        offset = 100
        while True:
            result = await fetch_apartment_data(page, key, room_type_id, apartment_type, limit=100, offset=offset)
            
            if not result['success']:
                break
            
            items_count_in_response = result.get('itemsCount', 0)
            
            if items_count_in_response == 0:
                break
            elif items_count_in_response < 100:
                if result['data'] and 'items' in result['data']:
                    all_apartments.extend(result['data']['items'])
                break
            else:
                if result['data'] and 'items' in result['data']:
                    all_apartments.extend(result['data']['items'])
                offset += 100
        
        print(f"   ✅ Собрано: {len(all_apartments)} квартир")
        
        # Возвращаем данные для добавления в общую структуру URL
        if all_apartments:
            return {
                'apartment_type': apartment_type,
                'room_type_id': room_type_id,
                'total_count': len(all_apartments),
                'apartments': all_apartments
            }
        return None
        
    except Exception as e:
        print(f"❌ Ошибка выполнения API запросов для {apartment_type}: {e}")


async def save_url_data_to_mongodb():
    """Сохраняет данные текущего URL в MongoDB"""
    try:
        # Проверяем, есть ли данные для сохранения
        if not CURRENT_URL_DATA.get('apartment_types') and CURRENT_URL_DATA.get('total_apartments', 0) == 0:
            print("⚠️ Нет новых данных для сохранения")
            return
        
        # Добавляем timestamp
        CURRENT_URL_DATA["scraped_at"] = datetime.now().isoformat()
        
        # Сохраняем одну запись для всего URL
        success = save_to_mongodb([CURRENT_URL_DATA])
        if success:
            print(f"💾 Сохранено: {CURRENT_URL_DATA.get('total_apartments', 0)} квартир в MongoDB")
            # Полностью очищаем данные для следующего URL
            CURRENT_URL_DATA.clear()
        else:
            print("❌ Не удалось сохранить данные в MongoDB")
        
    except Exception as e:
        print(f"❌ Ошибка сохранения данных в MongoDB: {e}")


async def fetch_apartment_data(page, key: str, room_type_id: str, apartment_type: str, limit: int = 100, offset: int = 0) -> Dict:
    """Выполняет fetch запрос к API Avito для получения данных о квартирах"""
    try:
        # Получаем текущий URL страницы
        current_url = await page.evaluate('() => window.location.href')
        base_url = current_url.split('?')[0]  # Убираем параметры
        
        # Извлекаем поддомен и путь из URL
        subdomain_slug = ""
        parsed = None
        try:
            from urllib.parse import urlparse as url_parse
            parsed = url_parse(current_url)
            hostname_parts = parsed.hostname.split('.') if parsed.hostname else []
            if len(hostname_parts) > 2 and hostname_parts[-2] == 'avito':
                candidate = hostname_parts[0]
                # Для www поддоменSlug должен быть пустым
                subdomain_slug = "" if candidate == 'www' else candidate
        except Exception as e:
            print(f"⚠️ Не удалось извлечь subdomain: {e}")
        
        # Формируем URL API
        # Если key есть - добавляем его в URL (старый формат)
        # Если key == "NO_KEY" - используем новый формат без ключа
        if key and key != "NO_KEY":
            # Старый формат с key всегда ходил на www
            api_url = f"https://www.avito.ru/web/2/newDevelopmentsCatalog/development/items?key={key}"
        else:
            # Новый формат без key: если поддомен отличный от www — используем его, иначе www
            host_prefix = f"{subdomain_slug}." if subdomain_slug else "www."
            api_url = f"https://{host_prefix}avito.ru/web/2/newDevelopmentsCatalog/development/items"
        
        # Формируем параметры URL для body: нужен ПОЛНЫЙ путь страницы каталога
        page_path = parsed.path if parsed and parsed.path else "/"
        url_params = f"{page_path}?limit={limit}&offset={offset}&roomsTypeIds[]={room_type_id}"
        
        request_body = {
            "url": url_params,
            # subdomainSlug должен быть пустым для www, иначе — имя поддомена
            "subdomainSlug": subdomain_slug or ""
        }
        
        # Выполняем fetch запрос через page.evaluate
        api_result = await page.evaluate('''
            async (apiUrl, requestBody) => {
                try {
                    const response = await fetch(apiUrl, {
                        "headers": {
                            "accept": "application/json, text/plain, */*",
                            "accept-language": "ru,en;q=0.9",
                            "content-type": "application/json",
                            "priority": "u=1, i",
                            "sec-ch-ua": "\\"Not A(Brand\\";v=\\"8\\", \\"Chromium\\";v=\\"132\\", \\"YaBrowser\\";v=\\"25.2\\", \\"Yowser\\";v=\\"2.5\\"",
                            "sec-ch-ua-mobile": "?0",
                            "sec-ch-ua-platform": "\\"Linux\\"",
                            "sec-fetch-dest": "empty",
                            "sec-fetch-mode": "cors",
                            "sec-fetch-site": "same-origin"
                        },
                        "body": JSON.stringify(requestBody),
                        "method": "POST",
                        "mode": "cors",
                        "credentials": "include"
                    });
                    
                    if (!response.ok) {
                        throw new Error(`HTTP ${response.status}: ${response.statusText}`);
                    }
                    
                    const data = await response.json();
                    console.log('API Response:', data);
                    console.log('Items type:', typeof data.items);
                    console.log('First item type:', data.items && data.items[0] ? typeof data.items[0] : 'no items');
                    console.log('First item:', data.items && data.items[0]);
                    return {
                        success: true,
                        data: data,
                        status: response.status,
                        totalItems: data.totalItems || 0,
                        itemsCount: data.items ? data.items.length : 0
                    };
                } catch (error) {
                    console.error('API Request Error:', error);
                    return {
                        success: false,
                        error: error.message,
                        data: null
                    };
                }
            }
        ''', api_url, request_body)
        
        return api_result
            
    except Exception as e:
        print(f"❌ Ошибка выполнения API запроса: {e}")
        return {
            'success': False,
            'error': str(e),
            'data': None
        }




async def restart_browser_with_new_proxy(browser, page):
    """Перезапускает браузер со случайным прокси"""
    try:
        print("🔄 Перезапуск браузера с новым прокси...")
        new_browser, new_page, proxy_url = await restart_browser(browser, headless=False)
        print(f"✅ Браузер перезапущен с прокси: {proxy_url}")
        return new_browser, new_page
        
    except Exception as e:
        print(f"❌ Ошибка перезапуска браузера: {e}")
        return browser, page


async def process_single_url(page, url: str, page_num: int, total_urls: int) -> Dict:
    """Обрабатывает один URL"""
    print(f"\n--- URL {page_num}/{total_urls} ---")
    print(f"🌐 {url}")
    
    result = {
        'url': url,
        'page_num': page_num,
        'timestamp': datetime.now().isoformat(),
        'success': False
    }
    
    max_retries = 5  # Максимальное количество попыток
    retry_count = 0

    while retry_count < max_retries:
        try:
            print(f"📥 Попытка {retry_count + 1}/{max_retries}: Загружаем страницу...")
            # Загружаем страницу
            await page.goto(url, waitUntil='domcontentloaded', timeout=TIMEOUT)
            print(f"✅ Страница успешно загружена")
            await asyncio.sleep(3)
            print(f"⏳ Ожидание 3 секунды для полной загрузки...")

            # Проверяем, не заблокирован ли IP
            print(f"🔍 Проверяем блокировку IP...")
            ip_blocked = await check_ip_blocked(page)
            print(f"📊 Результат проверки IP: {'🚫 Заблокирован' if ip_blocked else '✅ Доступен'}")

            if ip_blocked:
                if retry_count < max_retries - 1:
                    retry_count += 1
                    print(f"🔄 Попытка {retry_count}/{max_retries}: IP заблокирован, перезапускаем браузер с новым прокси...")
                    print(f"⏳ Пауза {PROXY_ERROR_PAUSE} секунд перед повтором...")
                    await asyncio.sleep(PROXY_ERROR_PAUSE)
                    # Возвращаем специальный код для перезапуска браузера
                    result['error'] = f"IP_BLOCKED_RETRY_{retry_count}"
                    return result
                else:
                    result['error'] = "IP заблокирован, исчерпаны попытки"
                    return result

            print(f"🎯 Страница готова к обработке, выходим из цикла загрузки")
            break  # Успешно загружено, выходим из цикла

        except Exception as e:
            error_message = str(e)
            print(f"❌ Ошибка загрузки страницы: {error_message}")

            if is_proxy_error(error_message) and retry_count < max_retries - 1:
                retry_count += 1
                print(f"🔄 Попытка {retry_count}/{max_retries}: Ошибка прокси, перезапускаем браузер с новым прокси...")
                print(f"⏳ Пауза {PROXY_ERROR_PAUSE} секунд перед повтором...")
                await asyncio.sleep(PROXY_ERROR_PAUSE)
                # Возвращаем специальный код для перезапуска браузера
                result['error'] = f"PROXY_ERROR_RETRY_{retry_count}"
                return result
            else:
                # Если не ошибка прокси или исчерпаны попытки
                result['error'] = error_message
                return result

    # Если мы дошли сюда, значит страница успешно загружена
    print("🔍 Начинаем обработку страницы...")

    try:
        # Проверяем, есть ли квартиры в ЖК
        print("🔍 Проверяем наличие квартир в ЖК...")
        has_apartments = await page.evaluate('''
            () => {
                const pageText = document.body.textContent || '';
                const noApartmentsTexts = [
                    'В этом ЖК нет предложений от застройщика',
                    'нет предложений от застройщика',
                    'Попробуйте уточнить параметры поиска'
                ];
                
                for (const text of noApartmentsTexts) {
                    if (pageText.includes(text)) {
                        return false;
                    }
                }
                return true;
            }
        ''')
        
        if not has_apartments:
            print("⚠️ В этом ЖК нет предложений от застройщика")
            print("📋 Собираем только информацию о ЖК (без квартир)")
        else:
            print("✅ Квартиры доступны, продолжаем полный сбор данных")
        
        # Полностью очищаем и инициализируем данные для текущего URL
        CURRENT_URL_DATA.clear()
        CURRENT_URL_DATA.update({
            'url': url,
            'total_apartments': 0,
            'apartment_types': {},
            'development': {},
            'scraped_at': ''
        })
        
        # Извлекаем информацию о ЖК
        development_info = await extract_development_info(page, url)
        
        # Извлекаем данные из вкладок
        tabs_data = await extract_development_tabs_data(page)
        if tabs_data.get('parameters') or tabs_data.get('korpuses'):
            development_info.update(tabs_data)
        
        # Сохраняем информацию о ЖК в глобальные данные
        CURRENT_URL_DATA['development'] = development_info
        
        # Собираем квартиры только если они есть
        if has_apartments:
            # Находим кнопки типов квартир и room-filter/option элементы
            search_result = await find_apartment_type_buttons(page)
            
            apartment_type_buttons = search_result['header_markers']
            room_filter_options = search_result['room_filter_options']

            print(f"📊 Результат поиска:")
            print(f"   - Header markers: {len(apartment_type_buttons)} кнопок")
            print(f"   - Room filter options: {len(room_filter_options)} элементов")

            if not apartment_type_buttons or not room_filter_options:
                # На странице не нашли элементов выбора типов квартир —
                # считаем, что сейчас нет квартир в наличии. Сохраняем только данные о ЖК.
                print("⚠️ Не найдены элементы выбора типов квартир — считаем, что нет квартир в наличии")
                print(f"\n🏁 Завершена обработка URL {page_num} (только информация о ЖК)")
                result['success'] = True
                return result

            # Сортируем кнопки по порядку: Студии, 1-комнатные, 2-комнатные, 3-комнатные, 4-комнатные
            type_order = ['Студии', '1-комнатные', '2-комнатные', '3-комнатные', '4-комнатные']
            sorted_buttons = sorted(apartment_type_buttons,
                                    key=lambda x: type_order.index(x['apartmentType']) if x[
                                                                                              'apartmentType'] in type_order else 999)

            print(f"📋 Типы квартир: {[btn['apartmentType'] for btn in sorted_buttons]}")

            # Кликаем ОДИН раз по первой кнопке для получения key (если он есть)
            api_key = None
            if sorted_buttons:
                first_button = sorted_buttons[0]
                print(f"\n🔑 Проверяем наличие API key...")
                api_key = await click_apartment_type_button(page, first_button, first_button['apartmentType'], room_filter_options)
                
                if not api_key:
                    print(f"❌ Не удалось выполнить клик - требуется перезапуск браузера")
                    result['error'] = "API_KEY_RETRY_1"
                    return result
                elif api_key == "NO_KEY":
                    print(f"✅ Используется новый формат API без ключа")
            
            # Теперь делаем API запросы для ВСЕХ типов квартир с полученным key
            print(f"\n🚀 Начинаем сбор данных для всех типов квартир...")
            processed_count = 0
            for option in room_filter_options:
                # Пропускаем "5+" если он есть
                if option['text'] == '5+':
                    continue
                
                apartment_type = option['text']
                room_type_id = option['optionId']
                
                print(f"\n📦 [{processed_count+1}/{len(room_filter_options)-1}] {apartment_type} (ID: {room_type_id})")
                apartment_data = await make_api_requests_for_type(page, api_key, apartment_type, room_type_id, development_info)
                
                # Добавляем данные в общую структуру URL
                if apartment_data:
                    CURRENT_URL_DATA['apartment_types'][apartment_type] = apartment_data
                    CURRENT_URL_DATA['total_apartments'] += apartment_data['total_count']
                
                processed_count += 1

            print(f"\n🏁 Завершена обработка URL {page_num}")
            print(f"📈 Обработано {processed_count} типов квартир")
        else:
            print(f"\n⚠️ ЖК без квартир - пропускаем сбор данных квартир")
            print(f"\n🏁 Завершена обработка URL {page_num} (только информация о ЖК)")
        
        result['success'] = True
        print(f"✅ Обработка URL {page_num} завершена")
        
    except Exception as e:
        print(f"❌ Ошибка обработки URL: {e}")
        result['error'] = str(e)
    
    return result


async def process_failed_urls(failed_urls_list, browser):
    """Обрабатывает список ошибочных URL"""
    try:
        print(f"🔄 Начинаем повторную обработку {len(failed_urls_list)} ошибочных URL...")
        
        retry_successful = 0
        retry_failed = 0
        
        for i, url in enumerate(failed_urls_list, 1):
            print(f"\n--- Повторная обработка URL {i}/{len(failed_urls_list)} ---")
            print(f"🌐 {url}")
            
            try:
                max_attempts = 3  # Меньше попыток для повторной обработки
                attempt_count = 0
                result = None
                
                # Цикл попыток с перезапуском браузера
                while attempt_count < max_attempts:
                    attempt_count += 1
                    print(f"🔄 Попытка {attempt_count}/{max_attempts}")
                    
                    try:
                        # Перезапускаем браузер со случайным прокси
                        browser, page, proxy_url = await restart_browser(browser, headless=False)
                        print(f"🌐 Используем прокси: {proxy_url}")
                        
                        result = await process_single_url(page, url, i, len(failed_urls_list))
                        
                        if result['success']:
                            retry_successful += 1
                            save_progress(url)
                            print(f"✅ Повторная обработка URL {i} завершена успешно")
                            break
                        else:
                            print(f"❌ Повторная обработка URL {i} неудачна: {result.get('error', 'Unknown error')}")
                            
                            if attempt_count < max_attempts:
                                print(f"🔄 Пробуем с другим прокси...")
                                await asyncio.sleep(PROXY_ERROR_PAUSE)
                            # Продолжаем цикл для новой попытки
                    except Exception as e:
                        print(f"❌ Ошибка при попытке {attempt_count}: {e}")
                        if attempt_count < max_attempts:
                            print(f"🔄 Пробуем с другим прокси...")
                            await asyncio.sleep(PROXY_ERROR_PAUSE)
                        # Продолжаем цикл для новой попытки
                
                if not result or not result['success']:
                    retry_failed += 1
                    save_failed_url(url, result.get('error', 'Unknown error') if result else 'No result')
                
                # Сохраняем данные после каждого URL
                await save_url_data_to_mongodb()
                
            except Exception as e:
                print(f"❌ Критическая ошибка при повторной обработке URL {i}: {e}")
                retry_failed += 1
                save_failed_url(url, str(e))
                continue
        
        print(f"\n📊 Статистика повторной обработки:")
        print(f"✅ Успешно повторно обработано: {retry_successful}")
        print(f"❌ Остались ошибочными: {retry_failed}")
        
        return browser
        
    except Exception as e:
        print(f"❌ Ошибка в функции повторной обработки: {e}")
        return browser


async def main():
    """Основная функция"""
    print("🚀 Avito Apartment Scraper")
    print("=" * 60)
    
    # Проверяем наличие файла со ссылками
    links_file = PROJECT_ROOT / 'catalog_links_all_pages.json'
    if not os.path.exists(links_file):
        print(f"❌ Файл {links_file} не найден!")
        print("Сначала запустите parse_avito_1.py для сбора ссылок")
        return
    
    # Загружаем ссылки
    with open(links_file, 'r', encoding='utf-8') as f:
        urls = json.load(f)
    
    print(f"✅ Загружено {len(urls)} URL")
    
    # Загружаем прогресс
    progress = load_progress()
    processed_urls = set(progress['processed_urls'])
    urls_to_process = [url for url in urls if url not in processed_urls]
    
    print(f"📊 Уже обработано: {len(processed_urls)} URL")
    print(f"📝 Осталось обработать: {len(urls_to_process)} URL")
    
    if not urls_to_process:
        print("✅ Все URL уже обработаны!")
        print(f"📂 Проверьте {FAILED_URLS_FILE} для повторной обработки ошибочных URL")
        return
    
    # Проверяем подключение к MongoDB
    mongo_client = get_mongo_client()
    if not mongo_client:
        print("❌ Не удалось подключиться к MongoDB. Завершение работы.")
        return
    mongo_client.close()
    
    # Запускаем браузер со случайным прокси
    browser, proxy_url = await create_browser(headless=False)
    page = await create_browser_page(browser)
    print(f"✅ Браузер запущен с прокси: {proxy_url}")
    
    # Статистика
    all_results = []
    successful = 0
    failed = 0
    start_time = time.time()
    
    try:
        # Обрабатываем каждый URL
        for i, url in enumerate(urls_to_process, 1):
            try:
                max_attempts = 5
                attempt_count = 0
                used_proxies = set()
                result = None
                
                # Цикл попыток с перезапуском браузера
                while attempt_count < max_attempts:
                    attempt_count += 1
                    print(f"🔄 Попытка {attempt_count}/{max_attempts} для URL {i}")
                    
                    result = await process_single_url(page, url, i, len(urls))

                    # Проверяем, нужен ли перезапуск браузера из-за ошибки прокси, блокировки IP или неполученного key
                    if (result.get('error', '').startswith('PROXY_ERROR_RETRY_') or
                            result.get('error', '').startswith('IP_BLOCKED_RETRY_') or
                            result.get('error', '').startswith('API_KEY_RETRY_')):
                        
                        if attempt_count >= max_attempts:
                            print(f"❌ Исчерпаны все попытки ({max_attempts}) для URL {i}")
                            result['error'] = f"Исчерпаны все попытки после {max_attempts} перезапусков браузера"
                            break
                        
                        if result.get('error', '').startswith('API_KEY_RETRY_'):
                            error_type = "неудачной попытки получить API key"
                        elif result.get('error', '').startswith('IP_BLOCKED_RETRY_'):
                            error_type = "блокировки IP"
                        else:
                            error_type = "ошибки прокси"
                        
                        print(f"🔄 Перезапуск браузера #{attempt_count} из-за {error_type}...")
                        browser, page = await restart_browser_with_new_proxy(browser, page)
                        # Продолжаем цикл для новой попытки
                    else:
                        # Успешно или окончательная ошибка
                        break

                all_results.append(result)
                
                if result['success']:
                    successful += 1
                    # Сохраняем прогресс для успешных URL
                    save_progress(url)
                else:
                    failed += 1
                    # Сохраняем ошибочный URL
                    save_failed_url(url, result.get('error', 'Unknown error'))
                
                # Сохраняем данные после каждого URL
                await save_url_data_to_mongodb()
                
                # Промежуточные результаты теперь сохраняются в MongoDB
                
            except KeyboardInterrupt:
                print(f"\n⚠️ Прерывание пользователем на URL {i}")
                break
            except Exception as e:
                print(f"❌ Критическая ошибка на URL {i}: {e}")
                failed += 1
                continue
    
    finally:
        await browser.close()
    
    # Итоговая статистика
    end_time = time.time()
    duration = end_time - start_time
    
    print("\n" + "=" * 60)
    print("📊 ИТОГОВАЯ СТАТИСТИКА:")
    print(f"✅ Успешно обработано: {successful}")
    print(f"❌ Ошибок: {failed}")
    print(f"⏱️  Время выполнения: {duration:.1f} секунд")
    print(f"💾 Данные сохранены в MongoDB: {DB_NAME}.{COLLECTION_NAME}")
    
    if successful > 0:
        print(f"\n🎉 Извлечение данных завершено!")
        print(f"💾 Все данные сохранены в MongoDB коллекции {COLLECTION_NAME}")
    
    # Обработка ошибочных URL
    if failed > 0:
        print(f"\n⚠️ Обнаружено {failed} ошибочных URL")
        print(f"📂 Список ошибочных URL сохранен в {FAILED_URLS_FILE}")
        print(f"\n🔄 Начинаем повторную обработку ошибочных URL...")
        
        failed_urls_data = load_failed_urls()
        if failed_urls_data:
            failed_urls_list = [item['url'] for item in failed_urls_data]
            print(f"📝 Повторная обработка {len(failed_urls_list)} URL...")
            
            # Очищаем файл ошибочных URL перед повторной обработкой
            with open(FAILED_URLS_FILE, 'w', encoding='utf-8') as f:
                json.dump([], f)
            
            # Запускаем повторную обработку
            await asyncio.sleep(2)  # Небольшая пауза
            print(f"\n🚀 Запускаем повторную обработку ошибочных URL...")
            browser = await process_failed_urls(failed_urls_list, browser)
    else:
        print(f"\n✅ Все URL обработаны успешно!")
    
    # Если все обработано успешно и нет ошибок, удаляем временные файлы
    if failed == 0 and successful > 0:
        print(f"\n🧹 Очистка временных файлов...")
        files_to_delete = [PROGRESS_FILE, FAILED_URLS_FILE, links_file]
        for file_path in files_to_delete:
            try:
                if os.path.exists(file_path):
                    os.remove(file_path)
                    print(f"   ✅ Удален: {file_path}")
            except Exception as e:
                print(f"   ⚠️ Не удалось удалить {file_path}: {e}")
        print(f"✅ Временные файлы очищены")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except RuntimeError as e:
        if "already running" in str(e):
            import nest_asyncio

            nest_asyncio.apply()
            asyncio.get_event_loop().run_until_complete(main())
        else:
            raise e
