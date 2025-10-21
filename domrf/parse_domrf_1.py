import asyncio
import time
import random
import json
import os
import sys
from pathlib import Path
from browser_manager import setup_stealth_browser, create_new_tab

# Директория текущего скрипта
PROJECT_ROOT = Path(__file__).resolve().parent

# Проверяем, запущены ли мы в Docker - если да, убеждаемся что используем headless режим
IS_DOCKER = os.path.exists('/.dockerenv') or os.environ.get('RUNNING_IN_DOCKER', False)
if IS_DOCKER:
    print("Запуск в Docker окружении, используем headless режим браузера")
    os.environ["PYPPETEER_HEADLESS"] = "1"

API_URL = "https://xn--80az8a.xn--d1aqf.xn--p1ai/%D1%81%D0%B5%D1%80%D0%B2%D0%B8%D1%81%D1%8B/api/kn/object"
PARAMS = {
    'offset': 0,
    'limit': 200,
    'sortField': 'obj_publ_dt',
    'sortType': 'desc',
    'searchValue': 'уфа',
    'objStatus': '0',
}

PROGRESS_FILE = PROJECT_ROOT / 'domrf_api_progress.json'
JSON_OUTPUT_FILE = PROJECT_ROOT / 'domrf_houses.json'

# Настройки повторных попыток
MAX_RETRIES = 10  # Максимальное количество повторных попыток
RETRY_DELAY = 120  # Задержка между попытками в секундах (10 минут)
RETRY_DELAY_INCREMENT = 60  # Увеличение задержки с каждой попыткой (1 минута)


async def fetch_api_in_browser(page, params):
    query = "&".join(f"{k}={v}" for k, v in params.items())
    url = f"{API_URL}?{query}"
    js_code = f'''
        async () => {{
            const resp = await fetch("{url}", {{
                headers: {{
                    'accept': 'application/json, text/plain, */*',
                    'authorization': 'Basic MTpxd2U=',
                }}
            }});
            if (!resp.ok) return null;
            return await resp.json();
        }}
    '''
    return await page.evaluate(js_code)


def load_progress():
    """Загружает сохраненный прогресс из файла"""
    if os.path.exists(PROGRESS_FILE):
        try:
            with open(PROGRESS_FILE, 'r', encoding='utf-8') as f:
                progress = json.load(f)
                print(f"Загружен прогресс: offset={progress['offset']}, {len(progress['houses'])} домов уже собрано")
                return progress
        except Exception as e:
            print(f"Ошибка при загрузке прогресса: {e}")
    return {'offset': 0, 'houses': []}


def save_progress(offset, houses):
    """Сохраняет текущий прогресс в файл"""
    try:
        progress = {'offset': offset, 'houses': houses}
        with open(PROGRESS_FILE, 'w', encoding='utf-8') as f:
            json.dump(progress, f, ensure_ascii=False, indent=2)
        print(f"Прогресс сохранен: offset={offset}, всего домов: {len(houses)}")
    except Exception as e:
        print(f"Ошибка при сохранении прогресса: {e}")


async def fetch_all_houses():
    # Загружаем сохраненный прогресс
    progress = load_progress()
    houses = progress['houses']
    start_offset = progress['offset']

    retry_count = 0

    while retry_count < MAX_RETRIES:
        print(f"\n🔄 Попытка {retry_count + 1} из {MAX_RETRIES}")

        browser = None
        try:
            # Используем функцию из open_browser.py для создания браузера и страницы
            # (прокси настроено внутри open_browser.py)
            browser, page1 = await setup_stealth_browser()
            print("Страница браузера создана с антидетект-настройками из open_browser.py")

            # Загружаем первую страницу и дождемся её полной загрузки
            try:
                await page1.goto(
                    "https://наш.дом.рф/сервисы/каталог-новостроек/список-объектов/список"
                    "?objStatus=0&search=уфа&residentialBuildings=1",
                    {'waitUntil': 'networkidle2', 'timeout': 30000})
                await asyncio.sleep(3)  # Дополнительная пауза для стабилизации страницы
                print("✅ Первая страница успешно загружена")
            except Exception as e:
                error_message = str(e)
                print(f"⚠️ Ошибка при загрузке первой страницы: {error_message}")

                # Проверяем, является ли это сетевой ошибкой или ошибкой прокси
                is_network_error = any(err in error_message for err in ['ERR_', 'net::', 'timeout', 'Navigation', 'Connection', 'PROXY'])
                if is_network_error:
                    print("🔄 Обнаружена сетевая/прокси ошибка! Перезапускаем с новым прокси...")
                    await asyncio.sleep(2)

                if browser:
                    await browser.close()
                continue

            # Создаем вторую вкладку
            page2 = await create_new_tab(browser)
            print("Вторая вкладка создана")

            # Загрузим вторую страницу с полным ожиданием загрузки
            try:
                print("Переход во второй вкладке")
                await asyncio.sleep(5)
                await page2.goto(
                    "https://наш.дом.рф/сервисы/каталог-новостроек/список-объектов/список?objStatus=0&search=уфа&residentialBuildings=1",
                    {'waitUntil': 'networkidle2', 'timeout': 30000})
                print("✅ Вторая вкладка загружена")
            except Exception as e:
                error_message = str(e)
                print(f"⚠️ Ошибка при загрузке второй вкладки: {error_message}")

                # Проверяем, является ли это сетевой ошибкой или ошибкой прокси
                is_network_error = any(err in error_message for err in ['ERR_', 'net::', 'timeout', 'Navigation', 'Connection', 'PROXY'])
                if is_network_error:
                    print("🔄 Обнаружена сетевая/прокси ошибка! Перезапускаем с новым прокси...")
                    await asyncio.sleep(2)

                if browser:
                    await browser.close()
                continue

            try:
                element_found = False
                try:
                    await page2.waitForSelector('button:has-text("Показать ещё")', {'timeout': 20000})
                    element_found = True
                except Exception:
                    print("Кнопка 'Показать ещё' не найдена, ищем другие элементы...")

                if not element_found:
                    try:
                        await page2.waitForSelector('[class*="NewBuildingItem__Wrapper"]', {'timeout': 20000})
                        element_found = True
                    except Exception as e:
                        print(f"Элементы новостроек не найдены: {e}")

                if not element_found:
                    try:
                        # Проверяем капчу
                        captcha_text = await page2.evaluate('''() => {
                            return document.body.innerText.includes("Нам очень жаль, но запросы с вашего устройства похожи на автоматические");
                        }''')

                        if captcha_text:
                            print("Обнаружена капча после ожидания! Перезапускаем браузер...")
                            if browser:
                                await browser.close()
                            await asyncio.sleep(random.uniform(5, 10))
                            continue
                        else:
                            print("Не удалось загрузить нужные элементы страницы")
                            if browser:
                                await browser.close()
                            continue
                    except Exception as e:
                        print(f"Ошибка при проверке капчи: {e}")
                        if browser:
                            await browser.close()
                        continue
            except Exception as e:
                print(f"Общая ошибка при проверке элементов страницы: {e}")
                if browser:
                    await browser.close()
                continue

            offset = start_offset  # Начинаем с сохраненного offset
            limit = int(PARAMS['limit'])  # Преобразуем строку в число
            page_count = 0
            api_errors = 0
            max_api_errors = 3  # Максимальное количество ошибок API подряд

            while True:
                params = PARAMS.copy()
                params['offset'] = offset
                try:
                    data = await fetch_api_in_browser(page2, params)
                    api_errors = 0  # Сбрасываем счетчик ошибок при успешном запросе
                except Exception as e:
                    api_errors += 1
                    print(f"Ошибка при запросе API (попытка {api_errors}/{max_api_errors}): {e}")

                    if api_errors >= max_api_errors:
                        print(
                            f"Достигнуто максимальное количество ошибок API подряд ({max_api_errors}). Сохраняем прогресс и выходим.")
                        break
                    else:
                        # Небольшая пауза перед повторной попыткой
                        await asyncio.sleep(random.uniform(2, 5))
                        continue

                if not data:
                    print(f"Ошибка или пустой ответ на offset={offset}")
                    break

                batch = data.get('data', {}).get('list', []) or data.get('houses', [])
                if not batch:
                    break

                for house in batch:
                    # Сохраняем все данные дома, не фильтруем по полям
                    houses.append(house)

                print(f"Fetched {len(batch)} houses (offset={offset})")
                page_count += 1

                # Сохраняем прогресс после каждой пачки данных
                save_progress(offset + limit, houses)

                offset += limit
                await asyncio.sleep(random.uniform(0.5, 1.5))  # Случайная пауза между запросами

            if browser:
                await browser.close()

        except Exception as e:
            error_message = str(e)
            print(f"Ошибка при работе с браузером: {e}")

            # Проверяем, является ли это ошибкой прокси
            if "ERR_PROXY_CONNECTION_FAILED" in error_message or "PROXY" in error_message:
                print("🔌 Обнаружена ошибка прокси! Перезапускаем с новым прокси...")
                await asyncio.sleep(2)

            if browser:
                try:
                    await browser.close()
                    print("Браузер закрыт после ошибки")
                except:
                    print("Не удалось закрыть браузер")

        # Если получили данные, выходим из основного цикла
        if houses:
            print("✅ Успешно получили данные, завершаем работу")
            break
        else:
            retry_count += 1
            if retry_count < MAX_RETRIES:
                delay = RETRY_DELAY + (retry_count - 1) * RETRY_DELAY_INCREMENT
                print(f"❌ Не удалось получить данные. Ожидание {delay} секунд перед следующей попыткой...")
                print(f"⏰ Следующая попытка через {delay // 60} минут {delay % 60} секунд")
                await asyncio.sleep(delay)
            else:
                print(f"❌ Достигнуто максимальное количество попыток ({MAX_RETRIES}). Завершаем работу.")

    return houses


def save_houses_to_json(houses, filename):
    """Сохраняет данные домов в JSON файл"""
    try:
        # Если файл уже существует, загружаем существующие данные
        existing_houses = []
        if os.path.exists(filename):
            try:
                with open(filename, 'r', encoding='utf-8') as f:
                    existing_data = json.load(f)
                    if isinstance(existing_data, list):
                        existing_houses = existing_data
                    else:
                        existing_houses = [existing_data]
            except Exception as e:
                print(f"Ошибка при чтении существующего файла {filename}: {e}")
                existing_houses = []

        # Объединяем существующие данные с новыми
        all_houses = existing_houses + houses

        # Сохраняем все данные в JSON файл
        with open(filename, 'w', encoding='utf-8') as f:
            json.dump(all_houses, f, ensure_ascii=False, indent=2)

        print(f"Добавлено {len(houses)} домов в {filename}. Всего в файле: {len(all_houses)} записей")

    except Exception as e:
        print(f"Ошибка при сохранении в JSON файл {filename}: {e}")


async def main_async():
    houses = await fetch_all_houses()
    save_houses_to_json(houses, JSON_OUTPUT_FILE)

    # После успешного сохранения в JSON, можно удалить файл прогресса
    if os.path.exists(PROGRESS_FILE):
        try:
            os.remove(PROGRESS_FILE)
            print(f"Файл прогресса {PROGRESS_FILE} удален")
        except Exception as e:
            print(f"Не удалось удалить файл прогресса: {e}")


def main():
    asyncio.get_event_loop().run_until_complete(main_async())


if __name__ == '__main__':
    main()
