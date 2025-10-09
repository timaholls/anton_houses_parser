"""
Модуль для работы с браузером через Pyppeteer
"""
import asyncio
import time
import random
import os

import pyppeteer

# Path to Chrome executable
EXECUTABLE_PATH = "/usr/bin/google-chrome-stable"

# Proxy configuration - using localhost
PORTS = [3128, 3129, 3130, 3131, 3132, 3133, 3134, 3135, 3136]

def get_random_user_agent():
    """Читает user-agent'ы из файла и возвращает случайный"""
    try:
        # Используем абсолютный путь относительно текущего скрипта
        with open("useragets.txt", encoding='utf-8') as f:
            user_agents = [line.strip() for line in f if line.strip()]
        if not user_agents:
            raise Exception('useragets.txt пустой!')
        return random.choice(user_agents)
    except Exception as e:
        print(f"Ошибка при чтении useragets.txt: {e}")
        # Фолбэк: Chrome user-agent
        return 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/100.0.4896.127 Safari/537.36'


def random_sleep(min_seconds=1, max_seconds=3):
    """Sleep for a random amount of time between min and max seconds"""
    duration = random.uniform(min_seconds, max_seconds)
    print(f"Waiting for {duration:.2f} seconds...")
    time.sleep(duration)


async def setup_stealth_browser():
    """Create a highly stealthed browser to avoid fingerprinting"""
    from pyppeteer import launch
    
    print("Setting up stealth Chrome browser...")

    # Выбираем случайный порт прокси
    proxy_port = random.choice(PORTS)
    proxy_server = f"http://192.168.0.148:{proxy_port}"
    
    print(f"Using proxy: {proxy_server}")


    # Launch browser with configuration
    browser = await launch(
        executablePath=EXECUTABLE_PATH,
        headless=False,
        args=[
            f'--proxy-server={proxy_server}',
            '--disable-dev-shm-usage',
            '--no-sandbox',
            '--disable-setuid-sandbox',
            f'--window-size=1920,1080',
            '--disable-blink-features=AutomationControlled',
            '--disable-features=IsolateOrigins,site-per-process',
        ],
        dumpio=False,
        autoClose=False,
        handleSIGINT=False,
        handleSIGTERM=False,
        handleSIGHUP=False
    )

    # Get the first page (or create new one)
    pages = await browser.pages()
    if pages:
        page = pages[0]
    else:
        page = await browser.newPage()

    # Set viewport
    await page.setViewport({'width': 1920, 'height': 1080})

    # Set user agent
    user_agent = get_random_user_agent()
    await page.setUserAgent(user_agent)

    # Set extra HTTP headers
    await page.setExtraHTTPHeaders({
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
        "Accept-Language": "ru-RU,ru;q=0.9,en-US;q=0.8,en;q=0.7",
        "Sec-Fetch-Dest": "document",
        "Sec-Fetch-Mode": "navigate",
        "Sec-Fetch-Site": "same-origin",
        "Sec-Fetch-User": "?1",
        "Upgrade-Insecure-Requests": "1",
        "DNT": "1"
    })

    # Inject script to hide webdriver
    await page.evaluateOnNewDocument('''() => {
        Object.defineProperty(navigator, 'webdriver', {
            get: () => false
        });
        
        // Chrome runtime
        window.chrome = {
            runtime: {}
        };
        
        // Permissions
        const originalQuery = window.navigator.permissions.query;
        window.navigator.permissions.query = (parameters) => (
            parameters.name === 'notifications' ?
                Promise.resolve({ state: Meteor.Notification.permission }) :
                originalQuery(parameters)
        );
        
        // Plugins
        Object.defineProperty(navigator, 'plugins', {
            get: () => [1, 2, 3, 4, 5]
        });
        
        // Languages
        Object.defineProperty(navigator, 'languages', {
            get: () => ['ru-RU', 'ru', 'en-US', 'en']
        });
    }''')

    # Set default timeouts (in milliseconds)
    page.setDefaultNavigationTimeout(60000)  # 60 seconds

    return browser, page


async def create_new_tab(browser, url=None):
    """
    Создает новую вкладку в существующем браузере.
    
    Args:
        browser: Браузер, в котором нужно создать новую вкладку
        url: Опциональный URL для загрузки на новой вкладке
        
    Returns:
        Новая страница (вкладка) браузера
    """
    # Создаем новую страницу
    page = await browser.newPage()

    # Set viewport
    await page.setViewport({'width': 1920, 'height': 1080})

    # Set extra HTTP headers
    await page.setExtraHTTPHeaders({
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
        "Accept-Language": "ru-RU,ru;q=0.9,en-US;q=0.8,en;q=0.7",
        "Sec-Fetch-Dest": "document",
        "Sec-Fetch-Mode": "navigate",
        "Sec-Fetch-Site": "same-origin",
        "Sec-Fetch-User": "?1",
        "Upgrade-Insecure-Requests": "1",
        "DNT": "1"
    })

    # Inject anti-detection script
    await page.evaluateOnNewDocument('''() => {
        Object.defineProperty(navigator, 'webdriver', {
            get: () => false
        });
        window.chrome = {
            runtime: {}
        };
    }''')

    # Set default timeout
    page.setDefaultNavigationTimeout(60000)

    # Если указан URL, открываем его
    if url:
        try:
            await page.goto(url, {'waitUntil': 'networkidle2', 'timeout': 90000})
            # Дополнительная задержка для стабилизации страницы
            random_sleep(3, 5)
            print(f"URL загружен успешно: {url}")
        except Exception as e:
            print(f"Ошибка при загрузке URL {url}: {e}")
            # Продолжаем работу с вкладкой, даже если URL не загрузился

    return page
