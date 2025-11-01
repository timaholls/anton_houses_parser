#!/usr/bin/env python3
"""
Модуль для управления браузером
Содержит функции для создания и настройки браузера с прокси
"""
import pyppeteer
import random

# Настройки браузера
EXECUTABLE_PATH = "/usr/bin/google-chrome-stable"
PROXY_HOST = "192.168.0.148"
PROXY_PORTS = [3128, 3129, 3130, 3131, 3132, 3133, 3134, 3135, 3136]


def get_random_proxy():
    """Возвращает случайный прокси из пула"""
    port = random.choice(PROXY_PORTS)
    proxy_url = f"http://{PROXY_HOST}:{port}"
    return proxy_url, port


async def create_browser(headless: bool = False):
    """
    Создает и запускает браузер со случайным прокси из пула
    
    Args:
        headless: Запускать браузер в фоновом режиме (True) или с интерфейсом (False)
    
    Returns:
        tuple: (browser, proxy_url) - Объект браузера и использованный прокси
    """
    proxy_url, port = get_random_proxy()

    browser = await pyppeteer.launch(
        executablePath=EXECUTABLE_PATH,
        headless=False,
        ignoreHTTPSErrors=True,
        defaultViewport={'width': 1920, 'height': 1080},
        args=[
            '--no-first-run',
            '--no-sandbox',  # Необходимо для работы в Docker
            '--disable-setuid-sandbox',  # Дополнительная защита для Docker
            '--disable-dev-shm-usage',  # Избегаем проблем с /dev/shm
            '--disable-gpu',  # Отключаем GPU для стабильности
            '--disable-software-rasterizer',
            '--disable-extensions',
            f'--proxy-server={proxy_url}',
        ]
    )
    return browser, proxy_url


async def create_browser_page(browser):
    """
    Создает новую страницу браузера с настроенным User-Agent
    
    Args:
        browser: Объект браузера pyppeteer
    
    Returns:
        page: Объект страницы браузера
    """
    page = await browser.newPage()
    await page.setUserAgent(
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 '
        '(KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
    )
    return page


async def restart_browser(old_browser, headless: bool = False):
    """
    Перезапускает браузер со случайным прокси из пула
    
    Args:
        old_browser: Старый объект браузера для закрытия
        headless: Запускать браузер в фоновом режиме
    
    Returns:
        tuple: (новый браузер, новая страница, proxy_url)
    """
    if old_browser:
        await old_browser.close()

    new_browser, proxy_url = await create_browser(headless)
    new_page = await create_browser_page(new_browser)

    return new_browser, new_page, proxy_url
