import pyppeteer
import random


# Настройки браузера
#EXECUTABLE_PATH = "C:\Program Files\Google\Chrome\Application\chrome.exe"
EXECUTABLE_PATH = "/usr/bin/google-chrome-stable"
PROXY_HOST = "192.168.0.148"
PROXY_PORTS = [3128, 3129, 3130, 3131, 3132, 3133, 3134, 3135, 3136]

# Реальные cookies из браузера
COOKIES = [
    {'name': 'adtech_uid', 'value': '956555b2-a1c0-4869-9711-aad49853daa0%3Adomclick.ru', 'domain': '.domclick.ru', 'path': '/'},
    {'name': 'top100_id', 'value': 't1.7711713.518704900.1754314029039', 'domain': '.domclick.ru', 'path': '/'},
    {'name': 'adrcid', 'value': 'ACT9CAjniQR3Pc6q5FFBxAg', 'domain': '.domclick.ru', 'path': '/'},
    {'name': 'tmr_lvid', 'value': 'ffd13db73c075de563df1d6ecbd071c5', 'domain': '.domclick.ru', 'path': '/'},
    {'name': 'tmr_lvidTS', 'value': '1754314029651', 'domain': '.domclick.ru', 'path': '/'},
    {'name': 'adrdel', 'value': '1754986797342', 'domain': '.domclick.ru', 'path': '/'},
    {'name': 'logoSuffix', 'value': '', 'domain': '.domclick.ru', 'path': '/'},
    {'name': 'iosAppLink', 'value': '', 'domain': '.domclick.ru', 'path': '/'},
    {'name': 'cookieAlert', 'value': '1', 'domain': '.domclick.ru', 'path': '/'},
    {'name': 'RETENTION_COOKIES_NAME', 'value': 'fdec34956162482380b8f19cdd92ad46:gMy0E3ELvVBN2W_xUB1BCHKk94s', 'domain': '.domclick.ru', 'path': '/'},
    {'name': 'sessionId', 'value': '332e995495654d16876ddfa0b43929d9:SafvGzdIYWwCRP7iDkbFHePoMLI', 'domain': '.domclick.ru', 'path': '/'},
    {'name': 'UNIQ_SESSION_ID', 'value': '0d5e5a921bb64bc1b8b645eb59b5921f:8YnvhGNM8tfIwu_0loyDOk2zSx4', 'domain': '.domclick.ru', 'path': '/'},
    {'name': 'regionAlert', 'value': '1', 'domain': '.domclick.ru', 'path': '/'},
    {'name': 'currentLocalityGuid', 'value': '857c0a08-7dc0-445e-a044-ed2f6d435a7b', 'domain': '.domclick.ru', 'path': '/'},
    {'name': 'currentRegionGuid', 'value': '1691f4a5-8e87-41ab-b0d3-05a0c7a07c76', 'domain': '.domclick.ru', 'path': '/'},
    {'name': 'currentSubDomain', 'value': 'ufa', 'domain': '.domclick.ru', 'path': '/'},
    {'name': 'regionName', 'value': '857c0a08-7dc0-445e-a044-ed2f6d435a7b:%D0%A3%D1%84%D0%B0', 'domain': '.domclick.ru', 'path': '/'},
    {'name': 'is-green-day-banner-hidden', 'value': 'true', 'domain': '.domclick.ru', 'path': '/'},
    {'name': 'is-ddf-banner-hidden', 'value': 'true', 'domain': '.domclick.ru', 'path': '/'},
    {'name': 'favoriteHintShowed', 'value': 'true', 'domain': '.domclick.ru', 'path': '/'},
    {'name': 'qrator_jsr', 'value': 'v2.0.1761037620.947.88a9ad25rDzYOeVe|4LxD3bxKS6L6XoyR|VnC9cWvf5bV6F0IdpH3c9+yGXDLzjgNwZHwLJVNbjSXykIuoMAAYk5vr7Ss6vTk/SDXX/10jqieGYoz97fF12A==-P5YitRwSbxqF+YQanwdL6UxBLEo=-00', 'domain': '.domclick.ru', 'path': '/'},
    {'name': 'qrator_jsid2', 'value': 'v2.0.1761037620.947.88a9ad25rDzYOeVe|MRXDyas0iEJzZVU7|3tJ2Kx3Q0+T+bpfvj5LpWymrdKwPZjUv+HpAl1r1JUSGVuFRRx5ysqoEAtHqvq3R7A3wNcrA9Xsu5Wqlu14KY9KIcKICE5Zc58ewMQYPMMBVJwB6/SqQUPAPXF+eeNqUNAxr34uznBF2bZj/bTlFPg==-EAJMOdGEXhgch6jfBf3LJ46ZYh8=', 'domain': '.domclick.ru', 'path': '/'},
    {'name': 'project-3518', 'value': '1148898-3', 'domain': '.domclick.ru', 'path': '/'},
    {'name': 'tmr_detect', 'value': '0%7C1761037628523', 'domain': '.domclick.ru', 'path': '/'},
    {'name': 't3_sid_7711713', 'value': 's1.2069167602.1761037622980.1761037634519.9.5.1.0..', 'domain': '.domclick.ru', 'path': '/'},
    {'name': 'tmr_reqNum', 'value': '250', 'domain': '.domclick.ru', 'path': '/'},
]


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
    pyppeteer.launcher.DEFAULT_ARGS = []
    browser = await pyppeteer.launch(
        executablePath=EXECUTABLE_PATH,
        headless=headless,
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


async def set_cookies(page, cookies=None):
    """
    Устанавливает cookies для страницы
    
    Args:
        page: Объект страницы браузера
        cookies: Список словарей с cookies (если None, использует COOKIES по умолчанию)
    """
    if cookies is None:
        cookies = COOKIES
    
    # Сначала переходим на домен, чтобы установить cookies
    await page.goto('https://domclick.ru')
    
    # Устанавливаем каждую cookie
    for cookie in cookies:
        await page.setCookie(cookie)
    
    print(f"✓ Установлено {len(cookies)} cookies для domclick.ru")


async def create_browser_page(browser, set_domclick_cookies=True):
    """
    Создает новую страницу браузера с настроенным User-Agent и cookies
    
    Args:
        browser: Объект браузера pyppeteer
        set_domclick_cookies: Устанавливать ли cookies для domclick.ru (по умолчанию True)
    
    Returns:
        page: Объект страницы браузера
    """
    page = await browser.newPage()
    
    # Устанавливаем cookies если требуется
    if set_domclick_cookies:
        await set_cookies(page)
    
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
    # Закрываем старый браузер перед созданием нового
    if old_browser:
        try:
            await old_browser.close()
        except Exception:
            pass  # Игнорируем ошибки закрытия, браузер может быть уже закрыт
    
    # Создаем новый браузер
    try:
        new_browser, proxy_url = await create_browser(headless)
        new_page = await create_browser_page(new_browser)
        return new_browser, new_page, proxy_url
    except Exception as e:
        # Если не удалось создать новый браузер, убеждаемся что старый закрыт
        if old_browser:
            try:
                await old_browser.close()
            except Exception:
                pass
        raise  # Пробрасываем исключение дальше

