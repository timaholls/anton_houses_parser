#!/usr/bin/env python3
"""
Скрипт для проверки содержимого страницы CIAN.

Использование:
    python check_page.py [номер_страницы]

Пример:
    python check_page.py 3
"""
import asyncio
import sys
from pathlib import Path
from dotenv import load_dotenv

# Директория текущего скрипта
PROJECT_ROOT = Path(__file__).resolve().parent

# Загружаем переменные окружения из .env файла (из корня проекта)
PROJECT_ROOT_PARENT = PROJECT_ROOT.parent
load_dotenv(dotenv_path=PROJECT_ROOT_PARENT / ".env")

from browser_manager import create_browser, create_browser_page
from urllib.parse import urlparse, parse_qs, urlencode, urlunparse

TARGET_URL = "https://ufa.cian.ru/novostroyki-bashkortostan/"


def add_page_param(url: str, page_num: int) -> str:
    """Добавляет или обновляет параметр p= в URL."""
    parsed = urlparse(url)
    query_params = parse_qs(parsed.query)
    query_params['p'] = [str(page_num)]
    new_query = urlencode(query_params, doseq=True)
    new_parsed = parsed._replace(query=new_query)
    return urlunparse(new_parsed)


async def wait_cards_loaded(page) -> None:
    """Ожидаем появления карточек ЖК на странице."""
    try:
        await page.waitForSelector('[data-name="GKCardComponent"]', {"timeout": 60000})
    except Exception as e:
        raise TimeoutError("Не найдены карточки ЖК [data-name='GKCardComponent']") from e


async def check_page(page, page_num: int):
    """Проверяет содержимое страницы."""
    page_url = add_page_param(TARGET_URL, page_num)
    print(f"\n{'='*70}")
    print(f"Проверка страницы {page_num}: {page_url}")
    print(f"{'='*70}\n")
    
    try:
        await page.goto(page_url, timeout=60000)
        await wait_cards_loaded(page)
        await asyncio.sleep(5)  # Даем время на полную загрузку
        
        # Собираем данные через JavaScript
        script = """
        () => {
          const cards = Array.from(document.querySelectorAll('[data-name="GKCardComponent"]'));
          const results = [];
          
          cards.forEach((card, index) => {
            // Находим название ЖК
            const titleElement = card.querySelector('[data-testid="newbuildingTitle"]');
            let title = '';
            let titleDebug = '';
            if (titleElement) {
              const textElements = titleElement.querySelectorAll('span');
              if (textElements.length > 0) {
                const lastSpan = Array.from(textElements).pop();
                title = lastSpan ? lastSpan.textContent.trim() : '';
                titleDebug = `найден [data-testid="newbuildingTitle"], spans: ${textElements.length}`;
              }
              if (!title) {
                title = titleElement.textContent.trim();
                titleDebug = `найден [data-testid="newbuildingTitle"], текст напрямую`;
              }
            } else {
              // Пробуем альтернативные селекторы
              const altTitle = card.querySelector('a[href*="/cat.php"]') || 
                              card.querySelector('a[href*="/novostroyki"]') ||
                              card.querySelector('h2') ||
                              card.querySelector('h3');
              if (altTitle) {
                title = altTitle.textContent.trim();
                titleDebug = `альтернативный селектор: ${altTitle.tagName}`;
              } else {
                titleDebug = 'НЕ НАЙДЕН [data-testid="newbuildingTitle"] и альтернативы';
              }
            }
            
            // Находим ссылку
            const linkElement = card.querySelector('[data-mark="RoomCounts"]');
            let link = '';
            let linkDebug = '';
            let isFromAgents = false;
            let linkText = '';
            if (linkElement) {
              // Получаем текст элемента для проверки на "от агентов"
              linkText = linkElement.textContent || '';
              const linkTextLower = linkText.toLowerCase();
              
              // Проверяем на наличие "от агентов"
              if (linkTextLower.includes('от агентов') || linkTextLower.includes('от агента')) {
                isFromAgents = true;
                linkDebug = `найден [data-mark="RoomCounts"], текст: "${linkText}" - ОТ АГЕНТОВ (ссылка не собирается)`;
              } else {
                // Получаем href атрибут только если не от агентов
                const href = linkElement.getAttribute('href') || linkElement.href || '';
                if (href) {
                  try {
                    if (href.startsWith('http://') || href.startsWith('https://')) {
                      link = href;
                    } else if (href.startsWith('//')) {
                      link = window.location.protocol + href;
                    } else if (href.startsWith('/')) {
                      link = window.location.origin + href;
                    } else {
                      const url = new URL(href, window.location.origin);
                      link = url.href;
                    }
                    linkDebug = `найден [data-mark="RoomCounts"], текст: "${linkText}", href: ${href.substring(0, 50)}`;
                  } catch (e) {
                    link = href;
                    linkDebug = `найден [data-mark="RoomCounts"], текст: "${linkText}", ошибка обработки href`;
                  }
                } else {
                  linkDebug = `найден [data-mark="RoomCounts"], текст: "${linkText}", но нет href`;
                }
              }
            } else {
              // Пробуем альтернативные селекторы для ссылки
              const altLink = card.querySelector('a[href*="/cat.php"]') || 
                             card.querySelector('a[href*="/novostroyki"]') ||
                             card.querySelector('a[href*="newobject"]');
              if (altLink) {
                const href = altLink.getAttribute('href') || altLink.href || '';
                if (href) {
                  try {
                    if (href.startsWith('http://') || href.startsWith('https://')) {
                      link = href;
                    } else if (href.startsWith('//')) {
                      link = window.location.protocol + href;
                    } else if (href.startsWith('/')) {
                      link = window.location.origin + href;
                    } else {
                      const url = new URL(href, window.location.origin);
                      link = url.href;
                    }
                    linkDebug = `альтернативная ссылка: ${altLink.tagName}, href: ${href.substring(0, 50)}`;
                  } catch (e) {
                    linkDebug = `альтернативная ссылка найдена, но ошибка обработки`;
                  }
                } else {
                  linkDebug = 'альтернативная ссылка найдена, но нет href';
                }
              } else {
                linkDebug = 'НЕ НАЙДЕН [data-mark="RoomCounts"] и альтернативы';
              }
            }
            
            // Собираем фотографии
            const photos = [];
            const carousel = card.querySelector('[data-name="CarouselBlock"]') || card.querySelector('[data-name="Carousel"]');
            if (carousel) {
              const images = carousel.querySelectorAll('img');
              images.forEach(img => {
                const src = img.getAttribute('src') || img.getAttribute('data-src') || img.src;
                if (src) {
                  try {
                    const url = new URL(src, window.location.origin).href;
                    if (url.includes('images.cdn-cian.ru') || url.includes('cdn-cian.ru')) {
                      photos.push(url);
                    }
                  } catch (e) {
                    if (src.startsWith('http') && (src.includes('images.cdn-cian.ru') || src.includes('cdn-cian.ru'))) {
                      photos.push(src);
                    }
                  }
                }
              });
            }
            
            const uniquePhotos = Array.from(new Set(photos));
            
            results.push({
              index: index + 1,
              title: title || '(без названия)',
              link: link || '(без ссылки)',
              photos_count: uniquePhotos.length,
              has_title: !!title,
              has_link: !!link,
              isFromAgents: isFromAgents,
              linkText: linkText,
              titleDebug: titleDebug,
              linkDebug: linkDebug
            });
          });
          
          return {
            total: cards.length,
            results: results
          };
        }
        """
        
        data = await page.evaluate(script)
        
        print(f"Всего найдено карточек ЖК: {data['total']}")
        print(f"\nДетальная информация:\n")
        
        valid_count = 0
        invalid_count = 0
        
        agents_count = 0
        
        for item in data['results']:
            is_from_agents = item.get('isFromAgents', False)
            if is_from_agents:
                agents_count += 1
                status = "⚠"
            elif item['has_title'] and item['has_link']:
                status = "✓"
                valid_count += 1
            else:
                status = "✗"
                invalid_count += 1
            
            print(f"{status} [{item['index']:2d}] {item['title']}")
            print(f"      Ссылка: {item['link']}")
            if is_from_agents:
                print(f"      ⚠ ОТ АГЕНТОВ: \"{item.get('linkText', '')}\" (ссылка не собирается)")
            print(f"      Фото: {item['photos_count']} шт.")
            if not item['has_title']:
                print(f"      ⚠ НЕТ НАЗВАНИЯ!")
                print(f"      Отладка: {item.get('titleDebug', 'нет информации')}")
            if not item['has_link'] and not is_from_agents:
                print(f"      ⚠ НЕТ ССЫЛКИ!")
                print(f"      Отладка: {item.get('linkDebug', 'нет информации')}")
            print()
        
        print(f"{'='*70}")
        print(f"Итого:")
        print(f"  Всего карточек: {data['total']}")
        print(f"  Валидных (с названием и ссылкой): {valid_count}")
        print(f"  От агентов (ссылка не собирается): {agents_count}")
        print(f"  Невалидных (нет названия или ссылки): {invalid_count}")
        print(f"{'='*70}\n")
        
        # Проверяем пагинатор
        pagination_script = """
        () => {
          const paginationItems = Array.from(document.querySelectorAll('[data-name="PaginationItem"]'));
          const pageNumbers = [];
          
          paginationItems.forEach(item => {
            const link = item.querySelector('a[data-name="PaginationLink"]');
            if (link) {
              const href = link.getAttribute('href') || link.href;
              if (href) {
                try {
                  const url = new URL(href, window.location.origin);
                  const p = url.searchParams.get('p');
                  if (p) {
                    const pageNum = parseInt(p, 10);
                    if (!isNaN(pageNum) && pageNum > 0) {
                      pageNumbers.push(pageNum);
                    }
                  }
                } catch (e) {
                  const match = href.match(/[&?]p=(\\d+)/);
                  if (match) {
                    const pageNum = parseInt(match[1], 10);
                    if (!isNaN(pageNum) && pageNum > 0) {
                      pageNumbers.push(pageNum);
                    }
                  }
                }
              }
            }
            
            const text = item.textContent.trim();
            const pageNum = parseInt(text, 10);
            if (!isNaN(pageNum) && pageNum > 0 && pageNum <= 1000) {
              pageNumbers.push(pageNum);
            }
          });
          
          if (pageNumbers.length === 0) {
            return 1;
          }
          return Math.max(...pageNumbers);
        }
        """
        
        max_page = await page.evaluate(pagination_script)
        print(f"Максимальная страница в пагинаторе: {max_page}")
        
    except Exception as e:
        print(f"Ошибка при проверке страницы: {e}")
        import traceback
        traceback.print_exc()


async def main():
    """Основная функция."""
    page_num = 3
    
    browser, proxy_url = await create_browser(headless=False)
    page = await create_browser_page(browser, set_domclick_cookies=False)
    
    try:
        await check_page(page, page_num)
    finally:
        await browser.close()


if __name__ == "__main__":
    asyncio.get_event_loop().run_until_complete(main())

