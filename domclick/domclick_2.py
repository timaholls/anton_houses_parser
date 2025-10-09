#!/usr/bin/env python3
"""
Проход по списку URL и сбор данных с карточек на страницах (только 1-й элемент).

Логика:
- Читает JSON со ссылками (complex_links.json по умолчанию)
- Для каждого URL определяет количество страниц по селектору пагинации
- Переходит по страницам с параметром offset (шаг 20): 0, 20, 40, ...
- На каждой странице ждёт [data-e2e-id="offers-list__item"], берёт первый элемент и
  собирает: изображения из галереи, параметры квартиры, адрес, название/ссылку ЖК
- Результаты пишет в offers_data.json (массив объектов)
- Прогресс хранит в progress_domclick_2.json: {"url_index": i, "offset": n}
- При ошибках делает до 3 попыток; после 3-й — перезапускает браузер (новый прокси)
  и продолжает с того же места
"""
import asyncio
import json
import os
from typing import List, Dict, Any, Tuple
from pathlib import Path

# Директория текущего скрипта
PROJECT_ROOT = Path(__file__).resolve().parent

from browser_manager import create_browser, create_browser_page, restart_browser
from db_manager import save_to_mongodb


LINKS_FILE = PROJECT_ROOT / "complex_links.json"
PROGRESS_FILE = PROJECT_ROOT / "progress_domclick_2.json"
OUTPUT_FILE = PROJECT_ROOT / "offers_data.json"  # больше не используется как основной, оставим для отладки
START_PAUSE_SECONDS = 5  # пауза после открытия URL
STEP_PAUSE_SECONDS = 5   # пауза между страницами/шагами


def load_links(path: str = LINKS_FILE) -> List[str]:
    with open(path, "r", encoding="utf-8") as f:
        data = json.load(f)
    # допускаем как список, так и словарь с ключом links
    if isinstance(data, dict) and "links" in data:
        return list(data.get("links") or [])
    return list(data or [])


def load_progress(path: str = PROGRESS_FILE) -> Tuple[int, int]:
    if not os.path.exists(path):
        return 0, 0
    try:
        with open(path, "r", encoding="utf-8") as f:
            obj = json.load(f)
        return int(obj.get("url_index", 0)), int(obj.get("offset", 0))
    except Exception:
        return 0, 0


def save_progress(url_index: int, offset: int, path: str = PROGRESS_FILE) -> None:
    tmp_path = path + ".tmp"
    with open(tmp_path, "w", encoding="utf-8") as f:
        json.dump({"url_index": url_index, "offset": offset}, f, ensure_ascii=False)
    os.replace(tmp_path, path)

def set_offset_param(url: str, offset: int) -> str:
    try:
        from urllib.parse import urlparse, parse_qsl, urlencode, urlunparse
        parts = urlparse(url)
        q = dict(parse_qsl(parts.query, keep_blank_values=True))
        q["offset"] = str(offset)
        new_query = urlencode(q, doseq=True)
        return urlunparse((parts.scheme, parts.netloc, parts.path, parts.params, new_query, parts.fragment))
    except Exception:
        sep = '&' if ('?' in url) else '?'
        return f"{url}{sep}offset={offset}"


class SkipUrlException(Exception):
    pass


async def wait_offers(page) -> None:
    try:
        await page.waitForSelector('[data-e2e-id="offers-list__item"]', {"timeout": 60000})
    except Exception as e:
        # Проверяем экран "Поиск не дал результатов"
        try:
            no_results = await page.evaluate("""
            () => {
              const t = (document.body && document.body.innerText) || '';
              return /Поиск не дал результатов/i.test(t);
            }
            """)
        except Exception:
            no_results = False
        if no_results:
            raise SkipUrlException("Поиск не дал результатов")
        # Иначе триггерим ретраи/перезапуск
        raise TimeoutError("Не найден список офферов [data-e2e-id='offers-list__item']") from e


async def get_pages_count(page) -> int:
    script = """
    () => {
      const nodes = Array.from(document.querySelectorAll('[data-e2e-id^="paginate-item-"]'));
      return nodes.length || 1;
    }
    """
    try:
        count = await page.evaluate(script)
        return int(count) if isinstance(count, (int, float)) else 1
    except Exception:
        return 1


async def collect_page_items_grouped(page) -> Dict[str, Any]:
    script = """
    () => {
      const toAbs = (u) => { try { return new URL(u, location.origin).href; } catch { return u || null; } };
      const isPngJpg = (u) => /\.(png|jpe?g)(?:$|\?|#)/i.test(String(u || ''));
      const pickFromSrcset = (srcset) => {
        if (!srcset) return null;
        const first = String(srcset).split(',')[0].trim().split(' ')[0];
        return first || null;
      };
      const extractImagesFromGallery = (root) => {
        if (!root) return [];
        const urls = new Set();
        root.querySelectorAll('img').forEach(img => {
          [img.getAttribute('src'), img.getAttribute('data-src'), img.getAttribute('data-lazy'), img.getAttribute('data-original')]
            .filter(Boolean).map(toAbs).filter(isPngJpg).forEach(u => urls.add(u));
          const picked = pickFromSrcset(img.getAttribute('srcset'));
          if (isPngJpg(picked)) urls.add(toAbs(picked));
        });
        root.querySelectorAll('source[srcset]').forEach(s => {
          const picked = pickFromSrcset(s.getAttribute('srcset'));
          if (isPngJpg(picked)) urls.add(toAbs(picked));
        });
        root.querySelectorAll("[style*='background']").forEach(el => {
          const st = el.getAttribute('style') || '';
          const m = st.match(/url\((['\"]?)(.*?)\1\)/i);
          if (m && isPngJpg(m[2])) urls.add(toAbs(m[2]));
        });
        return [...urls];
      };

      const normalizeRoom = (txt) => {
        if (!txt) return 'Другое';
        const s = txt.toLowerCase();
        if (s.includes('студ')) return 'Студия';
        const m = s.match(/(^|\s)([1-9]+)\s*[-–—]?\s*комн/i);
        if (m) return `${m[2]}-комн`;
        const m2 = s.match(/^([1-9]+)\s*[-–—]?\s*комм?/);
        if (m2) return `${m2[1]}-комн`;
        return 'Другое';
      };

      const items = Array.from(document.querySelectorAll('[data-e2e-id="offers-list__item"]'));
      if (!items.length) return null;

      const first = items[0];
      const addressEl = first.querySelector('[data-e2e-id="product-snippet-address"], [data-e2-id="product-snippet-address"]');
      const addressText = addressEl ? addressEl.textContent.replace(/\s+/g, ' ').trim() : null;
      const complexLinkEl = first.querySelector('a[href^="/complexes/"], a[href*="//domclick.ru/complexes/"], a[href*="//ufa.domclick.ru/complexes/"]');
      const complexName = complexLinkEl ? complexLinkEl.textContent.replace(/\s+/g, ' ').trim() : null;
      const complexHref = complexLinkEl ? toAbs(complexLinkEl.getAttribute('href') || complexLinkEl.href) : null;

      const offers = {};
      for (const item of items) {
        const offerEl = item.querySelector('[data-test="product-snippet-property-offer"]');
        const offerText = offerEl ? offerEl.innerText.replace(/\s+/g, ' ').trim() : null;
        const roomKey = normalizeRoom(offerText);
        const galleryRoot = item.querySelector('[data-e2e-id="product-snippet-gallery"]') || item.querySelector('[data-e2e-id^="product-snippet-gallery"]') || item;
        const images = extractImagesFromGallery(galleryRoot);
        const card = { offer: offerText, images };
        if (!offers[roomKey]) offers[roomKey] = [];
        offers[roomKey].push(card);
      }

      return { address: addressText, complexName, complexHref, offers };
    }
    """
    try:
        return await page.evaluate(script)
    except Exception:
        return None


async def run() -> None:
    urls = load_links(LINKS_FILE)
    if not urls:
        print("Файл со ссылками пуст или отсутствует:", LINKS_FILE)
        return

    url_index, offset = load_progress(PROGRESS_FILE)
    url_index = max(0, min(url_index, len(urls)))
    print(f"Старт: url_index={url_index}, offset={offset}, всего URL: {len(urls)}")

    results: List[Dict[str, Any]] = []
    if os.path.exists(OUTPUT_FILE):
        try:
            with open(OUTPUT_FILE, 'r', encoding='utf-8') as f:
                old = json.load(f)
                if isinstance(old, list):
                    results = old
        except Exception:
            pass

    browser, _ = await create_browser(headless=False)
    page = await create_browser_page(browser)

    try:
        while url_index < len(urls):
            base_url = urls[url_index]
            print(f"→ URL [{url_index+1}/{len(urls)}]: {base_url}")

            if offset % 20 != 0:
                offset = (offset // 20) * 20

            attempts = 0
            while True:
                try:
                    await page.goto(set_offset_param(base_url, 0), timeout=120000)
                    # Пауза для стабильной загрузки динамики
                    await asyncio.sleep(START_PAUSE_SECONDS)
                    await wait_offers(page)
                    pages_count = await get_pages_count(page)
                    break
                except SkipUrlException as e:
                    print(f"URL без результатов: {e}. Пропускаю {base_url}")
                    pages_count = 0
                    break
                except Exception as e:
                    attempts += 1
                    print(f"Ошибка при открытии базовой страницы: {e} (попытка {attempts}/3)")
                    if attempts >= 3:
                        browser, page, _ = await restart_browser(browser, headless=False)
                        attempts = 0
                    else:
                        await asyncio.sleep(2)

            total_pages = max(1, pages_count)

            # агрегируем данные по всем оффсетам для текущего URL
            aggregated_address: str = None
            aggregated_complex_name: str = None
            aggregated_complex_href: str = None
            aggregated_offers: Dict[str, List[Dict[str, Any]]] = {}

            while True:
                if offset >= total_pages * 20:
                    break
                current_url = set_offset_param(base_url, offset)
                print(f"  Переход: offset={offset} → {current_url}")

                attempts = 0
                while True:
                    try:
                        await page.goto(current_url, timeout=120000)
                        # Пауза между шагами для докрутки ленивых элементов
                        await asyncio.sleep(STEP_PAUSE_SECONDS)
                        await wait_offers(page)
                        data = await collect_page_items_grouped(page)
                        # Если не нашли нужные элементы — считаем за ошибку шага
                        if not data or not (data.get("offers") or {}):
                            raise ValueError("Селекторы не найдены или offers пустой")

                        if aggregated_address is None:
                            aggregated_address = data.get("address")
                            aggregated_complex_name = data.get("complexName")
                            aggregated_complex_href = data.get("complexHref")
                        # объединяем группы офферов
                        offers = data.get("offers") or {}
                        for group, cards in offers.items():
                            if group not in aggregated_offers:
                                aggregated_offers[group] = []
                            aggregated_offers[group].extend(cards)

                        offset += 20
                        # Небольшая дополнительная пауза перед следующим оффсетом
                        await asyncio.sleep(1)
                        save_progress(url_index, offset)
                        break
                    except SkipUrlException as e:
                        print(f"Offset без результатов: {e}. Завершаю обработку URL: {base_url}")
                        offset = total_pages * 20
                        break
                    except Exception as e:
                        attempts += 1
                        print(f"  Ошибка на странице offset={offset}: {e} (попытка {attempts}/3)")
                        if attempts >= 3:
                            browser, page, _ = await restart_browser(browser, headless=False)
                            attempts = 0
                        else:
                            await asyncio.sleep(2)

            # формируем запись под Mongo-схему
            def to_db_item() -> Dict[str, Any]:
                apartment_types: Dict[str, Any] = {}
                for group, cards in (aggregated_offers or {}).items():
                    apartment_types[group] = {
                        "apartments": [
                            {
                                "title": c.get("offer"),
                                "images": c.get("images") or [],
                            }
                            for c in cards
                        ]
                    }
                return {
                    "url": aggregated_complex_href or base_url,
                    "development": {
                        "complex_name": aggregated_complex_name,
                        "address": aggregated_address,
                        "source_url": base_url,
                    },
                    "apartment_types": apartment_types,
                }

            db_item = to_db_item()
            try:
                save_to_mongodb([db_item])
            except Exception as e:
                print(f"Ошибка записи в MongoDB: {e}. Сохраню в {OUTPUT_FILE} для отладки.")
                results.append({"sourceUrl": base_url, "data": {"address": aggregated_address, "complexName": aggregated_complex_name, "complexHref": aggregated_complex_href, "offers": aggregated_offers}})
                with open(OUTPUT_FILE, 'w', encoding='utf-8') as f:
                    json.dump(results, f, ensure_ascii=False, indent=2)

            url_index += 1
            offset = 0
            save_progress(url_index, offset)
    finally:
        await browser.close()


if __name__ == "__main__":
    asyncio.get_event_loop().run_until_complete(run())
