// Тестовый скрипт для проверки парсинга страницы квартиры CIAN
// Скопируйте и вставьте этот код в консоль браузера на странице квартиры

(() => {
  console.log('=== Тест парсинга страницы квартиры CIAN ===\n');
  
  const result = {
    title: "",
    main_photo: null,
    price: "",
    price_per_square: "",
    factoids: [],
    summary_info: [],
    decoration: {
      description: "",
      photos: []
    }
  };
  
  // 1. Название квартиры
  console.log('1. Проверка названия (OfferTitleNew):');
  const titleElement = document.querySelector('[data-name="OfferTitleNew"]');
  if (titleElement) {
    result.title = titleElement.textContent.trim();
    console.log(`   ✓ Найдено: "${result.title}"`);
    console.log(`   HTML: ${titleElement.outerHTML.substring(0, 200)}...`);
  } else {
    console.log('   ✗ НЕ найдено!');
    // Альтернативные варианты
    const alt1 = document.querySelector('[data-testid*="title"], [data-testid*="Title"]');
    if (alt1) {
      console.log(`   Альтернатива найдена: ${alt1.textContent.trim().substring(0, 50)}...`);
    }
  }
  console.log('');
  
  // 2. Первая фотография из галереи
  console.log('2. Проверка главной фотографии (OfferGallery):');
  const gallery = document.querySelector('[data-name="OfferGallery"]');
  if (gallery) {
    console.log('   ✓ Галерея найдена');
    const firstImg = gallery.querySelector('img');
    if (firstImg) {
      const src = firstImg.getAttribute('src') || firstImg.getAttribute('data-src') || firstImg.src;
      if (src) {
        try {
          result.main_photo = new URL(src, window.location.origin).href;
          console.log(`   ✓ Фото найдено: ${result.main_photo}`);
        } catch (e) {
          result.main_photo = src.startsWith('http') ? src : window.location.origin + src;
          console.log(`   ✓ Фото найдено: ${result.main_photo}`);
        }
      } else {
        console.log('   ✗ У изображения нет src');
      }
    } else {
      console.log('   ✗ В галерее не найдено img');
      // Показываем структуру галереи
      console.log(`   Структура галереи: ${gallery.outerHTML.substring(0, 300)}...`);
    }
  } else {
    console.log('   ✗ Галерея НЕ найдена!');
    // Альтернативные варианты
    const altGallery = document.querySelector('[data-testid="OfferGallery"], [data-testid*="gallery"]');
    if (altGallery) {
      console.log('   Альтернативная галерея найдена');
    }
  }
  console.log('');
  
  // 2.5. Цена (NewbuildingPriceInfo)
  console.log('2.5. Проверка цены (NewbuildingPriceInfo):');
  const priceInfo = document.querySelector('[data-name="NewbuildingPriceInfo"]');
  if (priceInfo) {
    console.log('   ✓ Блок цены найден');
    
    // Основная цена
    const priceAmount = priceInfo.querySelector('[data-testid="price-amount"]');
    if (priceAmount) {
      result.price = priceAmount.textContent.trim();
      console.log(`   ✓ Цена найдена: "${result.price}"`);
    } else {
      console.log('   ✗ Элемент price-amount НЕ найден');
      // Пробуем найти цену другим способом
      const priceSpans = priceInfo.querySelectorAll('span');
      priceSpans.forEach(span => {
        const text = span.textContent.trim();
        if (text.match(/[\d\s]+[РрP]/)) {
          console.log(`   Альтернативная цена: ${text}`);
          if (!result.price) {
            result.price = text;
          }
        }
      });
    }
    
    // Цена за квадратный метр
    const factItems = priceInfo.querySelectorAll('[data-name="OfferFactItem"]');
    factItems.forEach(item => {
      const spans = item.querySelectorAll('span');
      if (spans.length >= 2) {
        const label = spans[0].textContent.trim();
        const value = spans[1].textContent.trim();
        if (label.includes('метр') || label.includes('м²') || label.includes('квадрат')) {
          result.price_per_square = value;
          console.log(`   ✓ Цена за м² найдена: "${result.price_per_square}"`);
        }
      }
    });
    
    if (!result.price_per_square) {
      console.log('   ✗ Цена за м² НЕ найдена в OfferFactItem');
      // Пробуем найти в других местах
      const allText = priceInfo.textContent;
      const pricePerSquareMatch = allText.match(/([\d\s]+[РрP]\/м²)/);
      if (pricePerSquareMatch) {
        result.price_per_square = pricePerSquareMatch[1];
        console.log(`   Альтернативная цена за м²: ${result.price_per_square}`);
      }
    }
  } else {
    console.log('   ✗ Блок цены НЕ найден!');
    // Альтернативные варианты
    const altPrice = document.querySelector('[data-testid="price-amount"]');
    if (altPrice) {
      result.price = altPrice.textContent.trim();
      console.log(`   Альтернативная цена найдена: ${result.price}`);
    }
  }
  console.log('');
  
  // 3. Параметры (ObjectFactoidsItem)
  console.log('3. Проверка параметров (ObjectFactoidsItem):');
  const factoidItems = document.querySelectorAll('[data-name="ObjectFactoidsItem"]');
  console.log(`   Найдено элементов: ${factoidItems.length}`);
  
  if (factoidItems.length === 0) {
    console.log('   ✗ Элементы НЕ найдены!');
  } else {
    console.log('   ✓ Элементы найдены, проверяем структуру:');
    factoidItems.forEach((item, idx) => {
      if (idx < 5) { // Показываем первые 5
        const textDiv = item.querySelector('div[class*="--text"]');
        if (textDiv) {
          const spans = textDiv.querySelectorAll('span');
          if (spans.length >= 2) {
            const label = spans[0].textContent.trim();
            const value = spans[1].textContent.trim();
            result.factoids.push({ label: label, value: value });
            console.log(`   ${idx + 1}. ${label}: ${value}`);
          } else {
            console.log(`   ${idx + 1}. Неправильная структура (spans: ${spans.length})`);
            console.log(`      HTML: ${textDiv.outerHTML.substring(0, 150)}...`);
          }
        } else {
          console.log(`   ${idx + 1}. Не найден div с классом --text`);
          console.log(`      HTML: ${item.outerHTML.substring(0, 150)}...`);
        }
      }
    });
  }
  console.log('');
  
  // 4. Дополнительные данные (OfferSummaryInfoGroup)
  console.log('4. Проверка дополнительных данных (OfferSummaryInfoGroup):');
  const summaryGroups = document.querySelectorAll('[data-name="OfferSummaryInfoGroup"]');
  console.log(`   Найдено групп: ${summaryGroups.length}`);
  
  if (summaryGroups.length === 0) {
    console.log('   ✗ Группы НЕ найдены!');
  } else {
    console.log('   ✓ Группы найдены, проверяем структуру:');
    summaryGroups.forEach((group, groupIdx) => {
      const items = group.querySelectorAll('[data-name="OfferSummaryInfoItem"]');
      console.log(`   Группа ${groupIdx + 1}: ${items.length} элементов`);
      
      items.forEach((item, itemIdx) => {
        if (groupIdx === 0 && itemIdx < 5) { // Показываем первые 5 из первой группы
          const paragraphs = item.querySelectorAll('p');
          if (paragraphs.length >= 2) {
            const label = paragraphs[0].textContent.trim();
            const value = paragraphs[1].textContent.trim();
            result.summary_info.push({ label: label, value: value });
            console.log(`     ${itemIdx + 1}. ${label}: ${value}`);
          } else {
            console.log(`     ${itemIdx + 1}. Неправильная структура (p: ${paragraphs.length})`);
            console.log(`        HTML: ${item.outerHTML.substring(0, 150)}...`);
          }
        }
      });
    });
  }
  console.log('');
  
  // 5. Отделка квартиры (NewbuildingCurrentDecoration)
  console.log('5. Проверка отделки квартиры (NewbuildingCurrentDecoration):');
  const decoration = document.querySelector('[data-name="NewbuildingCurrentDecoration"]');
  if (decoration) {
    console.log('   ✓ Блок отделки найден');
    
    // Описание
    const subtitle = decoration.querySelector('div[class*="--subtitle"]');
    if (subtitle) {
      result.decoration.description = subtitle.textContent.trim();
      console.log(`   ✓ Описание найдено: "${result.decoration.description.substring(0, 100)}..."`);
    } else {
      console.log('   ✗ Описание НЕ найдено');
      // Пробуем альтернативные варианты
      const altDesc = decoration.querySelector('span, p');
      if (altDesc) {
        console.log(`   Альтернативное описание: ${altDesc.textContent.trim().substring(0, 100)}...`);
      }
    }
    
    // Фотографии отделки
    const gallery = decoration.querySelector('div[class*="--gallery"]');
    if (gallery) {
      const images = gallery.querySelectorAll('img');
      console.log(`   ✓ Галерея отделки найдена: ${images.length} изображений`);
      
      images.forEach((img, idx) => {
        if (idx < 5) { // Показываем первые 5
          const src = img.getAttribute('src') || img.getAttribute('data-src') || img.src;
          if (src) {
            try {
              const url = new URL(src, window.location.origin).href;
              result.decoration.photos.push(url);
              console.log(`     ${idx + 1}. ${url}`);
            } catch (e) {
              if (src.startsWith('http')) {
                result.decoration.photos.push(src);
                console.log(`     ${idx + 1}. ${src}`);
              }
            }
          }
        }
      });
    } else {
      console.log('   ✗ Галерея отделки НЕ найдена');
      // Пробуем найти изображения в самом блоке
      const allImages = decoration.querySelectorAll('img');
      if (allImages.length > 0) {
        console.log(`   Найдено изображений в блоке: ${allImages.length}`);
        allImages.forEach((img, idx) => {
          if (idx < 3) {
            const src = img.getAttribute('src') || img.src;
            console.log(`     ${idx + 1}. ${src}`);
          }
        });
      }
    }
  } else {
    console.log('   ✗ Блок отделки НЕ найден!');
  }
  console.log('');
  
  // Итоговый результат
  console.log('=== Итоговый результат парсинга ===');
  console.log(JSON.stringify(result, null, 2));
  console.log('\n=== Конец теста ===');
  
  return result;
})();

