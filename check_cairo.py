#!/usr/bin/env python3
"""
Скрипт для проверки доступности Cairo на Windows
"""
import sys

print("Проверка доступности Cairo...")
print("-" * 50)

try:
    import cairosvg
    print("✅ cairosvg импортирован успешно")
    
    # Пробуем простую операцию
    try:
        from PIL import Image
        import io
        svg_content = b'<svg xmlns="http://www.w3.org/2000/svg"><rect width="100" height="100"/></svg>'
        png_bytes = cairosvg.svg2png(bytestring=svg_content)
        print("✅ Cairo работает корректно!")
        print(f"   Размер PNG: {len(png_bytes)} байт")
    except Exception as e:
        print(f"❌ Cairo установлен, но не работает: {e}")
        print("   Возможно, не установлена системная библиотека Cairo")
        
except ImportError as e:
    print(f"❌ cairosvg не установлен: {e}")
    print("   Установите: pip install cairosvg")
except Exception as e:
    print(f"❌ Ошибка при импорте cairosvg: {e}")
    print("   Возможно, не установлена системная библиотека Cairo")
    print("\nСпособы установки Cairo на Windows:")
    print("1. GTK+ для Windows: https://github.com/tschoonj/GTK-for-Windows-Runtime-Environment-Installer")
    print("2. Conda: conda install -c conda-forge cairo")
    print("3. vcpkg: vcpkg install cairo")

print("-" * 50)
print("\nПримечание: Если Cairo недоступен, скрипт будет работать")
print("без водяного знака (изображения загружаются как есть).")

