#!/usr/bin/env python3
"""
Единый запуск: сначала domclick_1.py (сбор ссылок), пауза 10с, затем domclick_2.py (сбор карточек).
"""
import subprocess
import sys
import time
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parent
PYTHON = sys.executable or "python3"


def run_script(path: Path) -> int:
    print(f"Запуск: {path}", flush=True)
    # Добавляем -u для отключения буферизации Python
    # stdout=None и stderr=None означают прямой вывод в консоль
    proc = subprocess.run([PYTHON, "-u", str(path)], cwd=str(PROJECT_ROOT))
    print(f"Завершён: {path} (код {proc.returncode})", flush=True)
    return proc.returncode


def main() -> None:
    first = PROJECT_ROOT / "domclick_1.py"
    second = PROJECT_ROOT / "domclick_2.py"

    code1 = run_script(first)
    if code1 != 0:
        print("Внимание: domclick_1.py завершился с ошибкой. Продолжаю запуск domclick_2.py.", flush=True)

    print("\nПауза 10 секунд перед запуском второго скрипта...", flush=True)
    time.sleep(10)

    code2 = run_script(second)
    if code2 != 0:
        print("domclick_2.py завершился с ошибкой.", flush=True)

    # Удаляем временные файлы
    temp_files = ["complex_links.json", "progress_domclick_2.json"]
    for file_name in temp_files:
        file_path = PROJECT_ROOT / file_name
        if file_path.exists():
            try:
                file_path.unlink()
                print(f"Удален временный файл: {file_name}", flush=True)
            except Exception as e:
                print(f"Ошибка при удалении {file_name}: {e}", flush=True)
        else:
            print(f"Файл не найден: {file_name}", flush=True)


if __name__ == "__main__":
    main()


