import sys
import time


def run_stage(stage_name: str, func) -> bool:
    print(f"\n===== Начало этапа: {stage_name} =====", flush=True)
    start = time.time()
    try:
        func()
        elapsed = time.time() - start
        print(f"===== Этап '{stage_name}' завершён за {elapsed:.2f} c =====\n", flush=True)
        return True
    except Exception as e:
        elapsed = time.time() - start
        print(f"❌ Этап '{stage_name}' завершился ошибкой за {elapsed:.2f} c: {e}", flush=True)
        return False


def main():
    # Этап 1: сбор списка домов (создаёт domrf_houses.json)
    from parse_domrf_1 import main as stage1_main
    ok1 = run_stage("Сбор списка домов (parse_domrf_1)", stage1_main)

    if not ok1:
        print("Предыдущий этап завершился ошибкой. Продолжаем со вторым этапом по вашему запросу...", flush=True)

    # Этап 2: сбор деталей по домам и сохранение в MongoDB
    from parse_domrf_2 import main as stage2_main
    ok2 = run_stage("Сбор деталей и запись в MongoDB (parse_domrf_2)", stage2_main)

    from pathlib import Path
    PROJECT_ROOT = Path(__file__).resolve().parent
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

    # Итоговый код возврата: 0 если оба этапа ОК, иначе 1
    sys.exit(0 if (ok1 and ok2) else 1)


if __name__ == "__main__":
    main()
