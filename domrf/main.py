import sys
import time


def run_stage(stage_name: str, func) -> bool:
    print(f"\n===== Начало этапа: {stage_name} =====")
    start = time.time()
    try:
        func()
        elapsed = time.time() - start
        print(f"===== Этап '{stage_name}' завершён за {elapsed:.2f} c =====\n")
        return True
    except Exception as e:
        elapsed = time.time() - start
        print(f"❌ Этап '{stage_name}' завершился ошибкой за {elapsed:.2f} c: {e}")
        return False


def main():
    # Этап 1: сбор списка домов (создаёт domrf_houses.json)
    from parse_domrf_1 import main as stage1_main
    ok1 = run_stage("Сбор списка домов (parse_domrf_1)", stage1_main)

    if not ok1:
        print("Предыдущий этап завершился ошибкой. Продолжаем со вторым этапом по вашему запросу...")

    # Этап 2: сбор деталей по домам и сохранение в MongoDB
    from parse_domrf_2 import main as stage2_main
    ok2 = run_stage("Сбор деталей и запись в MongoDB (parse_domrf_2)", stage2_main)

    # Итоговый код возврата: 0 если оба этапа ОК, иначе 1
    sys.exit(0 if (ok1 and ok2) else 1)


if __name__ == "__main__":
    main()




