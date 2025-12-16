import os
import sqlite3

import pandas as pd


def main() -> None:
    print("=" * 60)
    print("РАБОТА С ДАТАСЕТОМ TITANIC")
    print("=" * 60)

    # Проверяем наличие файлов датасета
    csv_files = ["train.csv", "test.csv", "gender_submission.csv"]
    missing_files = []

    for file in csv_files:
        if not os.path.exists(file):
            missing_files.append(file)

    if missing_files:
        print(f"⚠️  Отсутствуют файлы датасета: {', '.join(missing_files)}")
        print("Пожалуйста, скачайте датасет Titanic с Kaggle:")
        print("https://www.kaggle.com/c/titanic/data")
        print(
            "И поместите файлы train.csv, test.csv и gender_submission.csv в текущую папку"
        )
        return

    print("✓ Файлы датасета найдены")

    # Шаг 1: Создаем соединение с базой данных
    conn: sqlite3.Connection = sqlite3.connect("titanic_database.db")
    cursor: sqlite3.Cursor = conn.cursor()

    print("\n1. ЗАГРУЗКА ДАННЫХ В БАЗУ...")

    # Загружаем тренировочные данные
    train_df = pd.read_csv("train.csv")
    train_df.to_sql("passengers_train", conn, if_exists="replace", index=False)
    print(f"✓ Таблица 'passengers_train' создана: {len(train_df)} записей")

    # Загружаем тестовые данные
    test_df = pd.read_csv("test.csv")
    test_df.to_sql("passengers_test", conn, if_exists="replace", index=False)
    print(f"   ✓ Таблица 'passengers_test' создана: {len(test_df)} записей")

    # Загружаем данные для submission
    submission_df = pd.read_csv("gender_submission.csv")
    submission_df.to_sql("submission_template", conn, if_exists="replace", index=False)
    print(f"   ✓ Таблица 'submission_template' создана: {len(submission_df)} записей")

    # Создаем объединенную таблицу для анализа
    combined_df = pd.concat([train_df, test_df], ignore_index=True)
    combined_df.to_sql("all_passengers", conn, if_exists="replace", index=False)
    print(f"   ✓ Таблица 'all_passengers' создана: {len(combined_df)} записей")

    # Шаг 2: Выполняем запросы
    print("\n2. ВЫПОЛНЕНИЕ SQL-ЗАПРОСОВ...")

    # Список запросов с описанием
    queries: list[dict[str, str]] = [
        {
            "name": "1. Первые 10 пассажиров из тренировочной выборки",
            "sql": "SELECT PassengerId, Name, Sex, Age, Survived FROM passengers_train LIMIT 10;",
        },
        {
            "name": "2. Общая статистика по выживанию",
            "sql": """
            SELECT
                COUNT(*) as total_passengers,
                SUM(Survived) as survived,
                ROUND(AVG(Survived) * 100, 2) as survival_rate_percent
            FROM passengers_train;
            """,
        },
        {
            "name": "3. Выживаемость по полу",
            "sql": """
            SELECT
                Sex,
                COUNT(*) as total,
                SUM(Survived) as survived,
                ROUND(AVG(Survived) * 100, 2) as survival_rate_percent
            FROM passengers_train
            GROUP BY Sex
            ORDER BY survival_rate_percent DESC;
            """,
        },
        {
            "name": "4. Выживаемость по классу каюты",
            "sql": """
            SELECT
                Pclass,
                COUNT(*) as total,
                SUM(Survived) as survived,
                ROUND(AVG(Survived) * 100, 2) as survival_rate_percent
            FROM passengers_train
            GROUP BY Pclass
            ORDER BY Pclass;
            """,
        },
        {
            "name": "5. Статистика по возрасту",
            "sql": """
            SELECT
                COUNT(*) as total,
                ROUND(AVG(Age), 2) as avg_age,
                MIN(Age) as min_age,
                MAX(Age) as max_age,
                COUNT(CASE WHEN Age < 18 THEN 1 END) as children
            FROM all_passengers
            WHERE Age IS NOT NULL;
            """,
        },
        {
            "name": "6. Топ-5 самых дорогих билетов",
            "sql": """
            SELECT PassengerId, Name, Pclass, Fare, Embarked
            FROM all_passengers
            WHERE Fare IS NOT NULL
            ORDER BY Fare DESC
            LIMIT 5;
            """,
        },
        {
            "name": "7. JOIN: Объединение тестовых данных с шаблоном submission",
            "sql": """
            SELECT
                t.PassengerId,
                t.Name,
                t.Sex,
                t.Age,
                t.Pclass,
                s.Survived as predicted_survival
            FROM passengers_test t
            LEFT JOIN submission_template s
            ON t.PassengerId = s.PassengerId
            ORDER BY t.PassengerId
            LIMIT 15;
            """,
        },
        {
            "name": "8. JOIN: Детальный анализ семьи (Сибли + Родители/Дети)",
            "sql": """
            SELECT
                Pclass,
                Sex,
                AVG(SibSp) as avg_siblings_spouses,
                AVG(Parch) as avg_parents_children,
                AVG(SibSp + Parch) as avg_family_size,
                COUNT(*) as passenger_count
            FROM all_passengers
            GROUP BY Pclass, Sex
            ORDER BY Pclass, Sex;
            """,
        },
        {
            "name": "9. Пассажиры с максимальным размером семьи",
            "sql": """
            SELECT
                PassengerId,
                Name,
                Pclass,
                SibSp,
                Parch,
                (SibSp + Parch) as family_size,
                CASE
                    WHEN (SibSp + Parch) > 4 THEN 'Большая семья'
                    WHEN (SibSp + Parch) > 1 THEN 'Средняя семья'
                    ELSE 'Маленькая семья/Один'
                END as family_category
            FROM all_passengers
            WHERE SibSp + Parch > 0
            ORDER BY family_size DESC
            LIMIT 10;
            """,
        },
        {
            "name": "10. Анализ по порту посадки",
            "sql": """
            SELECT
                Embarked,
                COUNT(*) as total_passengers,
                ROUND(AVG(Fare), 2) as avg_fare,
                ROUND(AVG(Age), 2) as avg_age,
                SUM(Survived) as survived
            FROM passengers_train
            WHERE Embarked IS NOT NULL
            GROUP BY Embarked
            ORDER BY total_passengers DESC;
            """,
        },
    ]

    # Шаг 3: Выполняем запросы и экспортируем результаты
    print("\n3. ЭКСПОРТ РЕЗУЛЬТАТОВ...")

    # Создаем папки для результатов
    os.makedirs("csv_results", exist_ok=True)
    os.makedirs("json_results", exist_ok=True)

    for i, query_info in enumerate(queries, 1):
        query_name = query_info["name"]
        sql_query = query_info["sql"]

        print(f"\n   Запрос {i}: {query_name}")
        if len(sql_query) > 80:
            print(f"   SQL: {sql_query[:80]}...")
        else:
            print(f"   SQL: {sql_query}")

        try:
            # Выполняем запрос
            df_result = pd.read_sql_query(sql_query, conn)

            # Экспорт в CSV
            csv_filename = f"csv_results/query_{i:02d}.csv"
            df_result.to_csv(csv_filename, index=False, encoding="utf-8")

            # Экспорт в JSON
            json_filename = f"json_results/query_{i:02d}.json"
            df_result.to_json(
                json_filename, orient="records", indent=2, force_ascii=False
            )

            print(f"   ✓ Результат: {len(df_result)} строк")
            print(f"   ✓ CSV: {csv_filename}")
            print(f"   ✓ JSON: {json_filename}")

            # Выводим первые 3 строки для наглядности
            if len(df_result) > 0:
                print("   Пример данных:")
                print(df_result.head(3).to_string(index=False))

        except Exception as e:
            print(f"   ✗ Ошибка: {e}")

    # Шаг 4: Создаем сводный отчет
    print("\n" + "=" * 60)
    print("СВОДНЫЙ ОТЧЕТ")
    print("=" * 60)

    # Общая информация о базе данных
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
    tables = cursor.fetchall()

    print("\nТаблицы в базе данных:")
    for table in tables:
        table_name = table[0]
        cursor.execute(f"SELECT COUNT(*) FROM {table_name};")
        count_result = cursor.fetchone()
        count = count_result[0] if count_result else 0
        print(f"  - {table_name}: {count} записей")

    # Закрываем соединение
    conn.close()

    print("\n" + "=" * 60)
    print("ВЫПОЛНЕНИЕ ЗАВЕРШЕНО!")
    print("=" * 60)
    print("\nРезультаты сохранены в папках:")
    print("  - csv_results/  (файлы CSV)")
    print("  - json_results/ (файлы JSON)")
    print("\nДля просмотра результатов откройте файлы в этих папках.")


if __name__ == "__main__":
    main()
