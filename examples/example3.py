import os
import sqlite3
from textwrap import dedent


def create_and_query_database():
    print("=" * 60)
    print("ПРИМЕР SQLITE С GROUP BY И РАЗНЫМИ РЕЖИМАМИ ВЫВОДА")
    print("=" * 60)

    # Удаляем старую базу данных, если существует
    db_name = "group_by_example.db"
    if os.path.exists(db_name):
        os.remove(db_name)
        print(f"✓ Удалена старая база данных {db_name}")

    # Создаем соединение с базой данных
    conn = sqlite3.connect(db_name)
    cursor = conn.cursor()

    # Создаем таблицу pages
    print("\n1. СОЗДАНИЕ ТАБЛИЦЫ PAGES...")
    cursor.execute(
        """
    CREATE TABLE pages (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        title TEXT NOT NULL,
        theme INTEGER NOT NULL
    );
    """
    )
    print("   ✓ Таблица 'pages' создана")

    # Вставляем данные как в примере
    print("\n2. ВСТАВКА ДАННЫХ...")
    pages_data = [
        ("What is Information", 1),
        ("Amount of Information", 1),
        ("Binary System", 2),
        ("Boolean Lows", 3),
    ]

    cursor.executemany("INSERT INTO pages (title, theme) VALUES (?, ?)", pages_data)
    print(f"   ✓ Вставлено {len(pages_data)} записей")

    # Сохраняем изменения
    conn.commit()

    print("\n3. ЭМУЛЯЦИЯ СЕССИИ SQLite:")
    print("-" * 60)

    # Первый запрос: SELECT title, theme FROM pages;
    print("sqlite> SELECT title, theme FROM pages;")
    cursor.execute("SELECT title, theme FROM pages;")
    rows = cursor.fetchall()

    # Выводим результаты в формате SQLite (без заголовков, через |)
    for row in rows:
        print(f"{row[0]}|{row[1]}")

    # Второй запрос: SELECT theme FROM pages GROUP BY theme;
    print("\nsqlite> SELECT theme FROM pages GROUP BY theme;")
    cursor.execute("SELECT theme FROM pages GROUP BY theme ORDER BY theme;")
    rows = cursor.fetchall()

    for row in rows:
        print(f"{row[0]}")

    # Включаем режим заголовков и колонок (эмулируем .header on и .mode column)
    print("\nsqlite> .header on")
    print("sqlite> .mode column")

    # Третий запрос: SELECT theme, count() FROM pages GROUP BY theme;
    print("\nsqlite> SELECT theme, count() FROM pages GROUP BY theme;")
    cursor.execute("SELECT theme, count() FROM pages GROUP BY theme ORDER BY theme;")
    rows = cursor.fetchall()

    # Получаем описание столбцов
    cursor.execute("SELECT theme, count() FROM pages GROUP BY theme ORDER BY theme;")
    column_names = [description[0] for description in cursor.description]

    # Определяем ширину колонок
    max_theme_len = max(len(str(row[0])) for row in rows)
    max_count_len = max(len(str(row[1])) for row in rows)
    max_header_theme_len = max(len(column_names[0]), max_theme_len)
    max_header_count_len = max(len(column_names[1]), max_count_len)

    # Выводим заголовки
    print(
        f"{column_names[0]:<{max_header_theme_len}} {column_names[1]:<{max_header_count_len}}"
    )
    print(f"{'-' * max_header_theme_len}  {'-' * max_header_count_len}")

    # Выводим данные
    for row in rows:
        print(f"{row[0]:<{max_header_theme_len}} {row[1]:<{max_header_count_len}}")

    # Четвертый запрос: SELECT theme, count() AS num FROM pages GROUP BY theme;
    print("\nsqlite> SELECT theme, count() AS num FROM pages GROUP BY theme;")
    cursor.execute(
        "SELECT theme, count() AS num FROM pages GROUP BY theme ORDER BY theme;"
    )
    rows = cursor.fetchall()

    # Получаем описание столбцов (уже с псевдонимом num)
    cursor.execute(
        "SELECT theme, count() AS num FROM pages GROUP BY theme ORDER BY theme;"
    )
    column_names = [description[0] for description in cursor.description]

    # Определяем ширину колонок
    max_theme_len = max(len(str(row[0])) for row in rows)
    max_num_len = max(len(str(row[1])) for row in rows)
    max_header_theme_len = max(len(column_names[0]), max_theme_len)
    max_header_num_len = max(len(column_names[1]), max_num_len)

    # Выводим заголовки
    print(
        f"{column_names[0]:<{max_header_theme_len}} {column_names[1]:<{max_header_num_len}}"
    )
    print(f"{'-' * max_header_theme_len}  {'-' * max_header_num_len}")

    # Выводим данные
    for row in rows:
        print(f"{row[0]:<{max_header_theme_len}} {row[1]:<{max_header_num_len}}")

    print("\nsqlite>")

    # Дополнительные запросы для обучения
    print("\n4. ДОПОЛНИТЕЛЬНЫЕ ЗАПРОСЫ ДЛЯ ОБУЧЕНИЯ:")
    print("-" * 60)

    # Запрос с ORDER BY
    print("а) Сортировка по теме и количеству:")
    cursor.execute(
        "SELECT theme, count() as count FROM pages GROUP BY theme ORDER BY count DESC;"
    )
    rows = cursor.fetchall()
    for row in rows:
        print(f"   Тема {row[0]}: {row[1]} страниц")

    # Запрос с WHERE и GROUP BY
    print("\nб) Только темы с более чем 1 страницей:")
    cursor.execute(
        "SELECT theme, count() as count FROM pages GROUP BY theme HAVING count() > 1;"
    )
    rows = cursor.fetchall()
    for row in rows:
        print(f"   Тема {row[0]}: {row[1]} страниц")

    # Подробная информация о страницах
    print("\nв) Подробный список всех страниц:")
    cursor.execute("SELECT id, title, theme FROM pages ORDER BY theme, title;")
    rows = cursor.fetchall()
    for row in rows:
        print(f"   ID:{row[0]:3} | Тема:{row[2]:2} | {row[1]}")

    # Экспорт данных
    print("\n5. ЭКСПОРТ ДАННЫХ...")
    export_data(conn)

    # Закрываем соединение
    conn.close()

    print("\n" + "=" * 60)
    print("ВЫПОЛНЕНИЕ ЗАВЕРШЕНО!")
    print("=" * 60)


def export_data(conn):
    """Экспорт данных в различные форматы"""
    cursor = conn.cursor()

    # Создаем папку для экспорта
    os.makedirs("export_group_by", exist_ok=True)

    # Экспорт всех страниц в CSV
    cursor.execute("SELECT * FROM pages;")
    rows = cursor.fetchall()
    column_names = [description[0] for description in cursor.description]

    import csv

    csv_file = "export_group_by/pages.csv"
    with open(csv_file, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(column_names)
        writer.writerows(rows)
    print(f"✓ Все данные экспортированы в {csv_file}")

    # Экспорт результатов GROUP BY в CSV
    cursor.execute(
        "SELECT theme, count() as page_count FROM pages GROUP BY theme ORDER BY theme;"
    )
    rows = cursor.fetchall()

    csv_file = "export_group_by/group_by_results.csv"
    with open(csv_file, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["theme", "page_count"])
        writer.writerows(rows)
    print(f"✓ Результаты GROUP BY экспортированы в {csv_file}")

    # Экспорт в JSON
    import json

    cursor.execute("SELECT * FROM pages;")
    rows = cursor.fetchall()

    data = []
    for row in rows:
        data.append({"id": row[0], "title": row[1], "theme": row[2]})

    json_file = "export_group_by/pages.json"
    with open(json_file, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    print(f"✓ Данные экспортированы в {json_file}")

    # Экспорт группированных данных в JSON
    cursor.execute(
        "SELECT theme, count() as count, GROUP_CONCAT(title, ', ') as titles FROM pages GROUP BY theme;"
    )
    rows = cursor.fetchall()

    grouped_data = []
    for row in rows:
        grouped_data.append(
            {"theme": row[0], "page_count": row[1], "titles": row[2].split(", ")}
        )

    json_file = "export_group_by/grouped_pages.json"
    with open(json_file, "w", encoding="utf-8") as f:
        json.dump(grouped_data, f, ensure_ascii=False, indent=2)
    print(f"✓ Группированные данные экспортированы в {json_file}")


def interactive_sqlite_simulation():
    """Имитация интерактивной сессии SQLite с более точным форматированием"""
    print("\n" + "=" * 60)
    print("ИМИТАЦИЯ ИНТЕРАКТИВНОЙ СЕССИИ SQLite (ТОЧНЫЙ ФОРМАТ)")
    print("=" * 60)

    db_name = "group_by_example.db"
    if not os.path.exists(db_name):
        print(f"База данных {db_name} не найдена!")
        return

    conn = sqlite3.connect(db_name)
    cursor = conn.cursor()

    print("\n# sqlite> SELECT title, theme FROM pages;")

    cursor.execute("SELECT title, theme FROM pages ORDER BY theme, title;")
    rows = cursor.fetchall()

    for row in rows:
        print(f"{row[0]}|{row[1]}")

    print("\nsqlite> SELECT theme FROM pages GROUP BY theme;")

    cursor.execute("SELECT theme FROM pages GROUP BY theme ORDER BY theme;")
    rows = cursor.fetchall()

    for row in rows:
        print(f"{row[0]}")

    print("\nsqlite> .header on")
    print("sqlite> .mode column")
    print("\nsqlite> SELECT theme,count() FROM pages GROUP BY theme;")

    cursor.execute("SELECT theme,count() FROM pages GROUP BY theme ORDER BY theme;")
    rows = cursor.fetchall()

    # Более точное форматирование как в примере (с тремя дефисами)
    print("theme    count()")
    print("---  ---")
    for row in rows:
        print(f"{row[0]}    {row[1]}")

    print("\nsqlite> SELECT theme, count() AS num FROM pages GROUP BY theme;")

    cursor.execute(
        "SELECT theme, count() AS num FROM pages GROUP BY theme ORDER BY theme;"
    )
    rows = cursor.fetchall()

    print("theme    num")
    print("---  ---")
    for row in rows:
        print(f"{row[0]}    {row[1]}")

    print("\nsqlite>")

    conn.close()


def create_sqlite_script():
    """Создание SQL-скрипта для выполнения в SQLite"""
    print("\n" + "=" * 60)
    print("СОЗДАНИЕ SQL-СКРИПТА ДЛЯ ВЫПОЛНЕНИЯ В SQLite")
    print("=" * 60)

    sql_script = """-- SQL-скрипт для повторения примера
-- Создание таблицы
CREATE TABLE IF NOT EXISTS pages (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    title TEXT NOT NULL,
    theme INTEGER NOT NULL
);

-- Очистка таблицы (если нужно)
DELETE FROM pages;

-- Вставка данных
INSERT INTO pages (title, theme) VALUES
    ('What is Information', 1),
    ('Amount of Information', 1),
    ('Binary System', 2),
    ('Boolean Lows', 3);

-- Запрос 1: Все страницы
SELECT title, theme FROM pages;

-- Запрос 2: Уникальные темы
SELECT theme FROM pages GROUP BY theme;

-- Включение заголовков и режима колонок
.header on
.mode column

-- Запрос 3: Группировка с подсчетом
SELECT theme, count() FROM pages GROUP BY theme;

-- Запрос 4: Группировка с псевдонимом
SELECT theme, count() AS num FROM pages GROUP BY theme;

-- Дополнительные полезные запросы
-- Все данные
SELECT * FROM pages;

-- Статистика по темам
SELECT theme, count() as page_count, GROUP_CONCAT(title) as titles
FROM pages
GROUP BY theme
ORDER BY page_count DESC;
"""

    script_file = "sqlite_group_by_script.sql"
    with open(script_file, "w", encoding="utf-8") as f:
        f.write(sql_script)

    print(f"✓ SQL-скрипт создан: {script_file}")
    print("\nДля выполнения в SQLite:")
    print(f"  sqlite3 my_database.db < {script_file}")
    print("\nИли в интерактивном режиме:")
    print(f"  sqlite3 my_database.db")
    print(f"  .read {script_file}")


def main():
    # Основной пример
    create_and_query_database()

    # Точная имитация формата вывода
    interactive_sqlite_simulation()

    # Создание SQL-скрипта
    create_sqlite_script()

    print("\n" + "=" * 60)
    print("ИНСТРУКЦИЯ ПО ИСПОЛЬЗОВАНИЮ:")
    print("=" * 60)
    print("\n1. Для проверки базы данных вручную:")
    print(f"   sqlite3 group_by_example.db")
    print("\n2. Полезные команды SQLite:")
    print("   .tables                    - показать таблицы")
    print("   .schema pages              - структура таблицы")
    print("   .headers ON               - включить заголовки")
    print("   .mode column              - формат колонок")
    print("   .mode list                - формат списка (|)")
    print("   SELECT * FROM pages;      - все записи")
    print("\n3. SQL-скрипт для повторения:")
    print("   sqlite3 group_by_example.db < sqlite_group_by_script.sql")


if __name__ == "__main__":
    main()
