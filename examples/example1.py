import os
import sqlite3


def create_and_populate_database():
    print("=" * 60)
    print("СОЗДАНИЕ БАЗЫ ДАННЫХ SQLITE С ТАБЛИЦЕЙ PAGES")
    print("=" * 60)

    # Удаляем старую базу данных, если существует
    if os.path.exists("example.db"):
        os.remove("example.db")
        print("✓ Удалена старая база данных")

    # Создаем соединение с базой данных
    conn = sqlite3.connect("example.db")
    cursor = conn.cursor()

    # Включаем поддержку внешних ключей
    cursor.execute("PRAGMA foreign_keys = ON;")
    print("✓ Включена поддержка внешних ключей")

    # Создаем таблицу sections для внешнего ключа
    print("\n1. СОЗДАНИЕ ТАБЛИЦЫ SECTIONS...")
    cursor.execute(
        """
    CREATE TABLE IF NOT EXISTS sections (
        _id INTEGER PRIMARY KEY,
        name TEXT NOT NULL
    );
    """
    )

    # Вставляем данные в sections
    cursor.executemany(
        """
    INSERT INTO sections (_id, name) VALUES (?, ?)
    """,
        [
            (1, "Основы информации"),
            (2, "Кодирование данных"),
            (3, "Теория вероятностей"),
        ],
    )
    print("   ✓ Таблица 'sections' создана и заполнена")

    # Создаем таблицу pages
    print("\n2. СОЗДАНИЕ ТАБЛИЦЫ PAGES...")
    cursor.execute(
        """
    CREATE TABLE IF NOT EXISTS pages (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        title TEXT,
        url TEXT NOT NULL,
        theme INTEGER NOT NULL,
        num INTEGER NOT NULL DEFAULT 100,
        FOREIGN KEY (theme) REFERENCES sections(_id)
    );
    """
    )
    print("   ✓ Таблица 'pages' создана")

    # Проверяем структуру таблицы
    print("\n3. СТРУКТУРА ТАБЛИЦ PAGES И SECTIONS...")
    cursor.execute("PRAGMA table_info(pages);")
    columns = cursor.fetchall()
    print("   Структура таблицы 'pages':")
    for col in columns:
        print(f"     - {col[1]} ({col[2]})")

    cursor.execute("PRAGMA table_info(sections);")
    columns = cursor.fetchall()
    print("   Структура таблицы 'sections':")
    for col in columns:
        print(f"     - {col[1]} ({col[2]})")

    # Вставляем первую запись (с указанием id)
    print("\n4. ВСТАВКА ДАННЫХ В ТАБЛИЦУ PAGES...")

    try:
        # Первая вставка (с указанием всех полей, включая id)
        print("   Вставка записи 1...")
        cursor.execute(
            """
        INSERT INTO pages (id, title, url, theme, num)
        VALUES (1, 'What is Information', 'information', 1, 1);
        """
        )
        print("   ✓ Запись 1 добавлена")
    except sqlite3.IntegrityError as e:
        print(f"   ✗ Ошибка при вставке записи 1: {e}")

    # Вставляем вторую запись (без указания id - будет использоваться AUTOINCREMENT)
    try:
        print("   Вставка записи 2...")
        cursor.execute(
            """
        INSERT INTO pages (title, url, theme, num)
        VALUES ('Amount of Information', 'amount-information', 1, 2);
        """
        )
        print("   ✓ Запись 2 добавлена")
    except sqlite3.IntegrityError as e:
        print(f"   ✗ Ошибка при вставке записи 2: {e}")

    # Добавим еще несколько записей для демонстрации
    print("\n5. ДОБАВЛЕНИЕ ДОПОЛНИТЕЛЬНЫХ ЗАПИСЕЙ...")
    additional_pages = [
        ("Information Theory", "information-theory", 1, 3),
        ("Binary Coding", "binary-coding", 2, 1),
        ("Probability Basics", "probability-basics", 3, 1),
        ("Entropy Calculation", "entropy-calculation", 1, 4),
    ]

    for page in additional_pages:
        cursor.execute(
            """
        INSERT INTO pages (title, url, theme, num)
        VALUES (?, ?, ?, ?);
        """,
            page,
        )
        print(f"   ✓ Добавлена: {page[0]}")

    # Проверяем целостность внешних ключей
    print("\n6. ПРОВЕРКА ЦЕЛОСТНОСТИ ВНЕШНИХ КЛЮЧЕЙ...")
    cursor.execute("PRAGMA foreign_key_check;")
    fk_check = cursor.fetchall()
    if not fk_check:
        print("   ✓ Целостность внешних ключей не нарушена")
    else:
        print("   ✗ Нарушена целостность внешних ключей:")
        for error in fk_check:
            print(f"     Таблица: {error[0]}, Строка: {error[1]}, FK: {error[2]}")

    # Выполняем запрос SELECT для просмотра всех записей
    print("\n7. ВЫВОД ВСЕХ ЗАПИСЕЙ ИЗ ТАБЛИЦЫ PAGES:")
    print("-" * 60)

    cursor.execute("SELECT * FROM pages;")
    rows = cursor.fetchall()

    # Выводим заголовки
    cursor.execute("PRAGMA table_info(pages);")
    columns = cursor.fetchall()
    column_names = [col[1] for col in columns]
    print(" | ".join(column_names))
    print("-" * 60)

    # Выводим данные
    for row in rows:
        print(" | ".join(str(value) if value is not None else "NULL" for value in row))

    # Более сложный запрос с JOIN для отображения названия раздела
    print("\n8. ЗАПРОС С JOIN ДЛЯ ОТОБРАЖЕНИЯ НАЗВАНИЙ РАЗДЕЛОВ:")
    print("-" * 80)

    cursor.execute(
        """
    SELECT
        pages.id,
        pages.title,
        pages.url,
        sections.name as theme_name,
        pages.num
    FROM pages
    JOIN sections ON pages.theme = sections._id
    ORDER BY pages.theme, pages.num;
    """
    )

    rows = cursor.fetchall()
    # Выводим заголовки
    print("id | title | url | theme_name | num")
    print("-" * 80)
    for row in rows:
        print(f"{row[0]} | {row[1]} | {row[2]} | {row[3]} | {row[4]}")

    # Сохраняем изменения
    conn.commit()

    # Экспортируем данные в CSV и JSON
    print("\n9. ЭКСПОРТ ДАННЫХ...")

    # Создаем папку для экспорта
    os.makedirs("export", exist_ok=True)

    # Экспорт в CSV
    import csv

    with open("export/pages.csv", "w", newline="", encoding="utf-8") as csvfile:
        writer = csv.writer(csvfile)
        # Заголовки
        cursor.execute("SELECT * FROM pages;")
        writer.writerow([description[0] for description in cursor.description])
        # Данные
        writer.writerows(cursor.fetchall())
    print("   ✓ Данные экспортированы в export/pages.csv")

    # Экспорт в JSON
    import json

    cursor.execute("SELECT * FROM pages;")
    rows = cursor.fetchall()
    column_names = [description[0] for description in cursor.description]

    data = []
    for row in rows:
        data.append(dict(zip(column_names, row)))

    with open("export/pages.json", "w", encoding="utf-8") as jsonfile:
        json.dump(data, jsonfile, ensure_ascii=False, indent=2)
    print("   ✓ Данные экспортированы в export/pages.json")

    # Закрываем соединение
    conn.close()

    print("\n" + "=" * 60)
    print("ВЫПОЛНЕНИЕ ЗАВЕРШЕНО!")
    print("=" * 60)

    # Дополнительная информация
    print("\nКОМАНДЫ ДЛЯ РУЧНОЙ РАБОТЫ С БАЗОЙ:")
    print("1. Открыть базу в командной строке:")
    print("   sqlite3 example.db")
    print("\n2. Полезные команды SQLite:")
    print("   .tables                    - показать все таблицы")
    print("   .schema pages              - показать структуру таблицы pages")
    print("   SELECT * FROM pages;       - показать все записи")
    print("   .headers ON               - включить заголовки столбцов")
    print("   .mode column              - красивый формат вывода")
    print("   .quit                     - выйти")


def interactive_sqlite_session():
    """Функция для имитации интерактивной сессии SQLite"""
    print("\n" + "=" * 60)
    print("ИМИТАЦИЯ ИНТЕРАКТИВНОЙ СЕССИИ SQLite")
    print("=" * 60)

    conn = sqlite3.connect("example.db")
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()

    print("sqlite> PRAGMA foreign_keys = ON;")
    cursor.execute("PRAGMA foreign_keys = ON;")
    print("sqlite> .schema pages")

    cursor.execute("SELECT sql FROM sqlite_master WHERE type='table' AND name='pages';")
    schema = cursor.fetchone()
    if schema:
        print(schema[0])

    print(
        "\nsqlite> INSERT INTO pages VALUES (1, 'What is Information', 'information', 1, 1);"
    )
    cursor.execute("DELETE FROM pages WHERE id = 1;")  # Удаляем, если уже есть
    cursor.execute(
        "INSERT INTO pages (id, title, url, theme, num) VALUES (1, 'What is Information', 'information', 1, 1);"
    )

    print(
        "sqlite> INSERT INTO pages (title, url, theme, num) VALUES ('Amount of Information', 'amount-information', 1, 2);"
    )
    cursor.execute(
        "INSERT INTO pages (title, url, theme, num) VALUES ('Amount of Information', 'amount-information', 1, 2);"
    )

    print("\nsqlite> SELECT * FROM pages;")
    cursor.execute("SELECT * FROM pages ORDER BY id;")
    rows = cursor.fetchall()

    # Форматируем вывод как в SQLite
    for row in rows:
        print(f"{row['id']}|{row['title']}|{row['url']}|{row['theme']}|{row['num']}")

    conn.commit()
    conn.close()

    print("\nsqlite>")


if __name__ == "__main__":
    # Создаем и заполняем базу данных
    create_and_populate_database()

    # Запускаем имитацию интерактивной сессии
    interactive_sqlite_session()
