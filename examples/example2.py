import os
import sqlite3


def create_and_modify_database():
    print("=" * 60)
    print("ПОВТОРЕНИЕ ПРИМЕРА С SQLITE: SELECT И DELETE")
    print("=" * 60)

    # Удаляем старую базу данных, если существует
    db_name = "pages_example.db"
    if os.path.exists(db_name):
        os.remove(db_name)
        print(f"✓ Удалена старая база данных {db_name}")

    # Создаем соединение с базой данных
    conn = sqlite3.connect(db_name)
    cursor = conn.cursor()

    # Создаем таблицу pages (упрощенная версия без внешних ключей)
    print("\n1. СОЗДАНИЕ ТАБЛИЦЫ PAGES...")
    cursor.execute(
        """
    CREATE TABLE pages (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        title TEXT,
        theme INTEGER NOT NULL,
        num INTEGER NOT NULL
    );
    """
    )
    print("   ✓ Таблица 'pages' создана")

    # Вставляем данные как в примере
    print("\n2. ВСТАВКА ДАННЫХ...")
    pages_data = [
        (1, "What is Information", 1, 1),
        (3, "Amount of Information", 1, 1),
        (4, "Binary System", 2, 1),
        (5, "Octal System", 2, 1),
        (6, "Lows of Logic Algebra", 3, 1),
    ]

    cursor.executemany(
        """
    INSERT INTO pages (id, title, theme, num) VALUES (?, ?, ?, ?)
    """,
        pages_data,
    )
    print(f"   ✓ Вставлено {len(pages_data)} записей")

    # Выводим начальное состояние таблицы
    print("\n3. НАЧАЛЬНОЕ СОСТОЯНИЕ ТАБЛИЦЫ:")
    print("sqlite> SELECT id, title, theme, num FROM pages;")

    cursor.execute("SELECT id, title, theme, num FROM pages ORDER BY id;")
    rows = cursor.fetchall()

    # Форматируем вывод точно как в примере
    for row in rows:
        # В примере theme и num выводятся слитно: "11" вместо "1 | 1"
        print(f"{row[0]} | {row[1]}|{row[2]}{row[3]}")

    # Выполняем DELETE операции
    print("\n4. ВЫПОЛНЕНИЕ DELETE ОПЕРАЦИЙ...")

    # Удаляем запись с id = 6
    print("sqlite> DELETE FROM pages WHERE id = 6;")
    cursor.execute("DELETE FROM pages WHERE id = 6;")
    print(f"   ✓ Удалена 1 запись с id=6")

    # Удаляем записи с theme = 2
    print("sqlite> DELETE FROM pages WHERE theme = 2;")
    cursor.execute("DELETE FROM pages WHERE theme = 2;")
    print(f"   ✓ Удалено 2 записи с theme=2")

    # Выводим финальное состояние таблицы
    print("\n5. ФИНАЛЬНОЕ СОСТОЯНИЕ ТАБЛИЦЫ:")
    print("sqlite> SELECT id, title, theme, num FROM pages;")

    cursor.execute("SELECT id, title, theme, num FROM pages ORDER BY id;")
    rows = cursor.fetchall()

    for row in rows:
        print(f"{row[0]} | {row[1]}|{row[2]}{row[3]}")

    print("sqlite> []")

    # Сохраняем изменения
    conn.commit()

    # Дополнительная информация
    print("\n" + "=" * 60)
    print("ДОПОЛНИТЕЛЬНАЯ ИНФОРМАЦИЯ:")
    print("=" * 60)

    # Статистика по таблице
    cursor.execute("SELECT COUNT(*) as total FROM pages;")
    total = cursor.fetchone()[0]
    print(f"Общее количество записей: {total}")

    cursor.execute("SELECT id, title, theme, num FROM pages;")
    rows = cursor.fetchall()
    print("\nДетальный список оставшихся записей:")
    print("id | title | theme | num")
    print("-" * 40)
    for row in rows:
        print(f"{row[0]:2} | {row[1]:25} | {row[2]:5} | {row[3]:3}")

    # Экспорт данных
    print("\n6. ЭКСПОРТ ДАННЫХ...")

    # Создаем папку для экспорта
    os.makedirs("export_pages", exist_ok=True)

    # Экспорт в CSV
    import csv

    csv_file = "export_pages/pages_after_deletes.csv"
    with open(csv_file, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["id", "title", "theme", "num"])
        writer.writerows(rows)
    print(f"✓ Данные экспортированы в {csv_file}")

    # Экспорт в JSON
    import json

    json_file = "export_pages/pages_after_deletes.json"
    data = []
    for row in rows:
        data.append({"id": row[0], "title": row[1], "theme": row[2], "num": row[3]})

    with open(json_file, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    print(f"✓ Данные экспортированы в {json_file}")

    # Закрываем соединение
    conn.close()

    print("\n" + "=" * 60)
    print("ВЫПОЛНЕНИЕ ЗАВЕРШЕНО!")
    print("=" * 60)
    print(f"\nБаза данных сохранена как: {db_name}")
    print("Для проверки выполните команды:")
    print(f"  sqlite3 {db_name}")
    print("  SELECT id, title, theme, num FROM pages;")


def interactive_mode():
    """Интерактивный режим для повторения примера вручную"""
    print("\n" + "=" * 60)
    print("ИНТЕРАКТИВНЫЙ РЕЖИМ SQLITE")
    print("=" * 60)

    db_name = "pages_example.db"

    if not os.path.exists(db_name):
        print(f"База данных {db_name} не найдена.")
        print("Сначала выполните основной скрипт.")
        return

    conn = sqlite3.connect(db_name)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()

    print("\nИмитация сессии SQLite:")
    print("-" * 60)

    commands = [
        "SELECT id, title, theme, num FROM pages;",
        "DELETE FROM pages WHERE id = 6;",
        "DELETE FROM pages WHERE theme = 2;",
        "SELECT id, title, theme, num FROM pages;",
    ]

    for cmd in commands:
        print(f"sqlite> {cmd}")

        if cmd.upper().startswith("SELECT"):
            cursor.execute(cmd)
            rows = cursor.fetchall()

            # Форматируем вывод как в примере
            for row in rows:
                print(f"{row['id']} | {row['title']}|{row['theme']}{row['num']}")
        else:
            cursor.execute(cmd)
            conn.commit()
            print("(Команда выполнена)")

    print("sqlite> []")

    conn.close()

    print("\nДля запуска реальной сессии SQLite выполните:")
    print(f"  sqlite3 {db_name}")


def additional_queries():
    """Дополнительные запросы для понимания работы"""
    print("\n" + "=" * 60)
    print("ДОПОЛНИТЕЛЬНЫЕ ЗАПРОСЫ ДЛЯ ОБУЧЕНИЯ")
    print("=" * 60)

    db_name = "pages_example_original.db"

    # Создаем копию базы с исходными данными
    if os.path.exists("pages_example.db"):
        import shutil

        shutil.copy2("pages_example.db", db_name)

        conn = sqlite3.connect(db_name)
        cursor = conn.cursor()

        # Восстанавливаем исходные данные
        cursor.execute("DELETE FROM pages;")
        pages_data = [
            (1, "What is Information", 1, 1),
            (3, "Amount of Information", 1, 1),
            (4, "Binary System", 2, 1),
            (5, "Octal System", 2, 1),
            (6, "Lows of Logic Algebra", 3, 1),
        ]
        cursor.executemany(
            """
        INSERT INTO pages (id, title, theme, num) VALUES (?, ?, ?, ?)
        """,
            pages_data,
        )
        conn.commit()

        print("\n1. Разные способы SELECT:")
        print("-" * 40)

        # Все записи
        print("а) Все записи:")
        cursor.execute("SELECT * FROM pages;")
        for row in cursor.fetchall():
            print(f"   {row}")

        # Только определенные столбцы
        print("\nб) Только заголовки:")
        cursor.execute("SELECT title FROM pages;")
        for row in cursor.fetchall():
            print(f"   {row[0]}")

        # Сортировка
        print("\nв) Сортировка по theme:")
        cursor.execute("SELECT * FROM pages ORDER BY theme;")
        for row in cursor.fetchall():
            print(f"   {row}")

        print("\n2. Разные способы DELETE:")
        print("-" * 40)

        # Удаление по условию
        print("а) Удалить все записи с num=1:")
        cursor.execute("DELETE FROM pages WHERE num = 1;")
        print(f"   Удалено записей: {cursor.rowcount}")

        # Восстанавливаем для следующего примера
        cursor.execute("DELETE FROM pages;")
        cursor.executemany(
            """
        INSERT INTO pages (id, title, theme, num) VALUES (?, ?, ?, ?)
        """,
            pages_data,
        )

        print("\nб) Удалить записи с theme=1 или theme=2:")
        cursor.execute("DELETE FROM pages WHERE theme IN (1, 2);")
        print(f"   Удалено записей: {cursor.rowcount}")

        # Восстанавливаем для следующего примера
        cursor.execute("DELETE FROM pages;")
        cursor.executemany(
            """
        INSERT INTO pages (id, title, theme, num) VALUES (?, ?, ?, ?)
        """,
            pages_data,
        )

        print("\nв) Удалить все записи (очистить таблицу):")
        cursor.execute("DELETE FROM pages;")
        print(f"   Удалено всех записей")

        conn.commit()
        conn.close()

        os.remove(db_name)

        print("\n✓ Дополнительные примеры выполнены")


if __name__ == "__main__":
    # Основной пример
    create_and_modify_database()

    # Интерактивный режим
    interactive_mode()

    # Дополнительные запросы
    additional_queries()
