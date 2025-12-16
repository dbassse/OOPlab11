import json
import os
import shutil
import sqlite3
import sys
import tempfile

import pandas as pd
import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))
from zad import main


@pytest.fixture
def setup_test_environment():
    """Фикстура для настройки тестовой среды"""
    # Создаем временную директорию
    temp_dir = tempfile.mkdtemp()
    original_dir = os.getcwd()
    os.chdir(temp_dir)

    yield temp_dir

    # Возвращаемся в исходную директорию и очищаем временную
    os.chdir(original_dir)
    shutil.rmtree(temp_dir)


@pytest.fixture
def create_test_csv_files():
    """Создает тестовые CSV файлы"""
    # Создаем тестовый train.csv
    train_data = {
        "PassengerId": [1, 2, 3, 4, 5],
        "Survived": [0, 1, 1, 0, 1],
        "Pclass": [3, 1, 3, 1, 2],
        "Name": [
            "Braund, Mr. Owen Harris",
            "Cumings, Mrs. John Bradley",
            "Heikkinen, Miss. Laina",
            "Futrelle, Mrs. Jacques Heath",
            "Allen, Mr. William Henry",
        ],
        "Sex": ["male", "female", "female", "female", "male"],
        "Age": [22.0, 38.0, 26.0, 35.0, 35.0],
        "SibSp": [1, 1, 0, 1, 0],
        "Parch": [0, 0, 0, 0, 0],
        "Ticket": ["A/5 21171", "PC 17599", "STON/O2. 3101282", "113803", "373450"],
        "Fare": [7.25, 71.28, 7.92, 53.1, 8.05],
        "Cabin": [None, "C85", None, "C123", None],
        "Embarked": ["S", "C", "S", "S", "S"],
    }
    train_df = pd.DataFrame(train_data)
    train_df.to_csv("train.csv", index=False)

    # Создаем тестовый test.csv
    test_data = {
        "PassengerId": [6, 7, 8],
        "Pclass": [3, 1, 3],
        "Name": [
            "Moran, Mr. James",
            "McCarthy, Mr. Timothy J",
            "Palsson, Master. Gosta Leonard",
        ],
        "Sex": ["male", "male", "male"],
        "Age": [None, 54.0, 2.0],
        "SibSp": [0, 0, 3],
        "Parch": [0, 0, 1],
        "Ticket": ["330877", "17463", "349909"],
        "Fare": [8.4583, 51.8625, 21.075],
        "Cabin": [None, "E46", None],
        "Embarked": ["Q", "S", "S"],
    }
    test_df = pd.DataFrame(test_data)
    test_df.to_csv("test.csv", index=False)

    # Создаем тестовый gender_submission.csv
    submission_data = {"PassengerId": [6, 7, 8], "Survived": [0, 0, 1]}
    submission_df = pd.DataFrame(submission_data)
    submission_df.to_csv("gender_submission.csv", index=False)

    return train_df, test_df, submission_df


def test_missing_files(setup_test_environment, capsys):
    """Тест на отсутствие файлов датасета"""
    # Не создаем файлы - они должны отсутствовать
    main()

    captured = capsys.readouterr()
    assert "Отсутствуют файлы датасета" in captured.out
    assert "train.csv" in captured.out
    assert "test.csv" in captured.out
    assert "gender_submission.csv" in captured.out


def test_successful_execution(setup_test_environment, create_test_csv_files, capsys):
    """Тест успешного выполнения программы"""
    main()

    captured = capsys.readouterr()

    # Проверяем вывод
    assert "РАБОТА С ДАТАСЕТОМ TITANIC" in captured.out
    assert "Файлы датасета найдены" in captured.out
    assert "ЗАГРУЗКА ДАННЫХ В БАЗУ" in captured.out
    assert "ВЫПОЛНЕНИЕ SQL-ЗАПРОСОВ" in captured.out
    assert "ЭКСПОРТ РЕЗУЛЬТАТОВ" in captured.out
    assert "СВОДНЫЙ ОТЧЕТ" in captured.out
    assert "ВЫПОЛНЕНИЕ ЗАВЕРШЕНО" in captured.out

    # Проверяем создание файла базы данных
    assert os.path.exists("titanic_database.db")

    # Проверяем создание папок с результатами
    assert os.path.exists("csv_results")
    assert os.path.exists("json_results")

    # Проверяем создание CSV файлов с результатами запросов
    csv_files = os.listdir("csv_results")
    assert len(csv_files) == 10  # 10 запросов
    assert all(f.startswith("query_") and f.endswith(".csv") for f in csv_files)

    # Проверяем создание JSON файлов с результатами запросов
    json_files = os.listdir("json_results")
    assert len(json_files) == 10  # 10 запросов
    assert all(f.startswith("query_") and f.endswith(".json") for f in json_files)


def test_database_tables(setup_test_environment, create_test_csv_files):
    """Тест создания таблиц в базе данных"""
    main()

    # Подключаемся к созданной базе данных
    conn = sqlite3.connect("titanic_database.db")
    cursor = conn.cursor()

    # Проверяем существование таблиц
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
    tables = [row[0] for row in cursor.fetchall()]

    expected_tables = [
        "passengers_train",
        "passengers_test",
        "submission_template",
        "all_passengers",
    ]

    for table in expected_tables:
        assert table in tables, f"Таблица {table} должна существовать"

    # Проверяем количество записей в таблицах
    cursor.execute("SELECT COUNT(*) FROM passengers_train")
    train_count = cursor.fetchone()[0]
    assert train_count == 5

    cursor.execute("SELECT COUNT(*) FROM passengers_test")
    test_count = cursor.fetchone()[0]
    assert test_count == 3

    cursor.execute("SELECT COUNT(*) FROM all_passengers")
    all_count = cursor.fetchone()[0]
    assert all_count == 8  # 5 + 3

    conn.close()


def test_csv_export_content(setup_test_environment, create_test_csv_files):
    """Тест содержимого экспортированных CSV файлов"""
    main()

    # Проверяем первый запрос
    query1_path = "csv_results/query_01.csv"
    assert os.path.exists(query1_path)

    df = pd.read_csv(query1_path)
    assert "PassengerId" in df.columns
    assert "Name" in df.columns
    assert "Sex" in df.columns
    assert "Age" in df.columns
    assert "Survived" in df.columns
    assert len(df) <= 10  # LIMIT 10 в запросе


def test_json_export_content(setup_test_environment, create_test_csv_files):
    """Тест содержимого экспортированных JSON файлов"""
    main()

    # Проверяем первый запрос
    query1_path = "json_results/query_01.json"
    assert os.path.exists(query1_path)

    with open(query1_path, "r", encoding="utf-8") as f:
        data = json.load(f)

    assert isinstance(data, list)
    if len(data) > 0:
        assert "PassengerId" in data[0]
        assert "Name" in data[0]
        assert "Sex" in data[0]
        assert "Age" in data[0]
        assert "Survived" in data[0]


def test_sql_queries_execution(setup_test_environment, create_test_csv_files):
    """Тест выполнения SQL запросов"""
    main()

    # Подключаемся к базе данных
    conn = sqlite3.connect("titanic_database.db")

    # Тестируем некоторые запросы
    queries_to_test = [
        # Запрос 2: Общая статистика по выживанию
        """
        SELECT
            COUNT(*) as total_passengers,
            SUM(Survived) as survived,
            ROUND(AVG(Survived) * 100, 2) as survival_rate_percent
        FROM passengers_train;
        """,
        # Запрос 3: Выживаемость по полу
        """
        SELECT
            Sex,
            COUNT(*) as total,
            SUM(Survived) as survived,
            ROUND(AVG(Survived) * 100, 2) as survival_rate_percent
        FROM passengers_train
        GROUP BY Sex;
        """,
        # Запрос 5: Статистика по возрасту
        """
        SELECT
            COUNT(*) as total,
            ROUND(AVG(Age), 2) as avg_age,
            MIN(Age) as min_age,
            MAX(Age) as max_age
        FROM all_passengers
        WHERE Age IS NOT NULL;
        """,
    ]

    for sql in queries_to_test:
        df = pd.read_sql_query(sql, conn)
        assert not df.empty, f"Запрос должен возвращать данные:\n{sql}"

    conn.close()


def test_folders_creation(setup_test_environment, create_test_csv_files):
    """Тест создания папок для результатов"""
    # Удаляем папки, если они существуют
    for folder in ["csv_results", "json_results"]:
        if os.path.exists(folder):
            shutil.rmtree(folder)

    main()

    assert os.path.exists("csv_results")
    assert os.path.exists("json_results")
    assert os.path.isdir("csv_results")
    assert os.path.isdir("json_results")


def test_data_integrity(setup_test_environment, create_test_csv_files):
    """Тест целостности данных при загрузке в БД"""
    train_df, test_df, submission_df = create_test_csv_files
    main()

    # Подключаемся к базе данных
    conn = sqlite3.connect("titanic_database.db")

    # Проверяем, что данные корректно загрузились
    db_train = pd.read_sql_query("SELECT * FROM passengers_train", conn)
    db_test = pd.read_sql_query("SELECT * FROM passengers_test", conn)

    # Проверяем количество строк
    assert len(db_train) == len(train_df)
    assert len(db_test) == len(test_df)

    # Проверяем ключевые колонки
    assert "PassengerId" in db_train.columns
    assert "Name" in db_train.columns
    assert "Sex" in db_train.columns
    assert "Age" in db_train.columns
    assert "Survived" in db_train.columns

    conn.close()


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
