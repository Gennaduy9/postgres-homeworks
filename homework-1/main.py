"""Скрипт для заполнения данными таблиц в БД Postgres."""

from config import host, user, password, db_name
from pathlib import Path, PureWindowsPath

import psycopg2
import csv


def connect_to(csv_file, table_name):
    try:
        # Подключается к существующей базе данных north
        with psycopg2.connect(
                host=host,
                user=user,
                password=password,
                database=db_name
        ) as connection:

            connection.autocommit = True

        # Курсор для выполнения операций с базой данных
        with connection.cursor() as cursor:
            cursor.execute(
                "SELECT version();"
            )

            print(f"Server version: {cursor.fetchall()}")

        # Выполняет запроса SQL к базе данных
        with connection.cursor() as cursor:
            cursor.execute(
                    f"SELECT * FROM {table_name}"
                )
            rows = cursor.fetchall()

            print("[INFO] Запрос выполнен успешно")

            # Создаем объект Path
            # filename = Path(f"C:\\Users\\Геннадий Михайлович\\PycharmProjects\\"
            #                 f"postgres-homeworks\\homework-1\\north_data\\{csv_file}")

            # Получаем абсолютный путь к файлу
            # abs_path = filename.resolve()
            # with open(abs_path, "r", encoding="windows-1251") as file:

            with open(f"north_data/{csv_file}", "r") as file:
                reader = csv.reader(file)
                next(reader) # Пропускаем заголовки столбцов

                for row in reader:
                    insert_query = f"INSERT INTO {table_name} VALUES ({', '.join(['%s'] * len(row))})"
                    cursor.execute(insert_query, row)

    except Exception as _ex:
        print("[INFO] Ошибка при работе PostgreSQL", _ex)
    finally:
        if connection:
            connection.close()
            print("[INFO] PostgreSQL соединение закрыто")


connect_to('customers_data.csv', 'customers')
connect_to('employees_data.csv', 'employees')
connect_to('orders_data.csv', 'orders')