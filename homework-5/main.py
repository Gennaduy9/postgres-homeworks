import json
import os

import psycopg2

from config import config


def main():
    script_file = 'fill_db.sql'
    json_file = 'suppliers.json'
    db_name = 'my_new_db'

    params = config()
    conn = None

    create_database(params, db_name)
    print(f"БД {db_name} успешно создана")

    params.update({'dbname': db_name})
    try:
        with psycopg2.connect(**params) as conn:
            with conn.cursor() as cur:
                execute_sql_script(cur, script_file)
                print(f"БД {db_name} успешно заполнена")

                create_suppliers_table(cur)
                print("Таблица suppliers успешно создана")

                suppliers = get_suppliers_data(json_file)
                insert_suppliers_data(cur, suppliers)
                print("Данные в suppliers успешно добавлены")

                add_foreign_keys(cur, json_file)
                print(f"FOREIGN KEY успешно добавлены")

    except(Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()


def create_database(params, db_name) -> None:
    """Создает новую базу данных."""
    with psycopg2.connect(
            host=params['host'],
            user=params['user'],
            password=params['password']
    ) as conn:

        conn.autocommit = True

    with conn.cursor() as cur:
        # Проверяем активные подключения к базе данных
        cur.execute(
            f"SELECT pg_terminate_backend(pg_stat_activity.pid) "
            f"FROM pg_stat_activity "
            f"WHERE pg_stat_activity.datname = '{db_name}' "
            f"AND pid <> pg_backend_pid();"
        )

        # Удаляем базу данных
        cur.execute(f"DROP DATABASE IF EXISTS {db_name};")

        # Создаем новую базу данных
        cur.execute(f"CREATE DATABASE {db_name};")


def execute_sql_script(cur, script_file) -> None:
     """Выполняет скрипт из файла для заполнения БД данными."""
     if os.path.exists(script_file):
         with open(script_file, 'r') as file:
             script = file.read()
             cur.execute(script)
     else:
         print(f"Файл {script_file} не существует")


def create_suppliers_table(cur) -> None:
     """Создает таблицу suppliers."""
     cur.execute("""
         CREATE TABLE IF NOT EXISTS suppliers (
             company_name VARCHAR(255) NOT NULL,
             contact VARCHAR(255) NOT NULL,
             address VARCHAR(255) NOT NULL,
             phone VARCHAR(255) NOT NULL,
             fax VARCHAR(255) NOT NULL,
             homepage VARCHAR(255) NOT NULL,
             products VARCHAR(255) NOT NULL
         );
         """)

def get_suppliers_data(json_file: str) -> list[dict]:
     """Извлекает данные о поставщиках из JSON-файла и возвращает список словарей с соответствующей информацией."""
     with open(json_file, 'r') as file:
         data = json.load(file)
     return data


def insert_suppliers_data(cur, suppliers: list[dict]) -> None:
     """Добавляет данные из suppliers в таблицу suppliers."""

     for supplier in suppliers:
         cur.execute("""
             INSERT INTO suppliers (company_name, contact, address, phone, fax, homepage, products)
             VALUES (%s, %s, %s, %s, %s, %s, %s)
             """, (
             supplier['company_name'],
             supplier['contact'],
             supplier['address'],
             supplier['phone'],
             supplier['fax'],
             supplier['homepage'],
             supplier['products']
         ))

def add_foreign_keys(cur, json_file) -> None:
     """Добавляет foreign key со ссылкой на supplier_id в таблицу products."""
     cur.execute("""
             ALTER TABLE products
             ADD COLUMN IF NOT EXISTS supplier_id INTEGER
         """)

     cur.execute("""
             ALTER TABLE products
             ADD CONSTRAINT fk_supplier
             FOREIGN KEY (supplier_id)
             REFERENCES suppliers (id)
         """)


if __name__ == '__main__':
    main()
