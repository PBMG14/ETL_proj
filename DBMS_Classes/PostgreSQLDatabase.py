from psycopg import connect, DatabaseError
from psycopg.errors import ConnectionTimeout
from config import *


class PostgreSQLDatabase:
    def __init__(self):
        conn_string = f"postgresql://{user}:{password}@{host}:{port}/{dbname}"
        self._connection = None
        try:
            self._connection = connect(conninfo=conn_string, autocommit=False)
            if self._connection:
                print("Соединение PostgreSQL создано успешно")
        except ConnectionTimeout as error:
            print("Превышено время соединения PostgreSQL", error)
        except (Exception, DatabaseError) as error:
            print("Ошибка при подключении PostgreSQL", error)

    def __enter__(self):
        cursor = None
        try:
            self._cursor = self._connection.cursor()
            if self._cursor:
                print("Соединение установлено")
        except (Exception, DatabaseError) as error:
            print("Ошибка при соединении PostgreSQL", error)
        return self._cursor

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self._connection:
            self._connection.commit()
            self._connection.close()
            print("Соединение PostgreSQL закрыто")
