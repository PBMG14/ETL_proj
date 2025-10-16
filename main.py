import pandas as pd
import config as config
from datetime import datetime
import SQL_Requests.postgresql_query as query
import SQL_Requests.clickhouse_query as ch_query
from DBMS_Classes.PostgreSQLDatabase import PostgreSQLDatabase
from DBMS_Classes.ClickHouseClient import ClickHouseClient

def create_data(cur):
    def create_temp_table(cur):
        cur.execute(query.create_query)

    def create_all_tables(cur):
        cur.execute(query.create_query2)

    create_temp_table(cur)
    create_all_tables(cur)


def insert_data(cur, lst_of_tuples):
    def insert_into_temp_table(cur, lst_of_tuples):
        cur.executemany(query.insert_query1, lst_of_tuples)

    def insert_into_all_tables(cur):
        cur.execute(query.insert_into_tmp_table)

    insert_into_temp_table(cur, lst_of_tuples)
    insert_into_all_tables(cur)


def prepare_data(df):
    my_list = []
    for _, row in df.iterrows():
        row = row.where(pd.notnull(row), None)
        my_list.append(tuple(row))
    return my_list


def init_postgreSQLDatabase(cur):
    try:
        create_data(cur)

        cur.execute("SELECT id FROM temp_data LIMIT 1")
        assert not bool(cur.fetchall())

        df = pd.read_excel(config.file_path)
        data = prepare_data(df)
        insert_data(cur, data)
        print("Данные успешно загружены")
    except Exception as e:
        print(f"Ошибка при выполнении команд: {e}")


def connect_to_clickhouse(client_cur, data):
    try:
        client_cur.command("DROP TABLE IF EXISTS purchases")
        client_cur.command("DROP TABLE IF EXISTS date_purchases")
        client_cur.command("DROP TABLE IF EXISTS date_purchases_by_gender")

        client_cur.command(ch_query.create_purchases)
        client_cur.command(ch_query.create_date_purchases)
        client_cur.command(ch_query.create_date_purchases_by_gender)

        if data:
            columns = ['clientcode', 'gender', 'price', 'amount', 'timestamp']
            rows = []

            for row in data:
                count = 0
                try:
                    clientcode = int(row[0])
                    gender = str(row[1])
                    price = float(row[2])
                    amount = float(row[3])
                    timestamp = row[4] if isinstance(
                        row[4], datetime) else datetime.now()

                    rows.append([clientcode, gender, price, amount, timestamp])
                except (ValueError, TypeError, IndexError) as e:
                    print(
                        f"Пропуск некорректной строки: {row}. Ошибка: {str(e)}")
                    continue
            if rows:
                client_cur.insert("purchases", rows, column_names=columns)

        client_cur.command("""
        INSERT INTO date_purchases (day, date_amount, date_price, average_price)
        SELECT 
            toStartOfDay(timestamp) AS day, 
            SUM(amount) as date_amount, 
            SUM(price) AS date_price, 
            date_price / date_amount 
        FROM purchases 
        GROUP BY day
        """)

        client_cur.command("""
        INSERT INTO date_purchases_by_gender (day, gender, date_amount, date_price, average_price)
        SELECT 
            toStartOfDay(timestamp) AS day, 
            gender, 
            SUM(amount) as date_amount, 
            SUM(price) AS date_price, 
            date_price / date_amount 
        FROM purchases 
        GROUP BY day, gender
        """)

    except Exception as e:
        print(f"Ошибка при работе с ClickHouse: {str(e)}")
        raise


if __name__ == "__main__":
    postgre_db = PostgreSQLDatabase()
    ch_client = ClickHouseClient()
    with postgre_db as cur:
        # init_postgreSQLDatabase(cur)
        data = cur.execute(
            """SELECT CLIENTCODE, GENDER, PRICE, AMOUNT, DATE_ FROM temp_data""").fetchall()
    with ch_client as client_cur:
        connect_to_clickhouse(client_cur, data)
