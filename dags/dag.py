from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.models import Variable
import pandas as pd
from airflow.decorators import task
import logging
from airflow.exceptions import AirflowException

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'retry_exponential_backoff': True,
}

with DAG(
    'etl_retail_data_v3_improved',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False,
    template_searchpath=['/opt/airflow/SQL_Requests'],
    tags=['retail', 'etl'],
    max_active_tasks=3,
    doc_md=__doc__,
) as dag:

    @task(task_id="prepare_excel_data")
    def prepare_data():
        """
        Подготовка данных из Excel файла.
        
        Возвращает:
            list: Список кортежей с очищенными данными
        
        Исключения:
            AirflowException: Если файл не может быть прочитан или данные невалидны
        """
        try:
            file_path = Variable.get("EXCEL_FILE_PATH")
            
            # Чтение Excel с обработкой ошибок
            try:
                df = pd.read_excel(file_path)
            except Exception as e:
                raise AirflowException(f"Ошибка чтения Excel файла: {str(e)}")
            
            # Проверка обязательных колонок
            required_columns = ['CLIENTCODE', 'GENDER', 'PRICE', 'AMOUNT', 'DATE_']
            if not all(col in df.columns for col in required_columns):
                missing = set(required_columns) - set(df.columns)
                raise AirflowException(f"В файле отсутствуют обязательные колонки: {missing}")
            
            # Обработка данных
            valid_rows = []
            for _, row in df.iterrows():
                try:
                    row = row.where(pd.notnull(row), None)
                    # Валидация типов данных
                    row['CLIENTCODE'] = int(row['CLIENTCODE']) if pd.notnull(row['CLIENTCODE']) else None
                    row['PRICE'] = float(row['PRICE']) if pd.notnull(row['PRICE']) else None
                    row['AMOUNT'] = float(row['AMOUNT']) if pd.notnull(row['AMOUNT']) else None
                    valid_rows.append(tuple(row))
                except (ValueError, TypeError) as e:
                    logging.warning(f"Пропуск строки с ошибкой: {row}. Ошибка: {str(e)}")
                    continue
            
            if not valid_rows:
                raise AirflowException("Не найдено валидных данных в Excel файле")
                
            return valid_rows
            
        except Exception as e:
            raise AirflowException(f"Ошибка подготовки данных: {str(e)}")

    # Создание временной таблицы в PostgreSQL
    create_temp_table = SQLExecuteQueryOperator(
        task_id='create_temp_table',
        sql='postgresql_query/create_query.sql',
        postgres_conn_id='postgres_default',
    )

    prepared_data = prepare_data()

    # Вставка данных во временную таблицу
    insert_to_temp_table = SQLExecuteQueryOperator(
        task_id='insert_to_temp_table',
        sql='postgresql_query/insert_query1.sql',
        postgres_conn_id='postgres_default',
        parameters=prepared_data,
    )

    # Создание нормализованных таблиц
    create_normalized_tables = SQLExecuteQueryOperator(
        task_id='create_normalized_tables',
        sql='postgresql_query/create_query2.sql',
        postgres_conn_id='postgres_default',
    )

    # Заполнение нормализованных таблиц
    populate_normalized_tables = SQLExecuteQueryOperator(
        task_id='populate_normalized_tables',
        sql='postgresql_query/insert_into_tmp_table.sql',
        postgres_conn_id='postgres_default',
    )

    @task(task_id="transfer_to_clickhouse")
    def transfer_to_clickhouse():
        """
        Перенос данных в ClickHouse с созданием схемы.
        
        Исключения:
            AirflowException: Если перенос данных не удался
        """
        try:
            from DBMS_Classes.PostgreSQLDatabase import PostgreSQLDatabase
            from DBMS_Classes.ClickHouseClient import ClickHouseClient
            
            postgre_db = PostgreSQLDatabase()
            ch_client = ClickHouseClient()
            
            # Получение данных из PostgreSQL
            with postgre_db as cur:
                data = cur.execute(
                    """SELECT CLIENTCODE, GENDER, PRICE, AMOUNT, DATE_ FROM temp_data""").fetchall()
            
            # Работа с ClickHouse
            with ch_client as client_cur:
                # Удаление старых таблиц (если нужно)
                client_cur.command("DROP TABLE IF EXISTS purchases, date_purchases, date_purchases_by_gender")
                
                # Создание таблиц
                with open('/opt/airflow/SQL_Requests/clickhouse_query/create_purchases.sql') as f:
                    client_cur.command(f.read())
                with open('/opt/airflow/SQL_Requests/clickhouse_query/create_date_purchases.sql') as f:
                    client_cur.command(f.read())
                with open('/opt/airflow/SQL_Requests/clickhouse_query/create_date_purchases_by_gender.sql') as f:
                    client_cur.command(f.read())

                # Вставка данных пачками
                if data:
                    columns = ['clientcode', 'gender', 'price', 'amount', 'timestamp']
                    rows = []
                    
                    for row in data:
                        try:
                            clientcode = int(row[0])
                            gender = str(row[1])
                            price = float(row[2])
                            amount = float(row[3])
                            timestamp = row[4] if isinstance(row[4], datetime) else datetime.now()
                            rows.append([clientcode, gender, price, amount, timestamp])
                        except (ValueError, TypeError, IndexError) as e:
                            logging.warning(f"Пропуск некорректной строки: {row}. Ошибка: {str(e)}")
                            continue
                    
                    if rows:
                        batch_size = int(Variable.get("CLICKHOUSE_BATCH_SIZE", default_var=1000))
                        for i in range(0, len(rows), batch_size):
                            client_cur.insert("purchases", rows[i:i+batch_size], column_names=columns)

                # Заполнение агрегирующих таблиц
                with open('/opt/airflow/SQL_Requests/clickhouse_query/insert_data_to_date_purchases.sql') as f:
                    client_cur.command(f.read())
                with open('/opt/airflow/SQL_Requests/clickhouse_query/insert_data_to_date_purchases_by_gender.sql') as f:
                    client_cur.command(f.read())
                    
        except Exception as e:
            raise AirflowException(f"Ошибка переноса в ClickHouse: {str(e)}")

    # Определение зависимостей
    create_temp_table >> prepared_data >> insert_to_temp_table
    insert_to_temp_table >> create_normalized_tables >> populate_normalized_tables
    populate_normalized_tables >> transfer_to_clickhouse()