import clickhouse_connect


class ClickHouseClient:
    def __init__(self):
        self._client = None
        try:
            self._client = clickhouse_connect.get_client(
                host="host.docker.internal", port=8123, username='admin', password='admin')
            print("Соединение с клиентом ClickHouse установлено")
        except Exception as e:
            print("Ошибка при соединении с клиентом ClickHouse", e)

    def __enter__(self):
        return self._client

    def __exit__(self, exc_type, exc_val, exc_tb):
        print("Соединение с клиентом ClickHouse закрыто")
