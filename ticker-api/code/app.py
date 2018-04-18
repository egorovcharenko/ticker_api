import logging
import time

import pymongo
from apistar import Include, Route
from apistar.components import schema
from apistar.frameworks.wsgi import WSGIApp as App
from apistar.handlers import docs_urls, static_urls
from pymongo.errors import ConnectionFailure


class MyApiApp(App):
    """Класс АПИ с возможностью доступа к нашей БД"""

    def __init__(self, db_layer, **kwargs):
        self.db_layer = db_layer
        super().__init__(**kwargs)


class DatabaseLayer:
    """Инкапсуляция доступа к БД"""

    def __init__(self, db_connection: str):
        """Инициализировать поллер адресом биржи и интервалом"""
        self.db_connection = db_connection

        # ожидаем подключения к БД - без этого продолжать нет смысла
        self.client = pymongo.MongoClient(db_connection)
        return
        while True:
            try:
                self.client.admin.command('ismaster')
                break
            except ConnectionFailure:
                logging.error('Не получилось подключиться к БД, продолжаем пробовать')
            finally:
                time.sleep(1)

        self.db_pairs = self.client.pairs


def get_ticker(pair: str = None):
    """Получить средне значение курса валютной пары за прошедшие 10 минут"""
    return {'message': pair}


db_layer = DatabaseLayer('mongodb')
routes = [
    Include('/docs', docs_urls),
    Route('/ticker', 'GET', get_ticker),
    Include('/static', static_urls)
]
app = MyApiApp(db_layer=db_layer, routes=routes)

if __name__ == '__main__':
    app.main()
