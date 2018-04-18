import datetime
import logging
import time

import pymongo
from apistar import Include, Route, http, Response
from apistar.frameworks.wsgi import WSGIApp as App
from apistar.handlers import docs_urls, static_urls
from pymongo.errors import ConnectionFailure


class MyApiApp(App):
    """Класс АПИ с возможностью доступа к нашей БД"""

    def __init__(self, **kwargs):
        db_layer = DatabaseLayer('mongodb', database_name='pairs')
        self.db_layer = db_layer

        super().__init__(**kwargs)


class DatabaseLayer:
    """Инкапсуляция доступа к БД"""

    def __init__(self, db_connection: str, database_name: str):
        """Инициализировать поллер адресом биржи и интервалом"""
        self.db_connection = db_connection

        # ожидаем подключения к БД - без этого продолжать нет смысла
        self.client = pymongo.MongoClient(db_connection)
        while True:
            try:
                self.client.admin.command('ismaster')
                break
            except ConnectionFailure:
                logging.error('Не получилось подключиться к БД, продолжаем пробовать')
            finally:
                time.sleep(1)

        self.db_pairs = self.client[database_name]


def get_ticker(pair: str = None) -> Response:
    """Получить среднее значение курса валютной пары за прошедшие 10 минут"""
    try:
        # проверяем, есть ли такая пара в БД
        if not pair in app.db_layer.db_pairs.collection_names():
            return Response({
                'message': f'Нет данных для такой пары: {pair}'
            }, status=400)
        now = datetime.datetime.utcnow()
        time_condition = {'$gte': now + datetime.timedelta(minutes=-10), '$lt': now}
        average = app.db_layer.db_pairs[pair].aggregate([
            {"$match": {"time": time_condition}},
            {"$group": {"_id": None, "average_price": {"$avg": "$value"}}}
        ])
        average_num = list(average)[0]['average_price']
        return Response({
            'pair': pair,
            'average': str(average_num),
        }, status=200)
    except Exception as ex:
        return Response({
            'message': f'Ошибка при получении курса для пары {pair}: {ex}'
        }, status=500)


routes = [
    Include('/docs', docs_urls),
    Route('/ticker', 'GET', get_ticker),
    Include('/static', static_urls)
]
app = MyApiApp(routes=routes)

if __name__ == '__main__':
    app.main()
