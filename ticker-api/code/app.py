import datetime
import logging
import time

import pymongo
from aiohttp import web
from pymongo.errors import ConnectionFailure


class DatabaseLayer:
    """Инкапсуляция доступа к БД"""

    def __init__(self, db_connection: str, database_name: str):
        """Инициализировать поллер адресом биржи и интервалом"""
        self.db_connection = db_connection

        self.cache = {}
        self.all_pairs_cached = {}
        self.all_pairs_cached_time = None

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


async def get_ticker(request):
    """Получить среднее значение курса валютной пары за прошедшие 10 минут"""
    try:
        pair = request.match_info.get('pair', "")
        now = datetime.datetime.utcnow()
        if (not db_layer.all_pairs_cached_time) or (db_layer.all_pairs_cached_time +
                                                    datetime.timedelta(minutes=10) < now):
            # обновить список пар в кеше
            db_layer.all_pairs_cached = [pair for pair in db_layer.db_pairs.collection_names()]
            db_layer.all_pairs_cached_time = now

        # Проверяем что такая пара есть
        if pair not in db_layer.all_pairs_cached:
            return web.json_response({
                'message': f'Нет данных для такой пары: {pair}'
            }, status=400)

        # сначала ищем в кеше и проверяем кто кеш свежий
        if pair not in db_layer.cache:
            cache_pair(now, pair)

        cached_time, cached_price = db_layer.cache[pair]
        if cached_time + datetime.timedelta(seconds=60) < now:
            # кеш устарел - кешируем заново
            cache_pair(now, pair)
        # возвращем из кеша всегда
        return web.json_response({
            'average': str(cached_price),
        })
    except Exception as ex:
        return web.json_response({
            'message': f'Ошибка при получении курса для пары {pair}: {ex}'
        }, status=500)


def cache_pair(now, pair):
    # получаем цену, кешим
    time_condition = {'$gte': now - datetime.timedelta(minutes=10), '$lt': now}
    average = db_layer.db_pairs[pair].aggregate([
        {"$match": {"time": time_condition}},
        {"$group": {"_id": None, "average_price": {"$avg": "$value"}}}
    ])
    average_price = list(average)[0]['average_price']
    db_layer.cache[pair] = (now, average_price)


app = web.Application()
app.add_routes([
    web.get('/ticker/{pair}', get_ticker),
])
db_layer = DatabaseLayer('mongodb', database_name='pairs')

if __name__ == '__main__':
    web.run_app(app)
