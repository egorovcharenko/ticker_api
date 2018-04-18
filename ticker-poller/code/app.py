import asyncio
import datetime
import logging
import sys
import time

import aiohttp
import pymongo
import requests
from pymongo.errors import ConnectionFailure


class TickerPoller:
    """Опрашиватель котировок с биржи

    Опрашивает каждые несколько секунд API биржы, сохраняет котировки в БД"""

    def __init__(self, exchange_api: str, db_connection: str, polling_interval: int = 5):
        """Инициализировать поллер адресом биржи и интервалом"""
        self.exchange_api = exchange_api
        self.polling_interval = polling_interval
        self.db_connection = db_connection
        self.pairs = []
        self.pairs_stripped = []
        self.pairs_initialized = asyncio.Event()
        self.loop = asyncio.get_event_loop()

        # логгирование
        logging.basicConfig(stream=sys.stdout, level=logging.DEBUG)

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

        self.db_pairs = self.client.pairs
        # db_status = self.client.admin.command("serverStatus")
        # logging.debug("Статус БД: %ss", db_status)

    def start_polling(self):
        """Начать опрос биржи в цикле asyncio"""
        asyncio.ensure_future(self.update_pairs())

        self.loop.run_until_complete(self.polling_loop())

    async def update_pairs(self):
        """Обновить валютные пары (делаем редко, т.к. меняются, вероятно, редко"""
        while True:
            try:
                response = requests.get(self.exchange_api + 'info')
                self.pairs = response.json()['pairs']
                self.pairs_stripped = [key for key, value in self.pairs.items()]
                logging.debug('Получили пары: %s', self.pairs_stripped)

                # Выставляем флаг получения пар
                self.pairs_initialized.set()

                # Спим час
                await asyncio.sleep(1 * 60 * 60)
            except TimeoutError:
                logging.debug('Таймаут при обращении к бирже')
                await asyncio.sleep(10)
            except Exception as ex:
                logging.critical('Ошибка при получении пар с биржи: %s', ex)
                await asyncio.sleep(10)

    async def polling_loop(self):
        """Цикл опроса биржи"""
        # ожидаем получения пар
        await self.pairs_initialized.wait()

        while True:
            # получить данные по парам
            pairs_string = '-'.join(self.pairs)
            async with aiohttp.ClientSession() as session:
                async with session.get(self.exchange_api + 'ticker/' + pairs_string) as response:
                    result = await response.json()
                    for pair, values in result.items():
                        data_to_write = {'time': datetime.datetime.utcnow(),
                                         'value': float(values['last'])}
                        # logging.debug(f"Получили данные по паре: {pair}, {data_to_write}")
                        self.db_pairs[pair].insert_one(data_to_write)
            logging.debug('Получили данные по парам')

            # Биржа кеширует данные, часто опрашивать смысла нет
            await asyncio.sleep(self.polling_interval)


if __name__ == "__main__":
    # Запустить опрос биржы
    poller = TickerPoller('https://wex.nz/api/3/', db_connection='mongodb')
    poller.start_polling()
