from apistar.test import TestClient
from .app import *


def test_http_request():
    """Протестировать самые базовые функции"""
    # подменяем базу данных на тестовую
    test_db_layer = DatabaseLayer(db_connection='localhost', database_name='test')
    app.db_layer = test_db_layer

    # заполняем тестовыми данными
    now = datetime.datetime.utcnow()
    test_pair = "test_pair"
    test_data = [
        {'time': now, 'value': 100.0},
        {'time': now, 'value': 0.0},
        {'time': now, 'value': 100.0},
        {'time': now, 'value': 0.0},
    ]
    test_db_layer.db_pairs[test_pair].insert_many(test_data)

    client = TestClient(app)

    # проверяем вычисление среднего
    response = client.get(f'http://localhost/ticker?pair={test_pair}')
    assert response.status_code == 200
    assert float(response.json()['average']) == 50.0

    # несуществующая пара
    response = client.get(f'http://localhost/ticker?pair=asdfasdfasdfasdf')
    assert response.status_code == 400
