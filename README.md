# ticker_api
Простой API, отдающий среднее значение котировки за 10 минут с биржи wex.nz.

# Как запустить
1. Клонировать репозиторий
2. docker compose up --build
3. По умолачанию разворачивается на localhost:8888, пример вызова: http://127.0.0.1:8888/ticker/btc_usd