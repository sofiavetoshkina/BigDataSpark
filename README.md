# Отчёт по лабораторной работе №2

**Тема:** ETL-пайплайн на Apache Spark (PostgreSQL → звезда → ClickHouse).

**Студент:** Ветошкина София, группа М8О-303Б-23

## Что сделано

Реализован ETL-пайплайн на Spark, который:
1. Забирает сырые данные (10 файлов `MOCK_DATA*.csv`, 10000 строк) из PostgreSQL.
2. Превращает их в модель «звезда» в PostgreSQL.
3. На основе звезды строит 6 витрин в ClickHouse.

## Структура проекта

```
BigDataSpark/
├── docker-compose.yml      — PostgreSQL, ClickHouse, Spark
├── исходные данные/        — 10 CSV файлов с данными
├── sql/
│   ├── 01_init.sql         — создание mock_data и загрузка CSV в PostgreSQL
│   └── queries.sql         — SQL-запросы к 6 витринам в ClickHouse для отчётов
├── spark/
│   ├── Dockerfile          — bitnami/spark + PostgreSQL и ClickHouse JDBC
│   ├── etl_to_star.py      — raw → звезда в PostgreSQL
│   ├── etl_to_marts.py     — звезда → 6 витрин в ClickHouse
│   └── run_etl.sh          — запускает оба Spark-джоба по очереди
└── REPORT.md
```

## Модель «звезда»

Одна таблица фактов и 6 измерений:

- `fact_sales` — факт продажи (ключи на все измерения, `sale_quantity`, `sale_total_price`).
- `dim_customer` — покупатели.
- `dim_seller` — продавцы.
- `dim_product` — товары.
- `dim_store` — магазины.
- `dim_supplier` — поставщики.
- `dim_date` — даты продаж (день, месяц, год, квартал).

## 6 витрин в ClickHouse

| Таблица | Что считает |
|--------|------|
| `mart_product_sales` | Топ продуктов, выручка по категориям, средний рейтинг |
| `mart_customer_sales` | Топ клиентов, распределение по странам, средний чек |
| `mart_time_sales` | Тренды продаж по месяцам и годам, средний размер заказа |
| `mart_store_sales` | Топ магазинов, распределение по городам/странам, средний чек |
| `mart_supplier_sales` | Топ поставщиков, средняя цена, распределение по странам |
| `mart_product_quality` | Рейтинги, количество отзывов, связь с продажами |

## Как запустить

```bash
docker compose up --build
```

Пайплайн запускается автоматически:
1. PostgreSQL поднимается, из `sql/01_init.sql` создаётся таблица `mock_data` и в неё копируются все 10 CSV.
2. Spark-контейнер ждёт БД, потом запускает `etl_to_star.py` — строится звезда в PostgreSQL.
3. Затем `etl_to_marts.py` читает звезду и пишет 6 витрин в ClickHouse.

## Как проверить результат

Звезда в PostgreSQL:
```bash
docker exec -it postgres psql -U my_user -d bigdata_lab2 -c "\dt"
docker exec -it postgres psql -U my_user -d bigdata_lab2 -c "SELECT count(*) FROM fact_sales;"
```

Витрины в ClickHouse (по одному примеру на каждую из 6 витрин):
```bash
docker exec -it clickhouse clickhouse-client --user click --password click --query "SHOW TABLES FROM default;"

# 1. Продукты — топ-10 по количеству продаж
docker exec -it clickhouse clickhouse-client --user click --password click --query "SELECT product_name, product_category, total_quantity_sold FROM mart_product_sales ORDER BY total_quantity_sold DESC LIMIT 10;"

# 2. Клиенты — топ-10 по сумме покупок
docker exec -it clickhouse clickhouse-client --user click --password click --query "SELECT customer_name, customer_country, total_spent FROM mart_customer_sales ORDER BY total_spent DESC LIMIT 10;"

# 3. Время — месячные тренды продаж
docker exec -it clickhouse clickhouse-client --user click --password click --query "SELECT year, month, total_sales, num_orders FROM mart_time_sales ORDER BY year, month;"

# 4. Магазины — топ-5 по выручке
docker exec -it clickhouse clickhouse-client --user click --password click --query "SELECT store_name, store_city, store_country, total_revenue FROM mart_store_sales ORDER BY total_revenue DESC LIMIT 5;"

# 5. Поставщики — топ-5 по выручке
docker exec -it clickhouse clickhouse-client --user click --password click --query "SELECT supplier_name, supplier_country, total_revenue FROM mart_supplier_sales ORDER BY total_revenue DESC LIMIT 5;"

# 6. Качество — корреляция рейтинга и объёма продаж
docker exec -it clickhouse clickhouse-client --user click --password click --query "SELECT corr(avg_rating, total_quantity_sold) AS rating_vs_sales_corr FROM mart_product_quality;"
```

Полный набор запросов (по 3 на каждую витрину, ровно под пункты из `README.md`) лежит в `sql/queries.sql` — их удобно запускать в DBeaver.

## Используемые драйверы

- PostgreSQL JDBC: `postgresql-42.7.3.jar`
- ClickHouse JDBC (официальный): `clickhouse-jdbc-0.6.3-all.jar`, класс `com.clickhouse.jdbc.ClickHouseDriver`

Запись в ClickHouse идёт именно через Spark JDBC (`df.write.jdbc(...)`), а не через `client.execute("INSERT ...")`.

## Остановить

```bash
docker compose down -v
```
