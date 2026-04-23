-- =====================================================================
-- Аналитические запросы к 6 витринам в ClickHouse
-- Выполнять в DBeaver (подключение к ClickHouse) или clickhouse-client
-- =====================================================================


-- =====================================================================
-- 1. Витрина продаж по продуктам (mart_product_sales)
-- =====================================================================

-- 1.1 Топ-10 самых продаваемых продуктов
SELECT product_name, product_category, total_quantity_sold
FROM mart_product_sales
ORDER BY total_quantity_sold DESC
LIMIT 10;

-- 1.2 Общая выручка по категориям продуктов
SELECT product_category,
       sum(total_revenue) AS revenue
FROM mart_product_sales
GROUP BY product_category
ORDER BY revenue DESC;

-- 1.3 Средний рейтинг и количество отзывов для каждого продукта
SELECT product_name, product_category, avg_rating, total_reviews
FROM mart_product_sales
ORDER BY avg_rating DESC;


-- =====================================================================
-- 2. Витрина продаж по клиентам (mart_customer_sales)
-- =====================================================================

-- 2.1 Топ-10 клиентов с наибольшей общей суммой покупок
SELECT customer_name, customer_country, total_spent
FROM mart_customer_sales
ORDER BY total_spent DESC
LIMIT 10;

-- 2.2 Распределение клиентов по странам
SELECT customer_country,
       count() AS num_customers,
       sum(total_spent) AS total_revenue
FROM mart_customer_sales
GROUP BY customer_country
ORDER BY total_revenue DESC;

-- 2.3 Средний чек для каждого клиента
SELECT customer_name, customer_country, avg_check
FROM mart_customer_sales
ORDER BY avg_check DESC;


-- =====================================================================
-- 3. Витрина продаж по времени (mart_time_sales)
-- =====================================================================

-- 3.1 Месячные тренды продаж
SELECT year, month, total_sales, num_orders
FROM mart_time_sales
ORDER BY year, month;

-- 3.2 Годовые тренды (сравнение выручки за разные периоды)
SELECT year,
       sum(total_sales) AS year_revenue,
       sum(num_orders)  AS year_orders
FROM mart_time_sales
GROUP BY year
ORDER BY year;

-- 3.3 Средний размер заказа по месяцам
SELECT year, month, avg_order_size
FROM mart_time_sales
ORDER BY year, month;


-- =====================================================================
-- 4. Витрина продаж по магазинам (mart_store_sales)
-- =====================================================================

-- 4.1 Топ-5 магазинов с наибольшей выручкой
SELECT store_name, store_city, store_country, total_revenue
FROM mart_store_sales
ORDER BY total_revenue DESC
LIMIT 5;

-- 4.2 Распределение продаж по городам и странам
SELECT store_country, store_city,
       sum(total_revenue) AS revenue,
       sum(num_sales)     AS sales
FROM mart_store_sales
GROUP BY store_country, store_city
ORDER BY revenue DESC;

-- 4.3 Средний чек для каждого магазина
SELECT store_name, store_city, avg_check
FROM mart_store_sales
ORDER BY avg_check DESC;


-- =====================================================================
-- 5. Витрина продаж по поставщикам (mart_supplier_sales)
-- =====================================================================

-- 5.1 Топ-5 поставщиков с наибольшей выручкой
SELECT supplier_name, supplier_country, total_revenue
FROM mart_supplier_sales
ORDER BY total_revenue DESC
LIMIT 5;

-- 5.2 Средняя цена товаров от каждого поставщика
SELECT supplier_name, supplier_country, avg_product_price
FROM mart_supplier_sales
ORDER BY avg_product_price DESC;

-- 5.3 Распределение продаж по странам поставщиков
SELECT supplier_country,
       sum(total_revenue) AS revenue,
       count()            AS num_suppliers
FROM mart_supplier_sales
GROUP BY supplier_country
ORDER BY revenue DESC;


-- =====================================================================
-- 6. Витрина качества продукции (mart_product_quality)
-- =====================================================================

-- 6.1 Продукты с наивысшим рейтингом
SELECT product_name, product_category, avg_rating, total_reviews
FROM mart_product_quality
ORDER BY avg_rating DESC
LIMIT 10;

-- 6.2 Продукты с наименьшим рейтингом
SELECT product_name, product_category, avg_rating, total_reviews
FROM mart_product_quality
ORDER BY avg_rating ASC
LIMIT 10;

-- 6.3 Корреляция между рейтингом и объёмом продаж
SELECT corr(avg_rating, total_quantity_sold) AS rating_vs_sales_corr
FROM mart_product_quality;

-- 6.4 Продукты с наибольшим количеством отзывов
SELECT product_name, product_category, total_reviews, avg_rating
FROM mart_product_quality
ORDER BY total_reviews DESC
LIMIT 10;
