# BigDataSpark

Анализ больших данных - лабораторная работа №2 - ETL реализованный с помощью Spark

## Содержание
1. Структура проекта
2. Запуск проекта
3. Описание аналитических запросов

## Структура проекта
- ```/data``` - исходные CSV-файлы для анализа
- ```/init``` - SQL-скрипты для инициализации PostgreSQL (создание схем ```mock``` и ```dwh```)
- ```/clickhouse_init``` - SQL-скрипт для создание витрин в ClickHouse
- ```/scripts``` - ETL-скрипты на PySpark:
    - ```etl_to_dwh.py``` - заполнение хранилища ```dwh```
    - ```etl_reports.py``` - вспомогательные функции для витрин
    - ```etl_to_clickhouse.py``` - построение витрин в ClickHouse
- ```/jars``` - JDBC-драйверы для работы PostgreSQL и ClickHouse
- ```/sql_analytics``` - аналитические запросы к витринам в ClickHouse
- ```docker-compose.yml```

## Запуск проекта
1. Запустите Docker Desktop.
2. Зайдите в папку-корень проекта.
3. Откройте командную строку cmd.exe в этой папке и выполните:
```docker-compose up -d```
4. После инициализации автоматически выполнятся скрипты SQL. Проверить их выполнение можно с помощью следующих команд:
```
docker logs postgres_lab2
docker logs clickhouse_lab2
```

__Примечание:__ шаги 5-6 выполнять строго 1 раз. Повторное выполнение приведёт к дублированию данных в таблицах. 

5. Выполните загрузку данных в схему ```dwh``` с помощью следующей команды:
```
docker exec -it spark_lab2 /opt/spark/bin/spark-submit  --master local[*]   --jars /opt/etl/drivers/postgresql-42.7.10.jar   /opt/etl/scripts/etl_to_dwh.py
```
6. Выполните построение витрин в ClickHouse с помощью следующей команды:
```
docker exec -it spark_lab2 /opt/spark/bin/spark-submit --master local[*]  --jars /opt/etl/drivers/postgresql-42.7.10.jar,/opt/etl/drivers/clickhouse-jdbc-all-0.9.8.jar  /opt/etl/scripts/etl_to_clickhouse.py
```
7. Просмотреть результаты можно, например, с помощью DBeaver.
 
   Параметры подключения для PostgreSQL:  
   хост: ```localhost```  
   порт: ```5433```  
   база данных: ```lab_db2```  
   пользователь: ```postgres```  
   пароль: ```postgres```  

   Параметры подключения для ClickHouse:  
   хост: ```localhost```  
   порт: ```8123```  
   база данных: ```reports```  
   пользователь: ```default```  
  пароль: ```123```

8. Для завершения работы выполните:
```docker-compose down -v```

## Описание аналитических запросов

Витрина продаж по продуктам (папка ```product_sales_data_mart```):  
1. ```top_ten_best_selling_products.sql``` - Топ-10 самых продаваемых продуктов
2. ```total_revenue_by_product_category.sql``` - Общая выручка по категориям продуктов
3. ```average_rating_and_review_number.sql``` - Средний рейтинг и количество отзывов для каждого продукта


Витрина продаж по клиентам (папка ```customer_sales_data_mart```):
1. ```top_ten_clients_by_sum.sql``` - Топ-10 клиентов с наибольшей суммой покупок
2. ```clients_by_countries.sql``` - Распределение клиентов по странам
3. ```average_check_by_client.sql``` - Средний чек для каждого клиента


Витрина продаж по времени (папка ```monthly_sales_data_mart```):
1. ```monthly_sales_trend.sql``` - Месячные тренды продаж
2. ```total_revenue_by_year.sql``` - Сравнение выручки за разные года
3. ```average_order_size_by_month.sql``` - Средний размер заказа по месяцам



Витрина продаж по магазинам (папка ```store_sales_data_mart```):
1. ```top_five_stores_highest_revenue.sql``` - Топ-5 магазинов с наибольшей выручкой
2. ```sales_by_city.sql``` - Распределение продаж по городам
3. ```sales_by_country.sql``` - Распределение продаж по странам
4. ```avg_check_for_store.sql``` - Средний чек для каждого магазина



Витрина продаж по поставщикам (папка ```supplier_sales_data_mart```):
1. ```top_five_suppliers_by_revenue.sql``` - Топ-5 поставщиков с наибольшей выручкой
2. ```avg_product_price_by_supplier.sql``` - Средняя цена товаров от каждого поставщика
3. ```revenues_by_supplier_countries.sql``` - Распределение продаж по странам поставщиков



Витрина качества продукции (папка ```product_quality_data_mart```):
1. ```products_with_highest_rating.sql``` - Продукты с наивысшим рейтингом
2. ```products_with_lowest_rating.sql``` - Продукты с наименьшим рейтингом
3. ```rating_sales_correlation.sql``` - Корреляция между рейтингом и объемом продаж
4. ```products_with_most_review_count.sql``` - Продукты с наибольшим числом отзывов
 
