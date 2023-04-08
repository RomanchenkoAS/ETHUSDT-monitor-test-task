#### Задача

1. Определить собственные движения фьючерса ETHUSDT, исключив из них движения вызванные влиянием цены BTCUSDT.

2. Написать программу на Python, которая в реальном времени (с минимальной задержкой) следит за ценой фьючерса ETHUSDT и используя выбранную методику, определяет собственные движение цены ETH. При изменении цены на 1% за последние 60 минут, программа выводит сообщение в консоль. При этом программа должна продолжает работать дальше, считаывая актуальную цену.

#### Зависимости

- asyncpg: асинхронное управление PostgreSQL
- binance: API Binance 
- pandas: анализ и управление данными

#### Файлы

- monitor_async.py - основной исходный код проекта, позволяющий мониторить собственные движения цены фьючерса ETHUSDT с минимальной задержкой.
- analysis.py - визуальное отображение собственного движения фьючерса ETHUSDT на графике для демонстрации методики расчета.
- db_config.py - код для конфигурации соединения с базой данных и выполнения SQL запросов 

#### Конфигурация

Прежде чем исполнять код, необходимо настроить базу данных psql.

HOST = "127.0.0.1" <br>
USER = "postgres" <br>
PASSWORD = "******" <br>
DB_NAME = "crypto" <br>

Кроме того, требуется создать реляции PostgreSQL следующим запросом:

CREATE TABLE IF NOT EXISTS <ethusdt | btcusdt> ( <br>
opentime timestamp without time zone NOT NULL, <br>
open double precision, <br>
high double precision, <br>
low double precision, <br>
close double precision, <br>
volume double precision, <br>
closetime timestamp without time zone, <br>
CONSTRAINT template_pkey PRIMARY KEY (opentime) ); <br>

#### Структура monitor_async.py

- execute(query_list): Функция принимает список SQL запросов и выполняет их в ранее сконфигурированной базе данных. При запросе SELECT возваращется результат запроса. 
- timestamp_generator(start, end, interval): Функция генерирует очередь временных меток между заданным "стартом" и "финишем" с определенным интервалом. Например, с 00:00 до данного момента, с интервалом в одну минуту.
- generate_queries(klines, symbol): Функция формирует SQL запросы INSERT из свечей, полученных из API Binancе.
- update_database(symbol, end_timestamp, output_queue=None): Эта функция обновляет базу данных до текущего момента, выполняется перед основным циклом работы программы.

#### Работа кода

Результат работы кода monitor_async.py выглядит следующим образом:

![image](https://user-images.githubusercontent.com/119735427/229299323-25887da0-755e-43c7-b762-2cc09e87eab4.png)

Анализ движений фьючерса при помощи analysis.py:

![image](https://user-images.githubusercontent.com/119735427/230715346-80898d11-2880-448a-a7b6-a1b9f8b59e1a.png)
