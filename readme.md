# Task
1. Determine the intrinsic movements of the ETHUSDT futures, excluding movements caused by the influence of the BTCUSDT price.

2. Write a real-time Python program with minimal delay that monitors the price of the ETHUSDT futures and, using the selected methodology, determines the intrinsic price movements of ETH. When the price changes by 1% in the last 60 minutes, the program outputs a message to the console. The program should continue running, continuously fetching the current price.

# Dependencies
- asyncpg: asynchronous PostgreSQL management
- binance: Binance API
- pandas: data analysis and management

# Files
- monitor_async.py: main source code of the project, allowing monitoring of the intrinsic price movements of ETHUSDT futures with minimal delay.
- analysis.py: visual representation of the intrinsic movements of ETHUSDT futures on a graph to demonstrate the calculation methodology.
- db_config.py: code for configuring the database connection and executing SQL queries.

  #### Structure of monitor_async.py
- execute(query_list): This function takes a list of SQL queries and executes them in the pre-configured database. For SELECT queries, the query result is returned.
- timestamp_generator(start, end, interval): This function generates a queue of timestamps between the specified "start" and "end" with a defined interval. For example, from 00:00 until the current moment, with a one-minute interval.
- generate_queries(klines, symbol): This function generates INSERT SQL queries from the candlestick data obtained from the Binance API.
- update_database(symbol, end_timestamp, output_queue=None): This function updates the database to the current moment and is executed before the main program loop.

# Configuration
Before executing the code, you need to configure the PostgreSQL database.
```
HOST = "127.0.0.1"
USER = "postgres"
PASSWORD = "******"
DB_NAME = "crypto"
```

In addition, you need to create the following PostgreSQL relations with the following query:

```CREATE TABLE IF NOT EXISTS <ethusdt | btcusdt> (
opentime timestamp without time zone NOT NULL,
open double precision,
high double precision,
low double precision, 
close double precision, 
volume double precision, 
closetime timestamp without time zone,
CONSTRAINT template_pkey PRIMARY KEY (opentime) );
```

# Code Execution
The result of executing the code monitor_async.py looks as follows:

![image](https://user-images.githubusercontent.com/119735427/229299323-25887da0-755e-43c7-b762-2cc09e87eab4.png)

Analysis of the futures movements using analysis.py:

![image](https://user-images.githubusercontent.com/119735427/230715346-80898d11-2880-448a-a7b6-a1b9f8b59e1a.png)
