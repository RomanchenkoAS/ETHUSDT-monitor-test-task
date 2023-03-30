from datetime import datetime, timedelta
from binance.client import Client # This is for initial updating the database on launch 
from binance import AsyncClient # This is for real-time monitoring
import time

from db_config import execute


def get_keys():
    """ Fetch binance API-keys from api_key.txt in current folder 
        NOTE: Api-key file is ignored by source control """

    try:
        with open('api_key.txt', 'r') as key:
            api_key = key.readline().strip()
            api_secret = key.readline().strip()
    except FileNotFoundError as _ex:
        print("[ERR] Error occured while opening the file containing keys: ", _ex)
        print("[ERR] API key file is not present in source control it should be delivered in order to run")

    return api_key, api_secret


def timestamp_generator(start, end, interval):
    """ Generates timestamps from start till now with given interval """
    
    current = start

    while (current < end):
        yield current
        current += timedelta(minutes=interval)

    # print(f"[TIMESTAMP] Last timestamp: {end}")


def generate_queries(klines, symbol):
    """ Format data recieved from API into a list of SQL queries to insert into database"""
    
    query_list = []

    for kline in klines:
        opentime = (datetime.fromtimestamp(
            kline[0]/1000)).strftime('%Y-%m-%d %H:%M:%S')
        open = kline[1]
        high = kline[2]
        low = kline[3]
        close = kline[4]
        volume = kline[5]
        closetime = (datetime.fromtimestamp(
            kline[6]/1000)).strftime('%Y-%m-%d %H:%M:%S')

        query = f"INSERT INTO {symbol.lower()} (opentime, open, high, low, close, volume, closetime) VALUES ('{opentime}',{open},{high}, {low}, {close}, {volume}, '{closetime}');"
        query_list.append(query)

    return query_list


def update_database(symbol, end_timestamp):

    print(f'[INFO] Updating data for {symbol}')

    last_row = execute(
        f"SELECT * FROM {symbol.lower()} ORDER BY opentime DESC LIMIT 1;")
    if last_row:
        # If a database exists, starting time would be when the last kline closed
        starting_timestamp = last_row[0][6]
    else:
        # Starting time year, month, day (+hour, minute, etc)
        starting_timestamp = datetime(2023, 2, 1)

    # to specify time delta in minutes * number of klines in response
    delta_minutes = 1 * 1000
    generator = timestamp_generator(
        starting_timestamp, end_timestamp, delta_minutes)

    # Initialize binance client
    api_key, api_secret = get_keys()
    client = Client(api_key, api_secret)

    # Counter for queries
    queries_num = 0

    while generator:
        try:
            # This is the timestamp value in YYYY-MM-DD HH-MM-SS
            timestamp = next(generator)
            # This is the value to make request - value in milliseconds (Unix timestamp)
            timestamp_msec = int(timestamp.timestamp())*1000

            print('[INFO] request start')

            klines = client.futures_klines(
                symbol=symbol, interval=Client.KLINE_INTERVAL_1MINUTE, startTime=timestamp_msec, limit=1000)

            query_list = generate_queries(klines, symbol)

            execute(query_list)

            # Query count
            queries_num += len(query_list)

            print('[INFO] request over\n')

        except StopIteration:
            print("[INFO] Loop is over")
            print("[INFO] Executed queries: ", queries_num)
            break


def main():

    symbols = ['ETHUSDT', 'BTCUSDT']
    current_time = datetime.now()
    for symbol in symbols:
        update_database(symbol, current_time)

    print('[INFO] Databases are up to date')

    for symbol in symbols:
        # Strip the last klines from db since they are not closed yet
        execute(
            f'DELETE FROM {symbol.lower()} WHERE opentime IN (SELECT opentime FROM {symbol.lower()} ORDER BY opentime DESC LIMIT 1);')

    # Start monitor

    # Initialize binance client
    api_key, api_secret = get_keys()
    client = Client(api_key, api_secret)

    print("\n[INFO] Start monitoring")
    while True:
        t0 = time.time()
        # print(t0)
        symbol = 'ETHUSDT' 
        ticker_e = client.futures_symbol_ticker(symbol=symbol)
        t1 = time.time()
        # print(ticker, ': ', round(t1 - t0,2))
        
        symbol = 'BTCUSDT'
        ticker_b = client.futures_symbol_ticker(symbol=symbol)
        t2 = time.time()
        # print(ticker, ': ', round(t2 - t1,2))
        
        print(f"ETHUDST = {ticker_e['price']} | BTCUSD = {ticker_b['price']} | runtime = {round(time.time() - t0,2)}")
        
        time.sleep(3)


if __name__ == '__main__':
    main()
