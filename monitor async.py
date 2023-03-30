from datetime import datetime, timedelta
from binance import AsyncClient # Client for real-time monitoring
import asyncio
import time

from db_config import execute


def timestamp_generator(start, end, interval):
    """ Generates timestamps from start point till end (e.g. now) with given interval """
    
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


async def update_database(symbol, end_timestamp):

    print(f'[INFO] Updating data for {symbol}')

    last_row = await execute(
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
    client = await AsyncClient.create()

    # Counter for queries
    queries_num = 0

    while generator:
        try:
            # This is the timestamp value in YYYY-MM-DD HH-MM-SS
            timestamp = next(generator)
            # This is the value to make request - value in milliseconds (Unix timestamp)
            timestamp_msec = int(timestamp.timestamp())*1000

            print('[INFO] request start')

            klines = await client.futures_klines(
                symbol=symbol, interval=client.KLINE_INTERVAL_1MINUTE, startTime=timestamp_msec, limit=1000)

            # Check if the last kline is already closed, if not - remove it from the list
            now_msec = int(datetime.now().timestamp() * 1000)
            # Get the open time of the last kline
            last_kline_open_time_msec = klines[-1][0]
            
            if last_kline_open_time_msec > (now_msec - 60000):
                klines.pop()

            query_list = generate_queries(klines, symbol)

            await execute(query_list)

            # Query count
            queries_num += len(query_list)

            print('[INFO] request over\n')

        except StopIteration:
            await client.close_connection()
            # print("[INFO] Loop is over")
            print("[INFO] Executed queries: ", queries_num)
            break


async def get_ticker(client, symbol):
    ticker = await client.futures_symbol_ticker(symbol=symbol)
    return ticker


async def main():

    symbols = ['ETHUSDT', 'BTCUSDT']
    current_time = datetime.now()
    for symbol in symbols:
        await update_database(symbol, current_time)

    print('[INFO] Databases are up to date')

    last_hour = await execute("""SELECT e.opentime, e.closetime, e.open, e.close, b.open, b.close 
            FROM ethusdt e 
            FULL OUTER JOIN btcusdt b 
            ON CAST(e.opentime AS TIMESTAMP(0)) = CAST(b.opentime AS TIMESTAMP(0))
            ORDER BY e.opentime DESC LIMIT 60;
            """)
    # print(last_hour)
    
    runtime = datetime.now().timestamp()
    # print(runtime)
    # Start monitoring

    # Initialize binance client
    client = await AsyncClient.create()
    symbols = ['ETHUSDT', 'BTCUSDT']
    
    print("\n[INFO] Start monitoring")
    try:
        while True:
            t0 = datetime.now().timestamp()
            tickers = await asyncio.gather(*[get_ticker(client, symbol) for symbol in symbols])
            print(f"ETHUDST = {tickers[0]['price']} | BTCUSD = {tickers[1]['price']} | runtime = {round(time.time() - t0, 2)}s")
            
            if datetime.now().timestamp() - runtime > 60:
                # print('5 sec passed')
                runtime = datetime.now().timestamp()
                print('[INFO] 60 sec passed, writing a new kline to the DB')
                current_time = datetime.now()
                for symbol in symbols:
                    await update_database(symbol, current_time)
                print('[INFO] Databases are up to date')
            
            # NOTE: API is limited up to 1200 request per minute hence the delay
            await asyncio.sleep(1)
            
    except Exception as _ex:
        await client.close_connection()
        print('[INFO] Script exited: ', _ex)

if __name__ == '__main__':
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

    
    # loop = asyncio.get_event_loop()
    loop.run_until_complete(main())
