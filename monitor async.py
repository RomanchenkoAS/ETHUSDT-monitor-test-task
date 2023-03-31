from datetime import datetime, timedelta
from binance import AsyncClient  # Client for real-time monitoring
import asyncio
import time

import traceback


import pandas as pd
# import matplotlib.pyplot as plt

from db_config import execute_async


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


async def update_database(symbol, end_timestamp, output_queue=None):

    print(f'[INFO] Updating data for {symbol}')

    last_row = await execute_async(
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

            # While function updates db in realtime we need to yield new klines for data analysis
            # yield klines[-1]
            if output_queue:
                await output_queue.put(klines[-1])

            query_list = generate_queries(klines, symbol)

            await execute_async(query_list)

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

    last_hour = await execute_async("""SELECT e.opentime, e.close, b.close 
            FROM ethusdt e 
            FULL OUTER JOIN btcusdt b 
            ON CAST(e.opentime AS TIMESTAMP(0)) = CAST(b.opentime AS TIMESTAMP(0))
            ORDER BY e.opentime DESC LIMIT 61;
            """)

    timer = datetime.now().timestamp()

    # Set up the dataframe
    df = pd.DataFrame(last_hour, columns=['opentime', 'e_close', 'b_close'])
    df.set_index('opentime', inplace=True)

    # This kline is used to calculate returns of ETHUSDT & BTCUSDT
    hour_ago_kline = df.iloc[60]

    # Start monitoring

    # Initialize binance client
    client = await AsyncClient.create()
    symbols = ['ETHUSDT', 'BTCUSDT']

    print("\n[INFO] Start monitoring")
    try:
        while True:

            # To record how long it took to make requests and recieve data
            t0 = datetime.now().timestamp()

            # Make async requests
            tickers = await asyncio.gather(*[get_ticker(client, symbol) for symbol in symbols])

            # Parse responses
            ethusdt = float(tickers[0]['price'])
            btcusdt = float(tickers[1]['price'])

            e_return = 100 * (ethusdt - hour_ago_kline['e_close']) / ethusdt
            b_return = 100 * (btcusdt - hour_ago_kline['b_close']) / btcusdt
            residual = e_return - b_return

            print(f"""ETHUDST = {ethusdt:.2f} ({e_return:.2f}%) Own change (1H) = {residual:.2f}% | BTCUSD = {btcusdt:.2f} ({b_return:.2f}%)| Request runtime = {(time.time() - t0):.2f}s""")

            # Notification whenever own movement exceeds 1%
            if residual > 1:
                print("[INFO] ETHUSDT own movement is over 1% for last hour ")

            # To update the database each minute
            if datetime.now().timestamp() - timer > 60:
                # Reset the timer
                timer = datetime.now().timestamp()
                print('[INFO] 60 sec passed, writing a new kline to the DB')

                current_time = datetime.now()

                result = []

                # To return data out of the async function use a Queue
                for symbol in symbols:
                    output = asyncio.Queue()
                    await update_database(symbol, current_time, output_queue=output)
                    result.append(await output.get())

                # Parse opentime and close prices from result
                eth_kline = result[0]
                btc_kline = result[1]

                opentime = (datetime.fromtimestamp(
                    eth_kline[0]/1000)).strftime('%Y-%m-%d %H:%M:%S')
                e_close = eth_kline[4]
                b_close = btc_kline[4]

                # Make recieved data a pandas dataframe
                new_data = {'opentime': opentime,
                            'e_close': e_close, 'b_close': b_close}
                new_row = pd.DataFrame(
                    new_data, columns=['opentime', 'e_close', 'b_close'], index=[0])
                new_row.set_index('opentime', inplace=True)

                # Add that new dataframe to the existing data
                df = pd.concat([new_row, df]).reset_index(drop=True)

                # Update kline to which data is compared
                hour_ago_kline = df.iloc[60]

                print('[INFO] Databases are up to date')

            # NOTE: API is limited up to 1200 request per minute hence the delay
            await asyncio.sleep(1)

    except Exception as _ex:
        await client.close_connection()
        print('[INFO] Script exited: ', _ex)


if __name__ == '__main__':
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    loop.run_until_complete(main())
