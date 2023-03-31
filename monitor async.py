from datetime import datetime, timedelta
from binance import AsyncClient # Client for real-time monitoring
import asyncio
import time

import pandas as pd
import matplotlib.pyplot as plt

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

    last_hour = await execute_async("""SELECT e.opentime, e.closetime, e.open, e.close, b.open, b.close 
            FROM ethusdt e 
            FULL OUTER JOIN btcusdt b 
            ON CAST(e.opentime AS TIMESTAMP(0)) = CAST(b.opentime AS TIMESTAMP(0))
            ORDER BY e.opentime DESC LIMIT 60;
            """)
    # print(len(last_hour))
    
    runtime = datetime.now().timestamp()
    last_hour = list(last_hour)
    last = last_hour[0]
    hour_ago = last_hour[60]
    print(hour_ago)
    print(last)
    print(last[0], last[1], last[2], last[3], last[4], last[5])
    
    eth_change = ( hour_ago[3] - last[3] ) / hour_ago[3]
    btc_change = ( hour_ago[5] - last[5] ) / hour_ago[5]
    eth_own = eth_change - btc_change
    
    print(eth_change)
    print(btc_change)
    print(eth_own)
    
    # Adding a new row
    # my_list.insert(0, new_row)
    
    
        
    # Set up the dataframe
    # df = pd.DataFrame(last_hour, columns=[
    #                 'opentime', 'closetime', 'e_open', 'e_close', 'b_open', 'b_close'])
    # df.set_index('opentime', inplace=True)

    # # Reverse row order so chronologically last row comes last in dataframe 
    # df = df.iloc[::-1]


    # # Calculate returns by getting percentage change on 'close' column
    # df['eth_returns'] = (df['e_open'] - df['e_close']) / df['e_open']
    # df['btc_returns'] = (df['b_open'] - df['b_close']) / df['b_open']

    # # Residuals (ETHUSDT own movements) would be the difference between ETHUSDT & BTCUSDT returns for given interval 
    # df['eth_hourly_returns'] = df['e_close'].pct_change(periods=60)
    # df['btc_hourly_returns'] = df['b_close'].pct_change(periods=60)
    # df['eth_residuals'] = df['eth_hourly_returns'] - df['btc_hourly_returns']
    # print(df[-60:])

    # # Create a new row as a Pandas Series
    # # new_row = pd.Series(['1680254100000', '1680254160000', 1, 1, 1, 1], index=['opentime', 'closetime', 'e_open', 'e_close', 'b_open', 'b_close'])
    
    # # Create a new DataFrame with the row you want to add
    # new_data = {'opentime': [1680254100000], 
    #             'closetime': [1680254160000],
    #             'e_open': [1], 
    #             'e_close': [1], 
    #             'b_open': [1], 
    #             'b_close': [1]}
    # new_df = pd.DataFrame(new_data)
    

    # # Append the new row to the DataFrame
    # df = pd.concat([df, new_df], ignore_index=True)
    
    # print(df[-60:])
    
    # hourly_data = df.resample('1H', on='closetime').last()
    
    # print(hourly_data)

    
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
            
            # TODO FIX THE TIME 5 -> 60
            if datetime.now().timestamp() - runtime > 60:
                # Reset the timer
                runtime = datetime.now().timestamp()
                print('[INFO] 60 sec passed, writing a new kline to the DB')
                
                current_time = datetime.now()
                
                result = []
                # To return 
                for symbol in symbols:
                    output = asyncio.Queue()
                    await update_database(symbol, current_time, output_queue=output)
                    result.append(await output.get())
                    
                    
                print('[INFO] Databases are up to date')
                print(result)
# [[1680254100000, '1791.85',  '1792.07',  '1791.44',  '1791.97',  '1343.762', 1680254159999, '2407725.23032', 1138, '617.806', '1106957.54965', '0'], 
#  [1680254160000, '27829.00', '27839.80', '27822.00', '27838.20', '238.402',  1680254219999, '6635333.68500', 2850, '122.800', '3417911.98010', '0']]
                
                # print("[DEBUG] Last hour klines: ", last_hour)
                for item in result:
                    # last_hour.extend(item)
                    pass
                
                
            
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
