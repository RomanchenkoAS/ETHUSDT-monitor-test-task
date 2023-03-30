""" This is a synchronous script for ETHUSDT own movement visualisation """

import pandas as pd # For data analysis
import matplotlib.pyplot as plt # For visualisation
from db_config import execute

# Get historical data by joining two tables 
# CAST clause is used to round up timestamps and avoid discrepancy of less then 1s
query = """SELECT e.opentime, e.closetime, e.open, e.close, b.open, b.close 
            FROM ethusdt e 
            FULL OUTER JOIN btcusdt b 
            ON CAST(e.opentime AS TIMESTAMP(0)) = CAST(b.opentime AS TIMESTAMP(0));
        """
rows = execute(query)

# Set up the dataframe
df = pd.DataFrame(rows, columns=[
                  'opentime', 'closetime', 'e_open', 'e_close', 'b_open', 'b_close'])
df.set_index('opentime', inplace=True)

# Calculate returns by getting percentage change on 'close' column
df['eth_returns'] = (df['e_open'] - df['e_close']) / df['e_open']
df['btc_returns'] = (df['b_open'] - df['b_close']) / df['b_open']

# Residuals (ETHUSDT own movements) would be the difference between ETHUSDT & BTCUSDT returns for given interval 
df['residuals'] = df['eth_returns'] - df['btc_returns'] 

# Calculate change for last hour
df['eth_own_change'] = df['residuals'].rolling(window=60).sum()

# Find where sum of hourly change exceeds 1%
markers = df[df['eth_own_change'].abs() > 0.01]

# Plots
plt.plot(df.index, df['eth_returns'], color='blue', label='ETH Returns')
plt.plot(df.index, df['btc_returns'], color='red', label='BTC Returns')
plt.plot(df.index, df['residuals'], color='green', label='ETH own movements')
plt.plot(df.index, df['eth_own_change'], color='pink', label='sum of ETH own change for last hour')

# Axes
plt.axhline(y=0, color='black', linestyle='--')
plt.axhline(y=0.01, color='black', linestyle='--')
plt.axhline(y=-0.01, color='black', linestyle='--')

# Add markers
plt.scatter(markers.index, markers['residuals'], color='purple', marker='*', label='>1% change for last hour')

# Legend
plt.title('Currency movements over time')
plt.xlabel('Datetime')
plt.ylabel('%')
plt.legend()

# Display
plt.show()

# Blue and red lines represents returns of ETHUSDT and BTCUSDT respectively for each time interval
# Green line is a substraction of them, which represent ETHUSDT OWN movement for this interval
# Pink line shows the sum ETHUSDT movements for the last hour
# Stars (*) are shown on points of time when sum of ETHUSDT movement exceeded 1%
