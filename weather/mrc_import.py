######  mrc_import.py  ######
# Pulls daily weather forecast from Environment Canada.
# 
# Original script from https://github.com/Toronto-Big-Data-Innovation-Team/activeto/blob/jasonlee/weekend_closures/scripts/import_weather.py

#Environment Canada imports
import asyncio
from types import coroutine
from env_canada import ECWeather, ECHistoricalRange

#other packages
import os
import sys
import datetime
import pandas as pd
import numpy as np
from configparser import ConfigParser
from psycopg2 import connect, sql
from psycopg2.extras import execute_values

"""CONFIG=ConfigParser()
CONFIG.read('config/db.cfg') # Change DB Settings in db.cfg
dbset=CONFIG['DBSETTINGS']
conn=connect(**dbset)"""

def pull_weather(today):
    ec_en = ECWeather(station_id='ON/s0000458', language='english')

    #for python 3.7+ ONLY:
    # asyncio.run(ec_en.update())

    loop = asyncio.get_event_loop()
    loop.run_until_complete(ec_en.update())

    return ec_en.conditions

def pull_weather_df(today):
    coord = ['43.74', '-79.37']
    
    ec = ECHistoricalRange(station_id='ON/s0000458', timeframe='daily', daterange=(today, today))
    ec.get_data()
    return ec.csv

def insert_weather(conn, weather_df):
    weather_fields = ['date', 'max_temp', 'min_temp', 'mean_temp', 'total_rain', 'total_snow', 'total_precip']
    with conn:
        with conn.cursor() as cur:
            insert_sql = '''INSERT INTO weather.historical_daily(dt, max_temp, min_temp, mean_temp, total_rain, total_snow, total_precip) VALUES %s'''
            execute_values(cur, insert_sql, weather_df[weather_fields].values)

if __name__ == '__main__':
   #Get current date to pull
    today = datetime.date.today()
    forecast = pull_weather(today)
    print(forecast)

    weather_csv = pull_weather_df(today)
    print(weather_csv)
   

    """weather_df = pd.read_excel(weather_file)
    weather_df = (weather_df.rename(columns={
        'Date/Time': 'date',
        'Max Temp (°C)': 'max_temp',
        'Min Temp (°C)': 'min_temp',
        'Mean Temp (°C)': 'mean_temp',
        'Total Rain (mm)': 'total_rain',
        'Total Snow (cm)': 'total_snow',
        'Total Precip (mm)': 'total_precip'})
        .replace({np.nan: None}))

    insert_weather(conn, weather_df)"""
