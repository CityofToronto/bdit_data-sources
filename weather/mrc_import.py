######  mrc_import.py  ######
# Pulls daily weather forecast from Environment Canada.
# 
# Original script from https://github.com/Toronto-Big-Data-Innovation-Team/activeto/blob/jasonlee/weekend_closures/scripts/import_weather.py

#Environment Canada imports
import asyncio
from types import coroutine
import env_canada
#from env_canada import ECHistoricalRange, get_historical_stations

#other packages
import os
import sys
import datetime
import pandas as pd
import numpy as np
from configparser import ConfigParser
from psycopg2 import connect, sql
from psycopg2.extras import execute_values

CONFIG=ConfigParser()
CONFIG.read('config/db.cfg') # Change DB Settings in db.cfg
dbset=CONFIG['DBSETTINGS']
conn=connect(**dbset)

def pull_weather(today):
    ec_en = env_canada.ECWeather(station_id='ON/s0000458', language='english')

    #for python 3.7+ ONLY:
    # asyncio.run(ec_en.update())

    loop = asyncio.get_event_loop()
    loop.run_until_complete(ec_en.update())

    curr_weather = ec_en.conditions
    yday = today - datetime.timedelta(days=1)
    
    weather_dict = { 
        "today_dict": {
            "date": today,
            "max_temp": None,
            "min_temp": None,
            "total_precip": None,
            "humidity": curr_weather['humidity'['value']],
            "wind_speed": curr_weather['wind_speed'['value']],
            "condition": curr_weather['condition'['value']],
            "text_summary": curr_weather['text_summary'['value']],
            "date_pulled": today
        },
        "yday_dict": {
            "date": yday,
            "max_temp_yday": curr_weather['high_temp_yesterday'['value']],
            "min_temp_yday": curr_weather['low_temp_yesterday'['value']],
            "total_precip_yday": curr_weather['precip_yesterday'['value']],
        }
    }
    
    
    return weather_dict

def pull_weather_df(today):
    coord = ['43.74', '-79.37']

    
    ec = env_canada.ECHistorical(station_id='ON/s0000458', year=2022, month=1, language="english", timeframe='2')
    loop = asyncio.get_event_loop()
    loop.run_until_complete(ec.update())

    return ec.csv

def insert_weather(conn, weather_df):
    weather_fields = ['date', 'max_temp_yday', 'min_temp_yday', 'total_precip_yday', 'humidity', 'wind_speed', 'condition', 'text_summary', 'date_pulled']
    with conn:
        with conn.cursor() as cur:
            insert_sql = '''INSERT INTO weather.historical_daily(dt, max_temp, min_temp, mean_temp, total_rain, total_snow, total_precip) VALUES %s'''
            execute_values(cur, insert_sql, weather_df[weather_fields].values)

if __name__ == '__main__':
#def historical_upsert(conn):
   #Get current date to pull
    
    print("process start")
    today = datetime.date.today()
    forecast = pull_weather(today)
    print(forecast)

    #weather_csv = pull_weather_df(today)
    #print(weather_csv)
   
    weather_df = pd.DataFrame.from_dict(forecast)

    insert_weather(conn, weather_df)
