######  mrc_import.py  ######
# Pulls daily weather forecast from Environment Canada.
# 
# Original script from https://github.com/Toronto-Big-Data-Innovation-Team/activeto/blob/jasonlee/weekend_closures/scripts/import_weather.py

#Environment Canada imports
import asyncio
from types import coroutine
import env_canada
#from env_canada import ECHistoricalRange, get_historical_stations

#website scraping
import requests
from requests import exceptions
from pathlib import Path
from bs4 import BeautifulSoup

#logger
import logging
from logging import exception

#sql
from psycopg2 import connect, sql
from psycopg2.extras import execute_values

#other packages
import os
import sys
import datetime
import pandas as pd
import numpy as np
import click
from configparser import ConfigParser

logger = logging.getLogger(__name__)
logging.basicConfig(level = logging.INFO)


"""CONFIG=ConfigParser()
CONFIG.read('config.cfg') # Change DB Settings in db.cfg
dbset=CONFIG['DBSETTINGS']
conn=connect(**dbset)"""


def request_url(url, payload):
    '''
    Request content from Weather Canada website
    '''
    try:
        logger.info('Scraping data from Weather Canada for %s...', str(run_date))
        r = requests.get(url, params=payload)
        soup = BeautifulSoup(r.content, 'html.parser')
        return soup
    except Exception as e:
        logger.error('Failed to request url. Exception: %s', str(e))

def get_payload(run_date, station):
    year = run_date.strftime("%Y")
    month = run_date.strftime("%m")
    day = run_date.strftime("%d")

    if station == 1:
        stationid = 54239
        stationname = 'Toronto'
    else:
        stationid = 51459
        stationname = 'YYZ'
    
    payload = {
    'StationID': 54239,
    'Prov': 'ON',
    'StartYear': 2019,
    'EndYear': year,
    'selRowPerPage': 25,
    'Line': 0,
    'searchMethod': 'contains',
    'Month': month,
    'Day': day,
    'txtStationName': 'Toronto',
    'timeframe': 1,
    'Year': year}

    return payload

def pull_weather(run_date, station):
    
    '''
    ec_en = env_canada.ECWeather(station_id='ON/s0000458', language='english')

    #for python 3.7+ ONLY:
    # asyncio.run(ec_en.update())

    loop = asyncio.get_event_loop()
    loop.run_until_complete(ec_en.update())
    curr_weather = ec_en.conditions
    '''
    url = 'https://climate.weather.gc.ca/climate_data/daily_data_e.html'
    payload = get_payload(run_date, station)


    #month_url = 'https://climate.weather.gc.ca/climate_data/daily_data_e.html?hlyRange=2013-06-11%7C2023-02-22&dlyRange=2013-06-13%7C2023-02-22&mlyRange=%7C&StationID=51459&Prov=ON&urlExtension=_e.html&searchType=stnName&optLimit=yearRange&StartYear=2020&EndYear=2023&selRowPerPage=25&Line=3&searchMethod=contains&Month=2&Day=22&txtStationName=toronto&timeframe=2&Year=2023'
    try:
        weather_context = request_url(url, payload)
        tmr_date = (run_date + datetime.timedelta(days=1)).strftime("%Y-%m-%d")

        date_query = run_date.strftime("")
        #find day in weather table
        days = []
        abbr_contents = weather_context.find_all("abbr")
        days.append(abbr_contents[0].text)

        for i in abbr_contents[1:]:
            data = []
            if day_contents = i.find(title=date_query):
                #add all <td> elements into data[]
                td_contents = day_contents.find_all("td")
                data.append(td_contents[0].text)


                
        weather_dict = { 
            "today_dict": {
                "date": today,
                "max_temp": data[0],
                "min_temp": data[1],
                "mean_temp": data[2],
                "total_rain": data[5],
                "total_snow": data[6],
                "total_precip": data[7],
                "date_pulled": today
        }
    }

        logger.info('''\nToday %s: \nMaxTemp: \nMinTemp: %s, \nMeanTemp: %s''', 
                        str(run_date),
                        weather_dict['max_temp'],
                        weather_dict['min_temp'],
                        weather_dict['mean_temp'] 
                        )
            

    except Exception as e:
        logger.error('Failed to collect historical data. Exception: %s', str(e))

    return weather_dict


def pull_weather_df(today):
    #coord = ['43.74', '-79.37']

    
    ec = env_canada.ECHistorical(station_id='ON/s0000458', year=2022, month=1, language="english", timeframe='2')
    loop = asyncio.get_event_loop()
    loop.run_until_complete(ec.update())

    return ec.csv

def insert_weather(conn, weather_df):
    weather_fields = ['date', 'max_temp', 'min_temp', 'mean_temp', 'total_rain', 'total_snow', 'total_precip']
    with conn:
        with conn.cursor() as cur:
            insert_sql = '''INSERT INTO weather.historical_daily_city(dt, temp_max, temp_min, mean_temp, total_rain, total_snow, total_precip) VALUES %s'''
            execute_values(cur, insert_sql, weather_df[weather_fields].values)

def upsert_weather(conn, weather_df):
    weather_fields = ['date', 'max_temp', 'min_temp', 'mean_temp', 'total_rain', 'total_snow', 'total_precip']
    with conn:
        with conn.cursor() as cur:
            upsert_sql = ''' INSERT INTO weather.historical_daily_city
                                (dt, temp_max, temp_min, mean_temp, total_rain, total_snow, total_precip)
                            VALUES %s
                            ON CONFLICT (dt)
                            DO UPDATE
                            SET (temp_max, temp_min, mean_temp, total_rain, total_snow, total_precip)
                                = (EXCLUDED.temp_max, EXCLUDED.temp_min, EXCLUDED.mean_temp, EXCLUDED.total_rain, EXCLUDED.total_snow, EXCLUDED.total_precip); '''
            execute_values(cur, upsert_sql, weather_df[weather_fields].values)

#if __name__ == '__main__':
def historical_upsert(cred, run_date):
    #Get current date to pull

    #use connection
    conn = cred.get_conn()
    
    print("process start")
    weather_dict = pull_weather(run_date, station = 1)

    #weather_csv = pull_weather_df(today)
    #print(weather_csv)

    today_df = pd.DataFrame.from_dict([weather_dict['today_dict']])
    upsert_weather(conn, yday_df)
    
    print("Process Complete")

