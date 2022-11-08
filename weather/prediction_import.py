######  prediction_import.py  ######
# Pulls the weather prediction for the following day from Environment Canada.
#


#Environment Canada imports
import asyncio
from types import coroutine
import env_canada

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
CONFIG.read('config.cfg') # Change DB Settings in db.cfg
dbset=CONFIG['DBSETTINGS']
conn=connect(**dbset)"""


#### pull_prediction(tmrw) ####
#
# Pulls a week forecast from environment canada, returns forecast dict for the day after the current date
# Params:
#     tmrwy: a valid datetime object of the day after the current date to be used in match w/ Env. Canada forecast
#
def pull_prediction(today, tmrw):

    tmrw_text = tmrw.strftime("%A")


    ec_en = env_canada.ECWeather(station_id='ON/s0000458', language='english')

    #for python 3.7+ ONLY:
    # asyncio.run(ec_en.update())

    loop = asyncio.get_event_loop()
    loop.run_until_complete(ec_en.update())

    forecast = ec_en.daily_forecasts
    for day in forecast:
        if(day['period'] == tmrw_text):
            daytime_forecast = day
        elif (day['period'] == tmrw_text + ' night'):
            nighttime_forecast = day
    
    tmrw_forecast = {
        "date": tmrw,
        "max_temp": daytime_forecast['temperature'],
        "min_temp": nighttime_forecast['temperature'],
        "precip_prob": daytime_forecast['precip_probability'],
        "text_summary_day": daytime_forecast['text_summary'],
        "text_summary_night": nighttime_forecast['text_summary'],
        "date_pulled": today
    }
    return tmrw_forecast
    


def insert_weather(conn, weather_df):
    weather_fields = ['date', 'max_temp', 'min_temp', 'precip_prob', 'text_summary_day', 'text_summary_night', 'date_pulled']
    with conn:
        with conn.cursor() as cur:
            insert_sql = '''INSERT INTO weather.prediction_daily
                            (
                                (dt, temp_max, temp_min, precip_prob, text_summary_day, text_summary_night, date_pulled)
                            ) VALUES %s
                            ON CONFLICT (dt)
                            DO UPDATE
                            SET (temp_max, temp_min, precip_prob, text_summary_day, text_summary_night, date_pulled)
                                = (EXCLUDED.temp_max, EXCLUDED.temp_min, EXCLUDED.precip_prob, EXCLUDED.text_summary_day, EXCLUDED.text_summary_night, EXCLUDED.date_pulled)'''
            execute_values(cur, insert_sql, weather_df[weather_fields].values)


#if __name__ == '__main__':
def prediction_upsert(conn, date):
    #Get current date to pull
    today = datetime.date.today()
    pull_date = today + datetime.timedelta(days=1)

    for i in range(0,5):
        day_forecast = (pull_prediction(today, pull_date))
        weather_df = pd.DataFrame.from_dict([day_forecast])
        #print("INSERTING: " + weather_df)
        insert_weather(conn, weather_df)
        pull_date = pull_date + datetime.timedelta(days=1)

    print("Process Complete")
