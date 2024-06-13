######  prediction_import.py  ######
# Pulls weather prediction data for the following 5 days from Environment Canada.
# This script runs every day with DAG pull_weather, which pull the following 
# 5 days of prediction data and upsert to weather.prediction_daily. 

# Note: This script can not be run for backfilling as historical prediction data
# is not available. 

#Environment Canada imports
import asyncio
from types import coroutine
import env_canada

#other packages
import datetime
import pandas as pd
from psycopg2.extras import execute_values

"""
# Uncomment when running script directly
from configparser import ConfigParser
from psycopg2 import connect
CONFIG=ConfigParser()
CONFIG.read('config.cfg') # Change DB Settings in db.cfg
dbset=CONFIG['DBSETTINGS']
conn=connect(**dbset)"""

def pull_prediction(today, forecast_day):
    '''
    Pulls a week forecast from environment canada, returns forecast dict for the day after the current date
    '''
    forecast_dow_text = forecast_day.strftime("%A")
    # Station_id is Toronto, determined by looking up the csv in 
    # https://pypi.org/project/env-canada/
    ec_en = env_canada.ECWeather(station_id='ON/s0000458', language='english')
    loop = asyncio.get_event_loop()
    loop.run_until_complete(ec_en.update())

    # Reads english forecasts
    forecast = ec_en.daily_forecasts
    for day in forecast:
        if(day['period'] == forecast_dow_text):
            daytime_forecast = day
        elif (day['period'] == forecast_dow_text + ' night'):
            nighttime_forecast = day

    day_forecast = {
            "date": forecast_day,
            "max_temp": daytime_forecast['temperature'],
            "min_temp": nighttime_forecast['temperature'],
            "precip_prob_day": daytime_forecast['precip_probability'],
            "precip_prob_night": nighttime_forecast['precip_probability'],
            "text_summary_day": daytime_forecast['text_summary'],
            "text_summary_night": nighttime_forecast['text_summary'],
            "date_pulled": today
            }
    return day_forecast
    
def insert_weather(conn, weather_df):
    '''
    Inserts forecase weather to the prediction_daily table in weather schema
    '''
    weather_fields = ['date', 'max_temp', 'min_temp', 'precip_prob_day', 'precip_prob_night', 'text_summary_day', 'text_summary_night', 'date_pulled']
    with conn:
        with conn.cursor() as cur:
            insert_sql = '''INSERT INTO weather.prediction_daily
                                (dt, temp_max, temp_min, precip_prob_day, precip_prob_night, text_summary_day, text_summary_night, date_pulled)
                            VALUES %s
                            ON CONFLICT (dt)
                            DO UPDATE
                            SET (temp_max, temp_min, precip_prob_day, precip_prob_night, text_summary_day, text_summary_night, date_pulled)
                                = (EXCLUDED.temp_max, EXCLUDED.temp_min, EXCLUDED.precip_prob_day, EXCLUDED.precip_prob_night, EXCLUDED.text_summary_day, EXCLUDED.text_summary_night, EXCLUDED.date_pulled)'''
            execute_values(cur, insert_sql, weather_df[weather_fields].values)

#if __name__ == '__main__':
def prediction_upsert(cred):
    #Get current date to pull
    today = datetime.date.today()
    pull_date = today + datetime.timedelta(days=1)

    #verify connection
    conn = cred.get_conn()

    # pull 5 days of forecasts 
    for i in range(0,5):
        day_forecast = (pull_prediction(today, pull_date))
        weather_df = pd.DataFrame.from_dict([day_forecast])
        insert_weather(conn, weather_df)
        pull_date = pull_date + datetime.timedelta(days=1)

    print("Process Complete")