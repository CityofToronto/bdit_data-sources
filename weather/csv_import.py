###################
#csv_import.py
####################
# Takes downloaded csv files of historical weather data
# from Environment Canada and inserts them into the historical airport
# data table.
# Link to download: https://climate.weather.gc.ca/historical_data/search_historic_data_e.html 

# MODIFY THE DESTINATION TABLE WHEN IMPORTING ANOTHER DATASET

import os
import pandas as pd
import numpy as np
from configparser import ConfigParser
from psycopg2 import connect, sql
from psycopg2.extras import execute_values

CONFIG=ConfigParser()
CONFIG.read('config.cfg') # Change DB Settings in db.cfg
dbset=CONFIG['DBSETTINGS']
conn=connect(**dbset)

# Weather file path (In Excel format)
DIRECTORY = "weather_csv"

def insert_weather(conn, weather_df):
    weather_fields = ['date', 'max_temp', 'min_temp', 'mean_temp', 'total_rain', 'total_snow', 'total_precip']
    with conn:
        with conn.cursor() as cur:
            insert_sql = '''INSERT INTO weather.historical_daily_airport(dt, temp_max, temp_min, mean_temp, total_rain, total_snow, total_precip) VALUES %s'''
            execute_values(cur, insert_sql, weather_df[weather_fields].values)

if __name__ == '__main__':

    for filename in os.listdir(DIRECTORY):
        f = os.path.join(DIRECTORY, filename)
        weather_df = pd.read_excel(f)
        weather_df = (weather_df.rename(columns={
            'Date/Time': 'date',
            'Max Temp (°C)': 'max_temp',
            'Min Temp (°C)': 'min_temp',
            'Mean Temp (°C)': 'mean_temp',
            'Total Rain (mm)': 'total_rain',
            'Total Snow (cm)': 'total_snow',
            'Total Precip (mm)': 'total_precip'})
            .replace({np.nan: None}))

        insert_weather(conn, weather_df)
        print("inserted "+ f)

    