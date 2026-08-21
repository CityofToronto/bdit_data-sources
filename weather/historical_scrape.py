# Pulls historical daily weather from Environment Canada
 
import logging
import datetime
import pandas as pd
import numpy as np
import asyncio
from psycopg import sql
from psycopg import connect
from env_canada import ECHistorical

logger = logging.getLogger(__name__)
logging.basicConfig(level = logging.INFO)

# Uncomment when running script directly
#from configparser import ConfigParser
#from psycopg import connect
#CONFIG=ConfigParser()
#CONFIG.read(str(Path.home().joinpath('db.cfg')))
#dbset = CONFIG['DBSETTINGS']
#conn = connect(**dbset)

def pull_weather(run_date_ds, stationid):
    '''
    Pull weather data for specified run_date and station

    run_date: Day of interested weather data
    station: station id to specify which station to pull weather data from
    '''
    # Format Date
    run_date = datetime.datetime.strptime(run_date_ds, '%Y-%m-%d')

    ec_en_csv = ECHistorical(station_id=stationid, year=run_date.year, language="english", format="csv")
    asyncio.run(ec_en_csv.update())
    df = pd.read_csv(ec_en_csv.station_data)
    df = df.replace({np.nan: None})
    row = df[df['Date/Time'] == run_date_ds]
    return row

def upsert_weather(cred, weather_df, stationid):

    weather_fields = ['Date/Time', 'Max Temp (°C)', 'Min Temp (°C)', 'Mean Temp (°C)', 'Total Rain (mm)', 'Total Snow (cm)', 'Total Precip (mm)']

    # Define schema and table name for upsert 
    schema_name = 'weather'

    if stationid == 6158355:
        station_table_name = 'historical_daily_city'
    elif stationid == 6158731:
        station_table_name = 'historical_daily_airport' 
    else:
        raise ValueError('Invalid Station ID. This function only supports pulling stationid 31688 and 51459') 

    with connect(**cred) as conn:
        with conn.cursor() as cur:
            upsert_sql = sql.SQL(
                '''
                INSERT INTO {table} (dt, temp_max, temp_min, mean_temp, total_rain, total_snow, total_precip)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (dt)
                DO UPDATE
                SET (temp_max, temp_min, mean_temp, total_rain, total_snow, total_precip)
                    = (EXCLUDED.temp_max, EXCLUDED.temp_min, EXCLUDED.mean_temp, EXCLUDED.total_rain, EXCLUDED.total_snow, EXCLUDED.total_precip);
                ''').format(table = sql.Identifier(schema_name, station_table_name))
            cur.executemany(upsert_sql, weather_df[weather_fields].values.tolist())

#if __name__ == '__main__':
def historical_upsert(cred, run_date, station_id):
    weather_dict = pull_weather(run_date, station_id)
    upsert_weather(cred, weather_dict, station_id)
    logger.info('Process Complete')
