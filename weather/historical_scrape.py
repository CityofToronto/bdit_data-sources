# Pulls historical daily weather from Environment Canada
 
import requests
from bs4 import BeautifulSoup
import logging
from psycopg2 import sql
from psycopg2.extras import execute_values
import datetime
import pandas as pd

logger = logging.getLogger(__name__)
logging.basicConfig(level = logging.INFO)

# Uncomment when running script directly
#from configparser import ConfigParser
#from psycopg2 import connect
#CONFIG=ConfigParser()
#CONFIG.read(str(Path.home().joinpath('db.cfg')))
#dbset = CONFIG['DBSETTINGS']
#conn = connect(**dbset)

def request_url(url, payload, run_date_ds):
    '''
    Request content from Weather Canada website

    url: base URL of weather canada website
    payload: additional options applied to the url to specify what data we want to pull 
    '''
    try:
        logger.info('Scraping data from Weather Canada for %s...', str(run_date_ds))
        r = requests.get(url, params=payload)
        soup = BeautifulSoup(r.content, 'html.parser')
        return soup
    except Exception as e:
        logger.error('Failed to request url. Exception: %s', str(e))

def get_payload(run_date, stationid):
    '''
    Construct payload for url 

    run_date: Day of interested weather data
    station: station id to specify which station to pull weather data from
    '''

    year = run_date.strftime("%Y")
    month = run_date.strftime("%-m")
    day = run_date.strftime("%-d")

    if stationid == 31688:
        # Toronto City Centre
        stationname = 'Toronto City'
    elif stationid == 51459:
        # Airport
        stationname = 'Toronto INTL A'
    else:
        raise ValueError('Invalid Station ID. This function only supports pulling stationid 31688 and 51459')        
    
    payload = {
    'StationID': stationid,
    'Prov': 'ON',
    'StartYear': 2019,
    'EndYear': year,
    'selRowPerPage': 25,
    'Line': 0,
    'searchMethod': 'contains',
    'Month': month,
    'Day': day,
    'txtStationName': stationname ,
    'timeframe': 1,
    'Year': year}

    return payload

def pull_weather(run_date_ds, stationid):
    '''
    Pull weather data for specified run_date and station

    run_date: Day of interested weather data
    station: station id to specify which station to pull weather data from
    '''
    # Format Date
    run_date = datetime.datetime.strptime(run_date_ds, '%Y-%m-%d')
    url = 'https://climate.weather.gc.ca/climate_data/daily_data_e.html'
    # Use payload to query for specific day and station
    payload = get_payload(run_date, stationid)

    try:
        weather_context = request_url(url, payload, run_date_ds)

        # Construct date structure to e.g. January 1, 2017
        # to find row for specific run_date within the monthly table
        date_query = run_date.strftime("%B %-d, %Y")

        page_detail = weather_context.find(title=date_query)
        # Grab table contents for specific day
        table_content = page_detail.parent.parent.find_all("td")
        # Construct dict with values we want     
        weather_dict = { 
            "today_dict": {
                        "date": run_date_ds,
                        "max_temp": table_content[0].get_text(strip=True), # strip to get rid of white space
                        "min_temp": table_content[1].get_text(strip=True),
                        "mean_temp": table_content[2].get_text(strip=True),
                        "total_rain": table_content[5].get_text(strip=True),
                        "total_snow": table_content[6].get_text(strip=True),
                        "total_precip": table_content[7].get_text(strip=True)}
                        }
        
        rundate_data = pd.DataFrame.from_dict([weather_dict['today_dict']])
        # Replace other attributes (e.g. LegendTT, LegendMM) to empty string
        rundate_data.replace(to_replace='Legend..', value='', regex=True, inplace=True)
        # Replace empty string to None
        rundate_data = rundate_data.replace({'': None})

    except Exception as e:
        logger.error('Failed to collect historical data. Exception: %s', str(e))

    return rundate_data

def upsert_weather(conn, weather_df, stationid):

    weather_fields = ['date', 'max_temp', 'min_temp', 'mean_temp', 'total_rain', 'total_snow', 'total_precip']

    # Define schema and table name for upsert 
    schema_name = 'weather'

    if stationid == 31688:
        station_table_name = 'historical_daily_city'
    elif stationid == 51459:
        station_table_name = 'historical_daily_airport' 
    else:
        raise ValueError('Invalid Station ID. This function only supports pulling stationid 31688 and 51459') 

    with conn:
        with conn.cursor() as cur:
            upsert_sql = sql.SQL(
                '''
                INSERT INTO {table} (dt, temp_max, temp_min, mean_temp, total_rain, total_snow, total_precip)
                VALUES %s
                ON CONFLICT (dt)
                DO UPDATE
                SET (temp_max, temp_min, mean_temp, total_rain, total_snow, total_precip)
                    = (EXCLUDED.temp_max, EXCLUDED.temp_min, EXCLUDED.mean_temp, EXCLUDED.total_rain, EXCLUDED.total_snow, EXCLUDED.total_precip);
                ''').format(table = sql.Identifier(schema_name, station_table_name))
            execute_values(cur, upsert_sql, weather_df[weather_fields].values)

#if __name__ == '__main__':
def historical_upsert(cred, run_date, station_id):
    #verify connection
    conn = cred.get_conn()
    weather_dict = pull_weather(run_date, station_id)

    upsert_weather(conn, weather_dict, station_id)
    
    logger.info('Process Complete')
