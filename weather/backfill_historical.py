'''
Python script to backfill historical data for one station between 
a specified range of dates
'''

import os
import sys
from datetime import datetime, timedelta, date
from configparser import ConfigParser
from historical_scrape import pull_weather, upsert_weather
from psycopg2 import connect, sql
from pathlib import Path
import click

CONFIG=ConfigParser()
CONFIG.read(str(Path.home().joinpath('db.cfg')))
dbset = CONFIG['DBSETTINGS']
conn = connect(**dbset)

@click.command()
@click.option('--start_dt', '-s', type = str, required = True, 
                help = '''Start date (text), inclusive lower bound. e.g. '2019-01-01' ''')
@click.option('--end_dt', '-e', type = str, required = True
                , help = '''End date (text), exclusive lower bound. e.g. '2019-02-01' ''')
@click.option('--station_id', '-i', type = int, required = True
                , help = 'Station Id, toronto city centre = 31688, airport = 51459')
def backfill_historical(start_dt, end_dt, station_id):
    start_date = datetime.strptime(start_dt, '%Y-%m-%d')
    end_date = datetime.strptime(end_dt, '%Y-%m-%d')  - timedelta(days=1) ## Exclusive upper bound

    # difference between current and previous date
    delta = timedelta(days=1)

    # store the dates between two dates in a list
    dates = []

    while start_date <= end_date:
        # add current date to list by converting  it to iso format
        dates.append(start_date.strftime("%Y-%m-%d"))
        # increment start date by timedelta
        start_date += delta
    
    # Pull historical data
    for i in dates:
        weather_dict = pull_weather(i, station = station_id)
        upsert_weather(conn, weather_dict, station_id)
        
if __name__ == '__main__':
    backfill_historical()        