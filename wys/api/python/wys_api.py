# -*- coding: utf-8 -*-
"""
Created on Wed Oct 17 15:26:52 2018

@author: rliu4, jchew, radumas
"""

import os
import datetime
import logging
import sys
import traceback
from time import sleep
from re import findall

import click
import json
import dateutil.parser
import psycopg2
from psycopg2 import sql
from psycopg2.extras import execute_values
from requests import Session, exceptions
from requests.exceptions import RequestException

from airflow.hooks.base import BaseHook
from airflow.providers.postgres.hooks.postgres import PostgresHook

SQL_DIR = os.path.join(os.path.dirname(os.path.abspath(os.path.dirname(__file__))), 'sql')

class WYS_APIException(Exception):
    """Base class for exceptions."""
    pass

class TimeoutException(Exception):
    """Exception if API gives a 504 error"""
    pass

def logger():
    #logging.basicConfig(format='%(asctime)s line %(lineno)d [%(levelname)s]: %(message)s')
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.INFO)
    return logger

logger=logger()
logger.debug('Start')
session = Session()
url='https://api.streetsoncloud.com/sv2'
signs_endpoint = '/signs'
schedule_endpoint = '/schedules'
statistics_url='/signs/statistics/location/'

time_delta = datetime.timedelta(days=1)
date=(datetime.datetime.today()+time_delta).strftime('%Y-%m-%d')
default_start=str(datetime.date.today()-time_delta)
default_end=str(datetime.date.today()-time_delta)
CONTEXT_SETTINGS = dict(
    default_map={'run_api': {'minutes':'1473','pull_time':'0:01', 'path':'config.cfg','location_flag': 0}}
)

def get_signs(api_key):
    headers={'Content-Type':'application/json','x-api-key':api_key}
    response=session.get(url+signs_endpoint, headers=headers)
    if response.status_code==200:
        signs=response.json()
        return signs
    logger.debug('get_signs done')

def get_location(location, api_key):
    headers={'Content-Type':'application/json','x-api-key':api_key}
    response=session.get(url+statistics_url+str(location)+'/period/5/speed_units/0', 
                         headers=headers)
    if response.status_code==200:
        statistics=str(response.content)
        return statistics
    else:
        return response.status_code

def location_id(api_key):
    ''' Using get_signs and get_location function'''
    logger.info('Pulling locations')
    for _ in range(3):
        try:
            signs=get_signs(api_key)
            location_id=[]
            for item in signs:
                location=item['location_id']
                sign_name=item['name']
                address=item['address']
                statistics=str(get_location(location, api_key))
                #logger.debug('DONE one item')
                if statistics[4:11] == 'LocInfo':
                    temp=[location, sign_name, address]
                    location_id.append(temp)
            logger.info(str(len(location_id))+' locations have data')
            return location_id
        except TimeoutException as exc_504:
            logger.exception(exc_504)
            sleep(180)
        except exceptions.ProxyError as prox:
            logger.error(prox)
            logger.warning('Retrying in 2 minutes')
            sleep(120)
        except exceptions.RequestException as err:
            logger.error(err)
            sleep(75)
        except Exception as e:
            logger.critical(traceback.format_exc())
            raise WYS_APIException(str(e))
    logger.info('location_id done')

def get_statistics_date(location, start_date, api_key):
    headers={'Content-Type':'application/json','x-api-key':api_key}
    response=session.get(url+statistics_url+str(location)+'/date/'+str(start_date)+'/speed_units/0', 
                         headers=headers)
    if response.status_code==200:
        statistics=response.json()
        return statistics
    elif response.status_code==204:
        error=response.json()
        logger.error('204 error    '+error['error_message'])
    elif response.status_code==404:
        error=response.json()
        logger.error('404 error for location %s, ' +error['error_message'], location)
    elif response.status_code==401:
        error=response.json()
        logger.error('401 error    '+error['error_message'])
    elif response.status_code==405:
        error=response.json()
        logger.error('405 error    '+error['error_message'])        
    elif response.status_code==504:
        error=response.json()
        logger.error('504 error')
        raise TimeoutException('Error'+str(response.status_code))
    else:
        raise WYS_APIException('Error'+str(response.status_code))

def parse_counts_for_location(api_id, raw_records):
    '''Parse the response for a given location and set of records

    Parameters
    ------------
    api_id : int
        Unique identifier for the sign.
    raw_records : list
        List of records returned for location and time.

    Returns 
    --------
    speed_counts
        List of speed count records to insert into database
    '''
    speed_counts = []
    for record in raw_records:
        datetime_bin=record['datetime']
        datetime_bin= dateutil.parser.parse(str(datetime_bin))
        counter=record['counter']
        for item in counter:
            this_speed=int(item['speed']) if item['speed'] else None
            this_count=int(item['count']) if item['count'] else None
            speed_row=[api_id, datetime_bin, this_speed, this_count]
            speed_counts.append(speed_row)
    return speed_counts

def parse_location(api_id, start_date, loc):
    '''Parse the location data for a given sign

    Parameters
    ------------
    api_id : int 
        Unique identifier for the sign.
    start_date : date 
        Date being processed.
    loc : dict 
        Dictionary of location information for the sign.

    Returns
    --------
    row
        Tuple representing a row of the wys.locations table

    '''
    geocode=loc.get('geocode')
    address=loc.get('address')
    name=loc.get('name')
    try:
        direction = findall("SB|NB|WB|EB", name)[0]
    except IndexError:
        direction = None
    return (api_id, address, name, direction, start_date, geocode)

def get_data_for_date(start_date, api_ids, api_key, conn):
    ''' Using get_statistics_date, parse_count_for_locations, parse_locations functions.
    Pull data for the provided date and list of signs to pull

    Parameters
    -----------
    start_date : date
        Date to pull data for
    signs_iterator : list
        List of api_id's (signs) to pull data from
    api_key : str
        Key to pull data from the api
    Returns
    --------
    speed_counts: list
        List of speed count rows to be inserted into wys.raw_data
    sign_locations: list
        List of active sign locations to be inserted into wys.locations
    '''
    speed_counts, sign_locations = [], []
    count = len(api_ids)
    for api_id in api_ids:
        try:
            logger.info('Pulling sign %s (# %s / %s).', api_id, api_ids.index(api_id), count)
            statistics=get_statistics_date(api_id, start_date, api_key)
            raw_data=statistics['LocInfo']
            raw_records=raw_data['raw_records']
            spd_cnts = parse_counts_for_location(api_id, raw_records)
            speed_counts.extend(spd_cnts)
            sign_location = parse_location(api_id, start_date, raw_data['Location'])
            sign_locations.append(sign_location)
        except TimeoutException:
            sleep(180)
        except exceptions.ProxyError as prox:
            logger.error(prox)
            logger.warning('Retrying in 2 minutes')
            sleep(120)
        except exceptions.RequestException as err:
            logger.error(err)
            sleep(75)
        except Exception as err:
            logger.error(err)
           
    try:
        with conn.cursor() as cur:
            logger.info('Inserting '+str(len(speed_counts))+' rows of data. Note: Insert gets roll back on error.')
            delete_sql = sql.SQL("""
                DELETE FROM wys.raw_data
                WHERE datetime_bin >= {}::timestamp AND datetime_bin < {}::timestamp + interval '1 day';
            """)
            delete_sql = delete_sql.format(sql.Literal(str(start_date)), sql.Literal(str(start_date)))
            cur.execute(delete_sql)
            insert_sql = sql.SQL("""
                INSERT INTO wys.raw_data (api_id, datetime_bin, speed, count)
                VALUES %s
                ON CONFLICT (datetime_bin, api_id, speed)
                DO NOTHING;
            """)
            execute_values(cur, insert_sql, speed_counts)
    except psycopg2.Error as exc:
        logger.critical('Error inserting speed count data')
        logger.critical(exc)
        sys.exit(1)

    return sign_locations

@click.group(context_settings=CONTEXT_SETTINGS)
def cli():
    pass

@cli.command()
@click.option('-s', '--start_date', default=default_start, help='format is YYYY-MM-DD for start date')
@click.option('-e' ,'--end_date', default=default_end, help='format is YYYY-MM-DD for end date')
@click.option('--location_flag' , default=0, help='enter the location_id of the sign')

def run_api(start_date, end_date, location_flag):
    api_key = get_api_key()
    wys_postgres = PostgresHook("wys_bot")
    conn = wys_postgres.get_conn()
    
    start_date = dateutil.parser.parse(str(start_date)).date()
    end_date = dateutil.parser.parse(str(end_date)).date()
    signs_list = get_sign_list(location_flag)

    logger.debug('Pulling data')

    while start_date<=end_date:
        logger.info('Pulling %s', str(start_date))
        get_data_for_date(start_date, signs_list, api_key)
        agg_1hr_5kph(start_date, start_date + time_delta, conn)
        start_date+=time_delta

    logger.info('Done')

def get_sign_list(location_flag = 0):
    api_key = get_api_key()
    signs_list=[]
    try:
        if location_flag == 0:
            signs_list=location_id(api_key)
        else:
            signs_list=[location_flag]
    except Exception:
        logger.critical("Couldn't parse sign parameter")
        logger.critical(traceback.format_exc())
        sys.exit(2)
    return signs_list

def get_api_key():
    #api connection
    api_conn = BaseHook.get_connection('wys_api_key')
    return api_conn.password

def agg_1hr_5kph(start_date, end_date, conn):
    try:
        with conn.cursor() as cur:
            params = (start_date, end_date)
            cur.execute("SELECT wys.clear_speed_counts_agg_5kph(%s, %s);", params)
            cur.execute("SELECT wys.aggregate_speed_counts_one_hour_5kph(%s, %s);", params)
            logger.info('Aggregated Speed Count Data')
    except psycopg2.Error as exc:
        logger.critical('Error aggregating data to 1-hour bins')
        logger.critical(exc)
        conn.close()
        sys.exit(1)

def update_locations(loc_table, conn):
    '''Update the wys.locations table for the date of data collection

    Parameters
    ------------
    conn : SQL connection object
        Connection object needed to connect to the RDS
    loc_table: list
        List of rows representing each active sign to be inserted or updated
    '''
    logger.info('Updating wys.locations')
    fpath = os.path.join(SQL_DIR, 'create-temp-table-daily_insersections.sql')
    with open(fpath, 'r', encoding='utf-8') as file:
        daily_intersections_sql = sql.SQL(file.read())

    update_fpath = os.path.join(SQL_DIR, 'select-update_locations.sql')
    with open(update_fpath, 'r', encoding='utf-8') as file:
        update_locations_sql = sql.SQL(file.read())

    with conn.cursor() as cur:
        execute_values(cur, daily_intersections_sql, loc_table)
        logger.info('Create and populated daily_intersections temp table.')
        cur.execute(update_locations_sql)
        logger.info('Finished updating intersections.')

def get_schedules(conn, api_key):
    headers={'Content-Type':'application/json','x-api-key':api_key}
    
    try:
        response=session.get(url+signs_endpoint+schedule_endpoint,
                         headers=headers)
        response.raise_for_status()
        schedule_list = response.json()
    except RequestException as exc: 
        logger.critical('Error querying API, %s', exc)
        logger.critical('Response: %s', exc.response)

    try:
        rows = [(schedule['name'], api_id)
                    for schedule in schedule_list if schedule['assigned_on_locations']
                        for api_id in schedule['assigned_on_locations'] if api_id]
    except TypeError as e:
        logger.critical('Error converting schedules response to values list.')
        logger.critical('Return value: %s', schedule_list)
        raise WYS_APIException(e)

    with conn.cursor() as cur:
        logger.debug('Inserting '+str(len(rows))+' rows of schedules')
        schedule_sql = '''
            INSERT INTO wys.sign_schedules_list (schedule_name, api_id) VALUES %s
            ON CONFLICT (api_id) DO UPDATE SET
            schedule_name = EXCLUDED.schedule_name
            '''
        execute_values(cur, schedule_sql, rows)

if __name__ == '__main__':
    cli()