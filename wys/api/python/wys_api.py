# -*- coding: utf-8 -*-
"""
Created on Wed Oct 17 15:26:52 2018

@author: rliu4, jchew, radumas
"""

import configparser
import datetime
import logging
import sys
import traceback
from time import sleep

import click
import json
import dateutil.parser
import psycopg2
from psycopg2 import sql
from psycopg2 import connect
from psycopg2.extras import execute_values
from requests import Session, exceptions
from requests.exceptions import RequestException

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
    response=session.get(url+signs_endpoint,
                         headers=headers)
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
    logger.debug('get_location done')

def location_id(api_key):
    ''' Using get_signs and get_location function'''
    logger.info('Pulling locations')
    for attempt in range(3):
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

def get_statistics_hourly(location, start_date, hr, api_key):
    headers={'Content-Type':'application/json','x-api-key':api_key}
    response=session.get(url+statistics_url+str(location)+'/date/'+
                        str(start_date)+'/from/'+str(hr).zfill(2)+
                        ':00/to/'+str(hr).zfill(2)+':59/speed_units/0', 
                        headers=headers)
    try:
        res = response.json()
    except json.decoder.JSONDecodeError as err:
        # catching invalid json responses `json.decoder.JSONDecodeError`
        logger.error(err + response)
        raise
    # Error handling
    if response.status_code==200:
        return res
    elif response.status_code==204:
        logger.error('204 error '+res['error_message'])
    elif response.status_code==404:
        logger.error('404 error for location %s, ' +res['error_message']+' or request duration invalid', location)
    elif response.status_code==401:
        logger.error('401 error    '+res['error_message'])
    elif response.status_code==405:
        logger.error('405 error    '+res['error_message'])        
    elif response.status_code==504:
        logger.error('504 timeout pulling sign %s for hour %s', 
                     location, hr)
        raise TimeoutException('Error'+str(response.status_code))
    else:
        raise WYS_APIException('Error'+str(response.status_code))

def get_statistics(location, start_date, api_key):
    headers={'Content-Type':'application/json','x-api-key':api_key}
    response=session.get(url+statistics_url+str(location)+'/date/'+str(start_date)+'/from/00:00/to/23:59/speed_units/0', 
                         headers=headers)
    if response.status_code==200:
        statistics=response.json()
        return statistics
    elif response.status_code==204:
        error=response.json()
        logger.error('204 error    '+error['error_message'])
    elif response.status_code==404:
        error=response.json()
        logger.error('404 error for location %s, ' +error['error_message']+' or request duration invalid', location)
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
    geocode=loc['geocode']
    address=loc['address']
    name=loc['name']
    if 'SB' in name:
        direction='SB'
    elif 'NB' in name:
        direction='NB'
    elif 'WB' in name:
        direction='WB'
    elif 'EB' in name:
        direction='EB'
    else:
        direction=None
    return (api_id, address, name, direction, start_date, geocode)

def get_data_for_date(start_date, signs_iterator, api_key):
    ''' Using get_statistics, parse_count_for_locations, parse_locations functions.
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
    # for each api_id: tuple of sign data and 24 hours to process
    sign_hr_iterator = {int(i[0]):[i,list(range(24))] for i in signs_iterator}
    # to assure each sign's location is only added once
    sign_locations_list = []
    for attempt in range(3):
        if attempt > 0:
            logger.info('Attempt %d, %d signs remaining',
                        attempt + 1,
                        len(sign_hr_iterator) )
        for sign in sign_hr_iterator.values():
            api_id=sign[0][0]
            name=sign[0][1]
            logger.debug(str(name))
            hr_iterator = sign[1]
            processed_hr = []
            for hr in hr_iterator:
                try:
                    statistics=get_statistics_hourly(api_id, start_date, hr, api_key)
                    raw_data=statistics['LocInfo']
                    raw_records=raw_data['raw_records']
                    spd_cnts = parse_counts_for_location(api_id, raw_records)
                    speed_counts.extend(spd_cnts)
                    if api_id not in sign_locations_list:
                        sign_location = parse_location(api_id, start_date, raw_data['Location'])
                        sign_locations.append(sign_location)
                        sign_locations_list.append(api_id)
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
                else:
                   # keep track of processed intervals
                   processed_hr.append(hr) 
            # only keep intervals with no data
            sign[1] = [h for h in sign[1] if h not in processed_hr]
        # only keep signs with missing data
        sign_hr_iterator = {i:sign_hr_iterator[i] for i in sign_hr_iterator if len(sign_hr_iterator[i][1]) > 0}
        # return if already got all requested data
        if sign_hr_iterator == {}:
            return speed_counts, sign_locations
    # log failures (if any)
    for api_id in sign_hr_iterator:
        logger.error('Failed to extract the data of sign #{} on {} for {} hours ({})'.format(
                    api_id, start_date, len(sign_hr_iterator[api_id][1]), 
                    ','.join(map(str,sign_hr_iterator[api_id][1]))))
    return speed_counts, sign_locations

@click.group(context_settings=CONTEXT_SETTINGS)
def cli():
    pass

@cli.command()
@click.option('-s', '--start_date', default=default_start, help='format is YYYY-MM-DD for start date')
@click.option('-e' ,'--end_date', default=default_end, help='format is YYYY-MM-DD for end date')
@click.option('--path' , default='config.cfg', help='enter the path/directory of the config.cfg file')
@click.option('--location_flag' , default=0, help='enter the location_id of the sign')

def run_api(start_date, end_date, path, location_flag):
    
    CONFIG = configparser.ConfigParser()
    CONFIG.read(path)
    api_main(start_date, end_date, location_flag, CONFIG)

def api_main(start_date=default_start,
             end_date=default_end,
             location_flag=0, CONFIG=None, conn=None, api_key=None):
    if CONFIG and not conn:
        key=CONFIG['API']
        api_key=key['key']
        dbset = CONFIG['DBSETTINGS']
        conn = connect(**dbset)

    start_date= dateutil.parser.parse(str(start_date)).date()
    end_date= dateutil.parser.parse(str(end_date)).date()

    signs_list=[]
    try:
        if location_flag ==0:
            signs_list=location_id(api_key)
        else:
            signs_list=[location_flag]
    except Exception:
        logger.critical("Couldn't parse sign parameter")
        logger.critical(traceback.format_exc())
        sys.exit(2)

    logger.debug('Pulling data')  
    while start_date<=end_date:
        logger.info('Pulling '+str(start_date))
        table, loc_table = get_data_for_date(start_date, signs_list, api_key)

        try:    
            with conn.cursor() as cur:
                logger.info('Inserting '+str(len(table))+' rows of data. Note: Insert gets roll back on error.')
                insert_sql = sql.SQL("INSERT INTO wys.raw_data(api_id, datetime_bin, speed, count) VALUES %s ON CONFLICT DO NOTHING")
                execute_values(cur, insert_sql, table)
                
        except psycopg2.Error as exc:
            logger.critical('Error inserting speed count data')
            logger.critical(exc)
            sys.exit(1)

        try:
            with conn.cursor() as cur:
                cur.execute("SELECT wys.aggregate_speed_counts_one_hour_5kph(%s, %s);", (start_date, start_date + datetime.timedelta(days=1)))
                logger.info('Aggregated Speed Count Data')
        except psycopg2.Error as exc:
            logger.critical('Error aggregating data to 1-hour bins')
            logger.critical(exc)
            conn.close()
            sys.exit(1)
        
        start_date+=time_delta
        conn.commit()

    try:
        update_locations(conn, loc_table)
        logger.info('Updated wys.locations')
    except psycopg2.Error as exc:
        logger.critical('Error updating locations')
        logger.critical(exc)
        conn.close()
        sys.exit(1)

    conn.commit()

    logger.info('Done')

def update_locations(conn, loc_table):
    '''Update the wys.locations table for the date of data collection

    Parameters
    ------------
    con : SQL connection object
        Connection object needed to connect to the RDS
    loc_table: list
        List of rows representing each active sign to be inserted or updated
    '''
    with conn.cursor() as cur:
        create_temp_table="""
            DROP TABLE IF EXISTS daily_intersections;
        
            CREATE TEMPORARY TABLE daily_intersections (
                api_id integer NOT NULL,
                address text,
                sign_name text,
                dir text,
                start_date date,
                loc text,
                geom geometry);
        """
        cur.execute(create_temp_table)
        execute_values(cur, 'INSERT INTO daily_intersections (api_id, address, sign_name, dir, start_date, loc) VALUES %s', loc_table)
        calc_geom_temp_table = """
            UPDATE daily_intersections
            SET geom = ST_Transform(ST_SetSRID(ST_MakePoint(split_part(regexp_replace(loc, '[()]'::text, ''::text, 'g'::text), ','::text, 2)::double precision, 
                                                            split_part(regexp_replace(loc, '[()]'::text, ''::text, 'g'::text), ','::text, 1)::double precision), 
                                               4326),
                                    2952);
            """
        cur.execute(calc_geom_temp_table)
        update_locations_sql="""
            -- most recent record of each sign
            WITH locations AS (
                SELECT DISTINCT ON(api_id) api_id, address, sign_name, dir, loc, start_date, geom, id
                FROM wys.locations 
                ORDER BY api_id, start_date DESC
            )
            -- New and moved signs
            , differences AS (
                SELECT a.api_id, a.address, a.sign_name, a.dir, a.start_date, 
                       a.loc, a.geom 
                FROM daily_intersections A
                LEFT JOIN locations B ON A.api_id = B.api_id
                WHERE
                    st_distance(A.geom, B.geom) > 100 -- moved more than 100m
                    OR A.dir <> B.dir -- changed direction
                    OR A.api_id NOT IN (SELECT api_id FROM locations) -- new sign
            )
            -- Insert new & moved signs into wys.locations
            , new_signs AS (
                INSERT INTO wys.locations (api_id, address, sign_name, dir, start_date, loc, geom)
                SELECT * FROM differences
            )
            -- Signs with new name and/or address
            , updated_signs AS (
                SELECT a.api_id, a.address, a.sign_name, a.dir, a.loc, a.start_date, 
                       a.geom, b.id 
                FROM daily_intersections A
                LEFT JOIN locations B ON A.api_id = B.api_id
                                      AND st_distance(A.geom, B.geom) < 100
                                      AND A.dir = B.dir
                                      AND (A.sign_name <> B.sign_name
                                        OR A.address <> B.address)
            )
            -- Update name and/or address
            UPDATE wys.locations 
                SET api_id = b.api_id,
                    address = b.address,
                    sign_name = b.sign_name,
                    dir = b.dir,
                    start_date = b.start_date,
                    loc = b.loc,
                    id = b.id,
                    geom = b.geom
                FROM updated_signs b
                WHERE locations.id = b.id            
        """
        cur.execute(update_locations_sql)

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
