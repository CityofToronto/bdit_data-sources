# -*- coding: utf-8 -*-
"""
Created on Wed Oct 17 15:26:52 2018

@author: rliu4
"""

import logging
from requests import Session
from requests import exceptions
import datetime
import configparser
from psycopg2 import connect
import psycopg2
from psycopg2.extras import execute_values
import dateutil.parser
import sys
from time import sleep
import traceback
import time
import click

class WYS_APIException(Exception):
    """Base class for exceptions."""
    pass
  
class TimeoutException(Exception):
    """Exception if API gives a 504 error"""
    pass

def logger():
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.DEBUG)
    formatter=logging.Formatter('%(asctime)s     	%(levelname)s    %(message)s', datefmt='%d %b %Y %H:%M:%S')
    file_handler = logging.FileHandler('logging.log')
    file_handler.setFormatter(formatter)
    logger.handlers.clear()
    stream_handler=logging.StreamHandler()
    stream_handler.setFormatter(formatter)
    logger.addHandler(file_handler)
    logger.addHandler(stream_handler)
    with open('logging.log', 'w'):
        pass
    return logger

def get_signs(api_key):
    headers={'Content-Type':'application/json','x-api-key':api_key}
    response=session.get(url+signs_endpoint,
                         headers=headers, proxies=session.proxies)
    if response.status_code==200:
        signs=response.json()
        return signs


def get_statistics(location, start_date, api_key):
    headers={'Content-Type':'application/json','x-api-key':api_key}
    response=session.get(url+statistics_url+str(location)+date_url+str(start_date)+statistics_endpoint, 
                         headers=headers, proxies=session.proxies)
    if response.status_code==200:
        statistics=response.json()
        return statistics
    elif response.status_code==204:
        error=response.json()
        logger.error('204 error    '+error['error_message'])
    elif response.status_code==404:
        error=response.json()
        logger.error('404 error    '+error['error_message']+' or request duration invalid')
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
        sys.exit()
        
def get_intersection(location, start_date, api_key):
    headers={'Content-Type':'application/json','x-api-key':api_key}
    response=session.get(url+statistics_url+str(location)+date_url+str(start_date)+intersection_endpoint, 
                         headers=headers, proxies=session.proxies)
    if response.status_code==200:
        statistics=response.json()
        return statistics
    elif response.status_code==204:
        error=response.json()
        logger.error('204 error    '+error['error_message'])
    elif response.status_code==404:
        error=response.json()
        logger.error('404 error    '+error['error_message']+' or request duration invalid')
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
        sys.exit()
        

def get_location(location, api_key):
    headers={'Content-Type':'application/json','x-api-key':api_key}
    response=session.get(url+statistics_url+str(location)+end, 
                         headers=headers, proxies=session.proxies)
    if response.status_code==200:
        statistics=str(response.content)
        return statistics
    else:
        return response.status_code



def location_id(api_key):
    logger.debug('Pulling locations')
    error_array=[]
    try:
        signs=get_signs(api_key)
        location_id=[]
        for item in signs:
            location=item['location_id']
            sign_name=item['name']
            address=item['address']
            statistics=str(get_location(location, api_key))
            if statistics[4:11] == 'LocInfo':
                temp=[location, sign_name, address]
                location_id.append(temp)
        logger.debug(str(len(location_id))+' locations have data')
        return location_id
    except TimeoutException as exc_504:
        error_array.append(str(address)+'      '+str(exc_504)+'       {}'.format(datetime.datetime.now()))
        sleep(180)
    except exceptions.RequestException as err:
        logger.error(err)
        error_array.append(str(address)+'       '+str(err)+'      {}'.format(datetime.datetime.now()))
        sleep(75)
    except exceptions.ProxyError as prox:
        logger.error(prox)
        logger.warning('Retrying in 2 minutes')
        error_array.append(str(address)+'       '+str(prox)+'      {}'.format(datetime.datetime.now()))
        sleep(120)
    except Exception as e:
        logger.critical(traceback.format_exc())
        raise WYS_APIException(str(e))

logger=logger()
logger.debug('Start')
session = Session()
session.proxies = {'https': 'https://137.15.73.132:8080'}
url='https://api.streetsoncloud.com/sv2'
signs_endpoint = '/signs'
statistics_url='/signs/statistics/location/'
statistics_endpoint = '/from/00:00/to/23:59/speed_units/0'
intersection_endpoint = '/from/12:00/to/12:10/speed_units/0'
period='/period/'
date_url = '/date/'
units='/speed_units/0'
end='/period/5/speed_units/0'
time_delta = datetime.timedelta(days=1)
date=(datetime.datetime.today()+time_delta).strftime('%Y-%m-%d')
default_start=str(datetime.date.today()-time_delta)
default_end=str(datetime.date.today()-time_delta)
CONTEXT_SETTINGS = dict(
    default_map={'run_api': {'minutes':'1473','pull_time':'0:01', 'path':'config.cfg','location_flag': 0}}
)

@click.group(context_settings=CONTEXT_SETTINGS)
def cli():
    pass

@cli.command()
@click.option('--start', '--start_date', default=default_start, help='format is YYYY-MM-DD for start date')
@click.option('--end' ,'--end_date', default=default_end, help='format is YYYY-MM-DD for end date')
@click.option('--path' ,'--path', default='config.cfg', help='enter the path/directory of the config.cfg file')
@click.option('--location_flag' ,'--location_flag', default=0, help='enter the location_id of the sign')
def run_api(start_date, end_date, path, location_flag):
    start_date= dateutil.parser.parse(str(start_date)).date()
    end_date= dateutil.parser.parse(str(end_date)).date()
    CONFIG = configparser.ConfigParser()
    CONFIG.read(path)
    api_main(start_date, end_date, location_flag, CONFIG)

def api_main(start_date, end_date, location_flag, CONFIG):
    key=CONFIG['API']
    api_key=key['key']
    dbset = CONFIG['DBSETTINGS']
    conn = connect(**dbset)
    table=[]
    error_array=[]
    signs_list=[]
    try:
        if location_flag ==0:
            signs_list=location_id(api_key)
            signs_iterator=signs_list
        else:
            temp_list=[location_flag, None]
            signs_list.append(temp_list)
            signs_iterator=signs_list
    except Exception as e:
        logger.critical(traceback.format_exc())

    logger.debug('Pulling data')  
    while start_date<=end_date:
        logger.info('Pulling '+str(start_date))
        for signs_iterator in signs_iterator:
            location=signs_iterator[0]
            name=signs_iterator[1]
            logger.debug(str(name))
           
            for attempt in range(3):
                try:
                    statistics=get_statistics(location, start_date, api_key)
                    raw_data=statistics['LocInfo']
                    raw_records=raw_data['raw_records']
                    for item in raw_records:
                        datetime_bin=item['datetime']
                        datetime_bin= dateutil.parser.parse(str(datetime_bin))
                        counter=item['counter']
                        for item in counter:
                            temp=[location, datetime_bin, item['speed'], item['count']]
                            table.append(temp)   
                            
                except TimeoutException as exc_504:
                    error_array.append(str(name)+'      '+str(exc_504)+'       {}'.format(datetime.datetime.now()))
                    sleep(180)
                except exceptions.RequestException as err:
                    logger.error(err)
                    error_array.append(str(name)+'       '+str(err)+'      {}'.format(datetime.datetime.now()))
                    sleep(75)
                except exceptions.ProxyError as prox:
                    logger.error(prox)
                    error_array.append(str(name)+'       '+str(prox)+'      {}'.format(datetime.datetime.now()))
                    logger.warning('Retrying in 2 minutes')
                    sleep(120)
                except Exception as e:
                    logger.critical(traceback.format_exc())
                    error_array.append(str(traceback.format_exc()))
            else:
                logger.critical(traceback.format_exc())
                pass
            signs_iterator=signs_list
        start_date+=time_delta
        
        try:    
            with conn.cursor() as cur:
                logger.debug('Inserting '+str(len(table))+' rows of data')
                execute_values(cur, 'INSERT INTO wys.raw_data (api_id, datetime_bin, speed, count) VALUES %s', table)
                conn.commit()
        except psycopg2.Error as exc:
            logger.exception(exc)
            with conn:
                conn.rollback()
            sys.exit()
        except Exception as e:
            logger.critical(traceback.format_exc())
        try:
            with conn.cursor() as cur:
                    counts_15min="SELECT wys.aggregate_speed_counts_15min();"
                    cur.execute(counts_15min)
                    conn.commit()
                    logger.info('Aggregated Speed Count Data')


        except Exception as e:
            logger.critical(traceback.format_exc())
   
    loc_table=[]   
    try:
        with conn.cursor() as cur:
            string="""
                DROP TABLE IF EXISTS daily_intersections;
            
                CREATE TEMPORARY TABLE daily_intersections (
                  api_id integer NOT NULL,
                  address text,
                  sign_name text,
                  dir text,
                  start_date date);
            """
            cur.execute(str(string))
            for signs_iterator in signs_iterator:
                location=signs_iterator[0]
                statistics=get_statistics(location,start_date,api_key)
                loc_info=statistics['LocInfo']
                loc=loc_info['Location']
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
                temp=[location, address, name, direction, start_date]
                loc_table.append(temp)
                signs_iterator=signs_list
                
            execute_values(cur, 'INSERT INTO daily_intersections (api_id, address, sign_name, dir, start_date) VALUES %s', loc_table)
            string="""
                        WITH locations AS (
                        SELECT api_id, address, sign_name, dir, max(start_date)
                        FROM wys.locations 
                        GROUP BY api_id, address, sign_name, dir),
                        
                        
                        
                        differences AS (
            
                        SELECT a.api_id, a.address, a.sign_name, a.dir, start_date FROM daily_intersections A
                        LEFT JOIN locations B ON A.api_id = B.api_id
                        AND A.sign_name = B.sign_name 
                        WHERE B.sign_name IS NULL 
                        
                        UNION
                        
                        
                        SELECT a.api_id, a.address, a.sign_name, a.dir, start_date FROM daily_intersections A
                        LEFT JOIN locations B ON A.api_id = B.api_id
                        AND B.address = A.address
                        WHERE B.address IS NULL
                        )
                        
                        INSERT INTO wys.locations (api_id, address, sign_name, dir, start_date) 
                        SELECT * FROM differences
            """
            cur.execute(str(string))
            conn.commit()

    except Exception as e:
        logger.critical(traceback.format_exc())
        error_array.append(str(traceback.format_exc()))

    
    
if __name__ == '__main__':
    cli()
