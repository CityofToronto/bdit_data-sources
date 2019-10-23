# -*- coding: utf-8 -*-
"""
Created on Wed Jun 13 10:15:56 2018

@author: rliu4
"""
import sys
import json
from requests import Session
from requests import exceptions
import datetime
import pytz
import dateutil.parser
import psycopg2
from psycopg2.extras import execute_values
from psycopg2 import connect, Error
import logging
import configparser
import click
import traceback
from time import sleep

class MiovisionAPIException(Exception):
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

logger=logger()
logger.debug('Start')
time_delta = datetime.timedelta(days=1)
default_start=str(datetime.date.today()-time_delta)
default_end=str(datetime.date.today())
local_tz=pytz.timezone('US/Eastern')
session = Session()
session.proxies = {}
url='https://api.miovision.com/intersections/'
tmc_endpoint = '/tmc'
ped_endpoint='/crosswalktmc'


CONTEXT_SETTINGS = dict(
    default_map={'run_api': {'flag': 0}}
)

@click.group(context_settings=CONTEXT_SETTINGS)
def cli():
    pass

@cli.command()
@click.option('--start_date', '--start_date', default=default_start, help='format is YYYY-MM-DD for start date')
@click.option('--end_date' ,'--end_date', default=default_end, help='format is YYYY-MM-DD for end date')
@click.option('--path' ,'--path', default='config.cfg', help='enter the path/directory of the config.cfg file')
@click.option('--intersection' ,'--intersection', default=0, help='enter the intersection_uid of the intersection')
@click.option('--pull' ,'--pull', default=None, help='enter 1 to not process the data')


def run_api(start_date, end_date, path, intersection, pull):

    CONFIG = configparser.ConfigParser()
    CONFIG.read(path)
    api_key=CONFIG['API']
    key=api_key['key']
    dbset = CONFIG['DBSETTINGS']
    conn = connect(**dbset)
    conn.autocommit = True
    logger.debug('Connected to DB')

    start_date= dateutil.parser.parse(str(start_date))
    end_date= dateutil.parser.parse(str(end_date))
    start_time=local_tz.localize(start_date)
    end_time=local_tz.localize(end_date)
    logger.info('Pulling from %s to %s' %(str(start_time),str(end_time)))
    
    try:
        pull_data(conn, start_time, end_time, intersection, path, pull, key)
    except Exception as e:
        logger.critical(traceback.format_exc())


def get_movement(item):
    if (item['entrance'] == 'N' and item['exit'] =='S'):
        return '1'
    elif item['entrance'] == 'S' and item['exit'] =='N':
        return '1'
    elif item['entrance'] == 'W' and item['exit'] =='E':
        return '1'
    elif item['entrance'] == 'E' and item['exit'] =='W':
        return '1'
    elif item['entrance'] == 'S' and item['exit'] =='W':
        return '2'
    elif item['entrance'] == 'N' and item['exit'] =='E':
        return '2'
    elif item['entrance'] == 'W' and item['exit'] =='N':
        return '2'
    elif item['entrance'] == 'E' and item['exit'] =='S':
        return '2'
    elif item['entrance'] == 'S' and item['exit'] =='E':
        return '3'
    elif item['entrance'] == 'E' and item['exit'] =='N':
        return '3'
    elif item['entrance'] == 'N' and item['exit'] =='W':
        return '3'
    elif item['entrance'] == 'W' and item['exit'] =='S':
        return '3'
    else:
        return '4'

def get_crosswalk(item):
    if (item['direction'] == 'CW'):
        return '5'
    else:
        return '6'
    
def get_classification(item):
    if (item['class'] == 'Pedestrian'):
        return '6'
    elif item['class'] == 'Light':
        return '1'
    elif item['class'] == 'Bicycle':
        return '2'
    elif item['class'] == 'Bus':
        return '3'
    elif item['class'] == 'SingleUnitTruck':
        return '4'
    elif item['class'] == 'ArticulatedTruck':
        return '5'
    elif item['class'] == 'WorkVan':
        return '8'
    else:
        return '0'

def get_intersection_tmc(table, start_time, end_iteration_time, intersection_id1, intersection_uid, key):
    headers={'Content-Type':'application/json','Authorization':key}
    params = {'endTime': end_iteration_time, 'startTime' : start_time}
    response=session.get(url+intersection_id1+tmc_endpoint, params=params, 
                         headers=headers, proxies=session.proxies)
    if response.status_code==200:
        tmc=json.loads(response.content.decode('utf-8'))
        for item in tmc:
            
            item['classification']=get_classification(item)
            item['volume']=item.pop('qty')
            item['movement']=get_movement(item)
            item['leg']=item.pop('entrance')
         
            temp=[intersection_uid, item['timestamp'], item['classification'], item['leg'], item['movement'], item['volume']]
            table.append(temp)

        return table
    elif response.status_code==404:
        error=json.loads(response.content)
        logger.error(error['error'])
    elif response.status_code==400:
        error=json.loads(response.content)
        logger.error(error['error'])
    elif response.status_code==504:
        raise TimeoutException('Error'+str(response.status_code))
    else:
        raise MiovisionAPIException('Error'+str(response.status_code))


def get_pedestrian(table, start_time, end_iteration_time, intersection_id1, intersection_uid, key):
    headers={'Content-Type':'application/json','Authorization':key}
    params = {'endTime': end_iteration_time, 'startTime' : start_time}
    
    response=session.get(url+intersection_id1+ped_endpoint, params=params, 
                         headers=headers, proxies=session.proxies)
    if response.status_code==200:
        ped=json.loads(response.content.decode('utf-8'))
        for item in ped:
            
            item['classification']='6'
            item['volume']=item.pop('qty')
            item['movement']=get_crosswalk(item)
            item['leg']=item.pop('crosswalkSide')
            item['exit_dir_name']=None

            temp=[intersection_uid, item['timestamp'], item['classification'], item['leg'],  item['movement'], item['volume']]
            table.append(temp)
            
        return table
    elif response.status_code==404:
        error=json.loads(response.content)
        logger.error(error['error'])
    elif response.status_code==400:
        error=json.loads(response.content)
        logger.error(error['error'])
    elif response.status_code==504:
        raise TimeoutException('Error'+str(response.status_code))
    else:
        raise MiovisionAPIException('Error'+str(response.status_code))

def process_data(conn, pull, start_time, end_iteration_time):
    if pull is None:
        try:
            with conn:
                with conn.cursor() as cur:
                    update="SELECT miovision_api.aggregate_15_min_tmc('"+str(start_time.strftime('%Y-%m-%d'))+"','"+str(end_iteration_time.strftime('%Y-%m-%d'))+"')"
                    cur.execute(update)
                    logger.info('Aggregated to 15 minute bins')

                    atr_aggregation="SELECT miovision_api.aggregate_15_min('"+str(start_time.strftime('%Y-%m-%d'))+"','"+str(end_iteration_time.strftime('%Y-%m-%d'))+"')"            
                    cur.execute(atr_aggregation)
                    logger.info('Completed data processing for {}'.format(start_time))

                    missing_dates_query="SELECT jchew.missing_dates('{}'::date)".format(start_time.strftime('%Y-%m-%d'))
                    cur.execute(missing_dates_query)
                    #need to switch back to miovision_api function once permission is no longer denied
                    logger.info('missing_dates_query done')
            
        except psycopg2.Error as exc:
            logger.exception(exc)
            sys.exit()
    else:
        logger.info('Data Processing Skipped')

    with conn:
        with conn.cursor() as cur:
            report_dates="SELECT miovision_api.report_dates('"+str(start_time.strftime('%Y-%m-%d'))+"','"+str(end_iteration_time.strftime('%Y-%m-%d'))+"');"
            cur.execute(report_dates)
            logger.info('report_dates done')

def refresh_views(conn):
    try:
        with conn:
            with conn.cursor() as cur:
                refresh_volumes_class='''REFRESH MATERIALIZED VIEW miovision_api.volumes_15min_by_class WITH DATA;'''
                cur.execute(refresh_volumes_class)
                logger.info('Refreshed m.view for volumes_15min_by_class')

                refresh_volumes='''REFRESH MATERIALIZED VIEW miovision_api.report_volumes_15min WITH DATA;'''
                cur.execute(refresh_volumes)
                logger.info('Refreshed m.view for report_volumes_15min')

                refresh_report_daily='''REFRESH MATERIALIZED VIEW miovision_api.report_daily WITH DATA;'''
                cur.execute(refresh_report_daily)
                logger.info('Refreshed m.view for report_daily')

    except psycopg2.Error as exc:
        logger.exception('Cannot Refresh Views')
        sys.exit()

def insert_data(conn, start_time, end_iteration_time, table):
    with conn:
        with conn.cursor() as cur:
            insert_data = '''INSERT INTO miovision_api.volumes (intersection_uid, datetime_bin, classification_uid, 
                             leg,  movement_uid, volume) VALUES %s'''
            execute_values(cur, insert_data, table)

    with conn:
        with conn.cursor() as cur:
            api_log="SELECT miovision_api.api_log('"+str(start_time.strftime('%Y-%m-%d'))+"','"+str(end_iteration_time.strftime('%Y-%m-%d'))+"')"
            cur.execute(api_log)

    logger.info('Inserted into volumes and updated log') 
    
    conn.notices=[]
    with conn:
        with conn.cursor() as cur: 
            invalid_movements="SELECT miovision_api.find_invalid_movements('"+str(start_time.strftime('%Y-%m-%d'))+"','"+str(end_iteration_time.strftime('%Y-%m-%d'))+"')"
            cur.execute(invalid_movements)
            invalid_flag=cur.fetchone()[0]

            logger.info(conn.notices[0]) 

def pull_data(conn, start_time, end_time, intersection, path, pull, key):

    time_delta = datetime.timedelta(days=1)
    end_iteration_time= start_time + time_delta    

    if int(intersection) > 0:
        with conn.cursor() as cur: 
            string="SELECT * from miovision_api.intersections WHERE intersection_uid = %s"
            cur.execute(string, str(intersection))
            intersection_list=cur.fetchall()
            logger.debug(intersection_list)
    else: 
        with conn.cursor() as cur: 
            string2="SELECT * from miovision_api.intersections"
            cur.execute(string2)
            intersection_list=cur.fetchall()
            logger.debug(intersection_list)

    while True:
        table=[]
        
        for intersection in intersection_list:
            intersection_uid=intersection[0]
            intersection_id1=intersection[1]
            intersection_name=intersection[2]
            logger.info(intersection_name+'     '+str(start_time))
            for attempt in range(3):
                try:
                    table=get_intersection_tmc(table, start_time, end_iteration_time, intersection_id1, intersection_uid, key)
                    table=get_pedestrian(table, start_time, end_iteration_time, intersection_id1, intersection_uid, key)
                    break
                except exceptions.ProxyError as prox:
                    logger.error(prox)
                    logger.warning('Retrying in 2 minutes')
                    sleep(120)
                except exceptions.RequestException as err:
                    logger.error(err)
                    sleep(75)
                except TimeoutException as exc_504:
                    logger.error(exc_504)
                    sleep(60)
                except MiovisionAPIException as miovision_exc:
                    logger.error('Cannot pull data')
                    logger.error(miovision_exc)
      
        logger.info('Completed data pulling for {}'.format(start_time))
        try: 
            insert_data(conn, start_time, end_iteration_time, table)
        except psycopg2.Error as exc:
            logger.exception(exc)
            sys.exit()
        
        process_data(conn, pull, start_time, end_iteration_time)

        end_iteration_time+=time_delta
        start_time+=time_delta
        if start_time>=end_time:
            break
    logger.info('Done')
        
if __name__ == '__main__':
    cli()
    run_api(start_date, end_date, path, intersection, pull)