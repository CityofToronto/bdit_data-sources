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
from time import sleep

class MiovisionAPIException(Exception):
    """Base class for exceptions."""
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



CONTEXT_SETTINGS = dict(
    default_map={'run_api': {'flag': 0}}
)

@click.group(context_settings=CONTEXT_SETTINGS)
def cli():
    pass

@cli.command()
@click.option('--start', '--start_date', default=default_start, help='format is YYYY-MM-DD for start date')
@click.option('--end' ,'--end_date', default=default_end, help='format is YYYY-MM-DD for end date')
@click.option('--path' ,'--path', default='config.cfg', help='format is YYYY-MM-DD for end date')
def run_api(start_date, end_date, path):
    CONFIG = configparser.ConfigParser()
    CONFIG.read(path)
    api_key=CONFIG['API']
    key=api_key['key']
    dbset = CONFIG['DBSETTINGS']
    conn = connect(**dbset)
    
    start_date= dateutil.parser.parse(str(start_date))
    end_date= dateutil.parser.parse(str(end_date))
    start_time=local_tz.localize(start_date)
    end_time=local_tz.localize(end_date)
    logger.info('Pulling from %s to %s' %(str(start_date),str(end_date)))
    pull_data(start_time, end_time)
  
    
CONFIG = configparser.ConfigParser()
CONFIG.read(r'config.cfg')
api_key=CONFIG['API']
key=api_key['key']
dbset = CONFIG['DBSETTINGS']
conn = connect(**dbset)

logger.debug('Connected to DB')
local_tz=pytz.timezone('US/Eastern')
session = Session()
session.proxies = {'https': ''}
url='https://api.miovision.com/intersections/'
tmc_endpoint = '/tmc'
ped_endpoint='/crosswalktmc'


def get_movement(item):
    if (item['entrance'] == 'N' and item['exit'] =='S'):
        return 'thru'
    elif item['entrance'] == 'S' and item['exit'] =='N':
        return 'thru'
    elif item['entrance'] == 'W' and item['exit'] =='E':
        return 'thru'
    elif item['entrance'] == 'E' and item['exit'] =='W':
        return 'thru'
    elif item['entrance'] == 'S' and item['exit'] =='W':
        return 'left'
    elif item['entrance'] == 'N' and item['exit'] =='E':
        return 'left'
    elif item['entrance'] == 'W' and item['exit'] =='N':
        return 'left'
    elif item['entrance'] == 'E' and item['exit'] =='S':
        return 'left'
    elif item['entrance'] == 'S' and item['exit'] =='E':
        return 'right'
    elif item['entrance'] == 'E' and item['exit'] =='N':
        return 'right'
    elif item['entrance'] == 'N' and item['exit'] =='W':
        return 'right'
    elif item['entrance'] == 'W' and item['exit'] =='S':
        return 'right'
    else:
        return 'u_turn'

def get_intersection_tmc(table, start_date, end_iteration_time, intersection_id1, intersection_name, lat, lng):
    headers={'Content-Type':'application/json','Authorization':key}
    params = {'endTime': end_iteration_time, 'startTime' : start_date}
    response=session.get(url+intersection_id1+tmc_endpoint, params=params, 
                         headers=headers, proxies=session.proxies)
    if response.status_code==200:
        tmc=json.loads(response.content.decode('utf-8'))
        for item in tmc:
            
        
            item['classification']=item.pop('class')
            item['volume']=item.pop('qty')
            item['movement']=get_movement(item)
            
            item['entry_dir_name']=item.pop('entrance')
            item['exit_dir_name']=item.pop('exit')
            temp=[intersection_name, lat, lng, item['timestamp'], item['classification'], item['entry_dir_name'],  item['exit_dir_name'],  item['movement'], item['volume']]
            table.append(temp)

        return table
    elif response.status_code==404:
        error=json.loads(response.content)
        logger.error(error['error'])
    elif response.status_code==400:
        error=json.loads(response.content)
        logger.error(error['error'])
    else:
        raise MiovisionAPIException('Error'+str(response.status_code))
        sys.exit()

def get_pedestrian(table, start_date, end_iteration_time, intersection_id1, intersection_name, lat, lng):
    headers={'Content-Type':'application/json','Authorization':key}
    params = {'endTime': end_iteration_time, 'startTime' : start_date}
    
    response=session.get(url+intersection_id1+ped_endpoint, params=params, 
                         headers=headers, proxies=session.proxies)
    if response.status_code==200:
        ped=json.loads(response.content.decode('utf-8'))
        for item in ped:
            
            item['classification']=item.pop('class')
            item['volume']=item.pop('qty')
            temp=str(item['direction'])
            item.pop('direction', None)
            item['movement']=temp.lower()
            item['entry_dir_name']=item.pop('crosswalkSide')
            item['exit_dir_name']=None
            temp=[intersection_name, lat, lng, item['timestamp'], item['classification'], item['entry_dir_name'],  item['exit_dir_name'],  item['movement'], item['volume']]
            table.append(temp)
            
        return table
    elif response.status_code==404:
        error=json.loads(response.content)
        logger.error(error['error'])
        
    elif response.status_code==400:
        error=json.loads(response.content)
        logger.error(error['error'])
    else:
        raise MiovisionAPIException('Error'+str(response.status_code))
        sys.exit()

# =============================================================================
# # =============================================================================
# # def views():
# #     try:
# #         with conn:
# #             with conn.cursor() as cur:
# #                 report_dates='''SELECT miovision_api.report_dates();'''
# #                 cur.execute(report_dates)
# #                 refresh_volumes_class='''REFRESH MATERIALIZED VIEW miovision_api.volumes_15min_by_class WITH DATA;'''
# #                 cur.execute(refresh_volumes_class)
# #                 refresh_volumes='''REFRESH MATERIALIZED VIEW miovision_api.report_volumes_15min WITH DATA;'''
# #                 cur.execute(refresh_volumes)
# #                 refresh_report_daily='''REFRESH MATERIALIZED VIEW miovision_api.report_daily WITH DATA;'''
# #                 cur.execute(refresh_report_daily)
# #                 conn.commit()
# #         logger.info('Updated Views')
# # =============================================================================
# 
#         
#     except:
#         logger.exception('Cannot Refresh Views')
#         sys.exit()
#         
# 
# =============================================================================
def pull_data(start_date, end_date):
    time_delta = datetime.timedelta(days=1)
    end_iteration_time= start_date + time_delta
    while True:
        table=[]
        with conn:
            with conn.cursor() as cur:
                select='''SELECT * from miovision.intersection_id;'''
                cur.execute(select)
                intersection_list=cur.fetchall()
                conn.commit()

        for intersection_list in intersection_list:
            intersection_id1=intersection_list[1]
            intersection_name=intersection_list[2]
            lat=str(intersection_list[3])
            lng=str(intersection_list[4])
            logger.debug(intersection_name+'     '+str(start_date))
            for attempt in range(3):
                try:
                    table=get_intersection_tmc(table, start_date, end_iteration_time, intersection_id1, intersection_name, lat, lng)
                    table=get_pedestrian(table, start_date, end_iteration_time, intersection_id1, intersection_name, lat, lng)
                    break
                except exceptions.ProxyError as prox:
                    logger.error(prox)
                    logger.warning('Retrying in 2 minutes')
                    sleep(120)
                except exceptions.RequestException as err:
                    logger.error(err)
                    sleep(60)
                except MiovisionAPIException as miovision_exc:
                    logger.error('Cannot pull data')
                    logger.error(miovision_exc)
                    #sleep(60)
                
        logger.info('Completed data pulling for {}'.format(start_date))
        try:
            with conn:
                with conn.cursor() as cur:
                    execute_values(cur, 'INSERT INTO miovision_api.raw_data (study_name, lat, lng, datetime_bin, classification, entry_dir_name, exit_dir_name,  movement, volume) VALUES %s', table)
                    conn.commit()
            logger.info('Inserted into raw data') 
            with conn:
                with conn.cursor() as cur: 
                    insert='''
        SELECT miovision_api.aggregate_15_min_tmc_new()'''
                    cur.execute(insert)
                    conn.commit()
            logger.info('Aggregated to 15 minute bins')        

            with conn:
                with conn.cursor() as cur:             
                    cur.execute('''SELECT miovision_api.aggregate_15_min();''')
                    conn.commit()
            logger.info('Completed data processing for {}'.format(start_date))
           
        except psycopg2.Error as exc:
            
            logger.exception(exc)
            with conn:
                    conn.rollback()
            sys.exit()
        start_date+=time_delta
        end_iteration_time= start_date + time_delta
        if start_date>=end_date:
            break
    #views()
    logger.info('Done')
    

if __name__ == '__main__':
    cli()
