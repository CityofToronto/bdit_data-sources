import sys
import json
import csv
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
    if (logger.hasHandlers()):
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

CONFIG = configparser.ConfigParser()
CONFIG.read(r'config.cfg')
api_key=CONFIG['API']
key=api_key['key']
dbset = CONFIG['DBSETTINGS']
conn = connect(**dbset)

logger.debug('Connected to DB')

session = Session()
session.proxies = {'https': 'https://137.15.73.132:8080'}
url='https://api.miovision.com/intersections/'
tmc_endpoint = '/tmc'
ped_endpoint='/crosswalktmc'


CONTEXT_SETTINGS = dict(
    default_map={'runserver': {'flag': 0}}
)

@click.group(context_settings=CONTEXT_SETTINGS)
def cli():
    pass

@cli.command()
@click.option('--flag', default=0, help='Set flag=1 to set a custom date range to pull data')
def run_api(flag):
    time_delta = datetime.timedelta(days=1)

    default_start=datetime.date.today()-time_delta
    default_end=datetime.date.today()
    click.echo(flag)
    if flag==1:
        
        start_date, end_date=dates()
        logger.info('Pulling from %s to %s' %(str(start_date),str(end_date)))
        pull_data(start_date, end_date)
           
    else:
        start_date= dateutil.parser.parse(str(default_start))
        end_date= dateutil.parser.parse(str(default_end))
        start_time=start_date.astimezone(pytz.timezone('US/Eastern'))
        end_time=start_date.astimezone(pytz.timezone('US/Eastern'))
        logger.info('Pulling from %s to %s' %(str(start_date),str(end_date)))
        pull_data(start_time, end_time)
        views()
        
    
def dates():
    time_delta = datetime.timedelta(days=1)
    max_date=datetime.datetime.now()-time_delta
    max_start=max_date.astimezone(pytz.timezone('US/Eastern'))
    while True:
        while True:
            start = click.prompt('Enter the start date in YYYY-MM-DD format', default=str)
            start_date=dateutil.parser.parse(start)
            start_time=start_date.astimezone(pytz.timezone('US/Eastern'))
            if start_time>=max_start:
                print('Date not valid, must be at least 1 day before today')
                continue
            else:
                break
            
        while True:  
            end = click.prompt('Enter the end date in YYYY-MM-DD format', default=str)
            end_date=dateutil.parser.parse(end)
            end_time=end_date.astimezone(pytz.timezone('US/Eastern'))
            if end_time>=datetime.datetime.now().astimezone(pytz.timezone('US/Eastern')):
                print('Date not valid, end date cannot be after today')
                continue
            elif end_date<=start_date:
                print('Date not valid, must be at least 1 day after the start date')
                continue
            else:
                break
        confirm = click.prompt('Type Y to confirm %s is your start date and %s is your end date' %(str(start_time),str(end_time)), default=str)
        if confirm == 'Y':
            break
    return start_time, end_time  

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
        tmc=json.loads(response.content)
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

    elif response.status_code==404:
        error=json.loads(response.content)
        logger.error(error['error'])
    else:
        raise MiovisionAPIException(str(response.status_code))
        sys.exit()

def get_pedestrian(table, start_date, end_iteration_time, intersection_id1, intersection_name, lat, lng):
    headers={'Content-Type':'application/json','Authorization':key}
    params = {'endTime': end_iteration_time, 'startTime' : start_date}
    
    response=session.get(url+intersection_id1+ped_endpoint, params=params, 
                         headers=headers, proxies=session.proxies)
    if response.status_code==200:
        ped=json.loads(response.content)
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
        raise MiovisionAPIException(str(response.status_code))
        sys.exit()

def views():
    try:
        with conn:
            with conn.cursor() as cur:
                report_dates='''SELECT rliu.report_dates();'''
                cur.execute(report_dates)
                refresh_volumes_class='''REFRESH MATERIALIZED VIEW rliu.volumes_15min_by_class WITH DATA;'''
                cur.execute(refresh_volumes_class)
                refresh_volumes='''REFRESH MATERIALIZED VIEW rliu.report_volumes_15min WITH DATA;'''
                cur.execute(refresh_volumes)
                refresh_report_daily='''REFRESH MATERIALIZED VIEW rliu.report_daily WITH DATA;'''
                cur.execute(refresh_report_daily)
        logger.info('Updated Views')
        logger.info('Done')
        
    except:
        logger.exception('Cannot Refresh Views')
        sys.exit()
        

def pull_data(start_date, end_date):
    time_delta = datetime.timedelta(days=1)
    end_iteration_time= start_date + time_delta
    while True:
        table=[]
        with open('intersection_id.csv', 'r') as int_id_file:
            intersection_id=csv.DictReader(int_id_file)
            for row in intersection_id:
                intersection_id1=str(row['id'])
                intersection_name=str(row['name'])
                lat=str(row['lat'])
                lng=str(row['lng'])
                logger.info(intersection_name+'     '+str(start_date))
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
                        #sys.exit()
                    except MiovisionAPIException as miovision_exc:
                        logger.error('Cannot pull data')
                        logger.error(miovision_exc)
                        #sleep(60)
                
        logger.info('Completed data pulling for {}'.format(start_date))
        try:
            with conn:
                with conn.cursor() as cur:
                    execute_values(cur, 'INSERT INTO rliu.raw_data_new (study_name, lat, lng, datetime_bin, classification, entry_dir_name, exit_dir_name,  movement, volume) VALUES %s', table)
                    conn.commit
            logger.info('Inserted into raw data new') 
            with conn:
                with conn.cursor() as cur: 
                    insert='''INSERT INTO rliu.raw_data(study_id, study_name, lat, lng, datetime_bin, classification, entry_dir_name, entry_name, exit_dir_name, exit_name, movement, volume)
                                SELECT * FROM rliu.raw_data_new;
        SELECT rliu.aggregate_15_min_tmc_new()'''
                    cur.execute(insert)
                    conn.commit
            logger.info('Aggregated to 15 minute bins and inserted to raw data')        

            with conn:
                with conn.cursor() as cur:             
                    cur.execute('''SELECT rliu.aggregate_15_min(); TRUNCATE rliu.raw_data_new;''')
                    conn.commit
            logger.info('Completed data processing for {}'.format(start_date))
        except psycopg2.Error as exc:
            
            logger.exception(exc)
            with conn:
                    conn.rollback()
            sys.exit()
        views()
        start_date+=time_delta
        end_iteration_time= start_date + time_delta
        if start_date>=end_date:
            break
    

if __name__ == '__main__':
    cli()
