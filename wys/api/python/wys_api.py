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
#from notify_email import send_mail
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

def roundTime(dt=None, roundTo=60):
   """function to round to nearest 5 minutes
   Author: Thierry Husson 2012 - Use it as you want but don't blame me.
   """
   seconds = (dt.replace(tzinfo=None) - dt.min).seconds
   rounding = (seconds+roundTo/2) // roundTo * roundTo
   return dt + datetime.timedelta(0,rounding-seconds,-dt.microsecond)

def speed_bin(speed):
    if 0<=speed<5:
        return 1
    if 5<=speed<10:
        return 2
    if 10<=speed<15:
        return 3
    if 15<=speed<20:
        return 4
    if 20<=speed<25:
        return 5
    if 25<=speed<30:
        return 6
    if 30<=speed<35:
        return 7
    if 35<=speed<40:
        return 8
    if 40<=speed<45:
        return 9
    if 45<=speed<50:
        return 10
    if 50<=speed<55:
        return 11
    if 55<=speed<60:
        return 12
    if 60<=speed<65:
        return 13
    if 65<=speed<70:
        return 14
    if 70<=speed<75:
        return 15
    if 75<=speed<80:
        return 16
    if speed>=80:
        return 17

def get_signs(api_key):
    headers={'Content-Type':'application/json','x-api-key':api_key}
    response=session.get(url+signs_endpoint,
                         headers=headers, proxies=session.proxies)
    if response.status_code==200:
        signs=response.json()
        return signs


def get_statistics(location, minutes, api_key):
    headers={'Content-Type':'application/json','x-api-key':api_key}
    response=session.get(url+statistics_endpoint+str(location)+period+minutes+units, 
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
    response=session.get(url+statistics_endpoint+str(location)+end, 
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
statistics_endpoint='/signs/statistics/location/'
period='/period/'
units='/speed_units/0'
end='/period/5/speed_units/0'
time_delta = datetime.timedelta(days=1)
date=(datetime.datetime.today()+time_delta).strftime('%Y-%m-%d')

CONTEXT_SETTINGS = dict(
    default_map={'run_api': {'minutes':'1473','pull_time':'0:01', 'path':'config.cfg','location_flag': 0}}
)

@click.group(context_settings=CONTEXT_SETTINGS)
def cli():
    pass

@cli.command()
@click.option('--minutes', '--minutes', default='1473', help='amount of minutes to request')
@click.option('--pull_time' ,'--pull_time', default='0:01', help='time to start pulling data')
@click.option('--path' ,'--path', default='config.cfg', help='enter the path/directory of the config.cfg file')
@click.option('--location_flag' ,'--location_flag', default=0, help='enter the location_id of the sign')
def run_api(minutes, pull_time, path, location_flag):
    
    if dateutil.parser.parse(str(pull_time))<datetime.datetime.now():
        pull_time=(datetime.datetime.today()+time_delta).strftime('%Y-%m-%d')+'T'+pull_time
    pull_time= dateutil.parser.parse(str(pull_time))
    logger.info('Pulling '+minutes+' minutes of data')
    CONFIG = configparser.ConfigParser()
    CONFIG.read(path)
    api_main(minutes, pull_time, location_flag, CONFIG)

def api_main(minutes, pull_time, location_flag, CONFIG):
    key=CONFIG['API']
    api_key=key['key']
    dbset = CONFIG['DBSETTINGS']
    #email=CONFIG['EMAIL']
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
        #send_mail(email['to'], email['from'], 'Error retrieving list of signs', str(traceback.format_exc()))  
    while datetime.datetime.now()< pull_time:
        time.sleep(10)
    logger.debug('Pulling data')  
    for signs_iterator in signs_iterator:
        location=signs_iterator[0]
        name=signs_iterator[1]
        logger.debug(str(name))
        for attempt in range(3):
            try:
                statistics=get_statistics(location, minutes, api_key)
                raw_data=statistics['LocInfo']
                raw_records=raw_data['raw_records']
                for item in raw_records:
                    datetime_bin=item['datetime']
                    datetime_bin= dateutil.parser.parse(str(datetime_bin))
                    counter=item['counter']
                    for item in counter:
                        if datetime_bin<pull_time-time_delta:
                            pass
                        elif datetime_bin>pull_time:
                            pass
                        else:
                            speed=item['speed']
                            speed=int(speed)
                            speed=speed_bin(speed)
                            temp=[location, datetime_bin, str(speed), item['count']]
                            table.append(temp)   
                break
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
            #send_mail(email['to'], email['from'], 'WYS API Error','\n'.join(map(str, error_array)))  
            pass
        signs_iterator=signs_list
        
    try:    
        with conn.cursor() as cur:
            logger.debug('Inserting '+str(len(table))+' rows of data')
            execute_values(cur, 'INSERT INTO wys.speed_counts (api_id, datetime_bin, speed_id, count) VALUES %s', table)
            conn.commit()
    except psycopg2.Error as exc:
        logger.exception(exc)
        with conn:
            conn.rollback()
        #send_mail(email['to'], email['from'], 'Postgres Error', str(traceback.format_exc()))  
        sys.exit()
    except Exception as e:
        logger.critical(traceback.format_exc())
        #send_mail(email['to'], email['from'], 'Postgres Error', str(traceback.format_exc()))  
    
   
    loc_table=[]   
    try:
        with conn.cursor() as cur:
            string="SELECT DISTINCT a.api_id FROM wys.speed_counts a LEFT JOIN wys.locations b using (api_id) WHERE b.api_id IS NULL"
            cur.execute(str(string))
            intersection_list=cur.fetchall()
            conn.commit()
        for item in intersection_list:
            statistics=get_statistics(str(item[0]),'5',api_key)
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
            temp=[str(item[0]), address, name, direction]
            loc_table.append(temp)
            
        with conn.cursor() as cur:
            logger.debug('Inserting '+str(len(loc_table))+' new locations')
            execute_values(cur, 'INSERT INTO wys.locations (api_id, address, sign_name, dir) VALUES %s', loc_table)
            conn.commit()
            logger.info('Inserted Data')
            counts_15min="SELECT wys.aggregate_speed_counts_15min();"
            cur.execute(counts_15min)
            conn.commit()
            logger.info('Aggregated Speed Count Data')
            volumes_15min='''SELECT wys.aggregate_volumes_15min();'''
            cur.execute(volumes_15min)
            conn.commit()
            logger.info('Aggregated Volumes Data')
            refresh_report_dates='''REFRESH MATERIALIZED VIEW wys.report_dates WITH DATA;'''
            cur.execute(refresh_report_dates)
            conn.commit()
            refresh_counts_average='''REFRESH MATERIALIZED VIEW wys.counts_average WITH DATA;'''
            cur.execute(refresh_counts_average)
            conn.commit()
            refresh_counts_full='''REFRESH MATERIALIZED VIEW wys.counts_15min_full WITH DATA;'''
            cur.execute(refresh_counts_full)
            conn.commit()
            refresh_volume_average='''REFRESH MATERIALIZED VIEW wys.volume_average WITH DATA;'''
            cur.execute(refresh_volume_average)
            conn.commit()
            refresh_volume_full='''REFRESH MATERIALIZED VIEW wys.volumes_15min_full WITH DATA;'''
            cur.execute(refresh_volume_full)
            conn.commit()
            logger.info('Updated Views')
    except Exception as e:
        logger.critical(traceback.format_exc())
        error_array.append(str(traceback.format_exc()))
        #send_mail(email['to'], email['from'], 'Error', str(traceback.format_exc()))  
    
    
if __name__ == '__main__':
    cli()
