# -*- coding: utf-8 -*-
"""
Created on Wed Oct 17 15:26:52 2018

@author: rliu4
"""


import json
import csv
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

def get_signs(api_key):
    headers={'Content-Type':'application/json','x-api-key':api_key}
    #params = {'endTime': end_iteration_time, 'startTime' : start_time}
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
    elif response.status_code==504:
        error=response.json()
        logger.error('504 error    '+error['error_message'])
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
            address=item['address']
            statistics=str(get_location(location, api_key))
            if statistics[4:11] == 'LocInfo':
                temp=[location, address]
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

CONTEXT_SETTINGS = dict(
    default_map={'run_api': {'minutes':'1443','pulltime':'2:01', 'path':'config.cfg','location_flag': 0}}
)

@click.group(context_settings=CONTEXT_SETTINGS)
def cli():
    pass

@cli.command()
@click.option('--minutes', '--minutes', default='1443', help='amount of minutes to request')
@click.option('--pull_time' ,'--pull_time', default='2:01', help='time to start pulling data')
@click.option('--path' ,'--path', default='config.cfg', help='enter the path/directory of the config.cfg file')
@click.option('--location_flag' ,'--location_flag', default=0, help='enter the location_id of the sign')
def run_api(minutes, pull_time, path, location_flag):

    
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
    if location_flag ==0:
        signs_list=location_id(api_key)
        signs_iterator=signs_list
    else:
        temp_list=[location_flag, None]
        signs_list.append(temp_list)
        signs_iterator=signs_list
    while datetime.datetime.now()< pull_time:
        time.sleep(10)
    logger.debug('Pulling data')  
    for signs_iterator in signs_iterator:
        location=signs_iterator[0]
        for attempt in range(3):
            try:
                statistics=get_statistics(location, minutes, api_key)
                raw_data=statistics['LocInfo']
                raw_records=raw_data['raw_records']
                location_info=raw_data['Location']
                name=location_info['name']
                logger.debug(str(name))
                for item in raw_records:
                    datetime_bin=item['datetime']
                    datetime_bin= dateutil.parser.parse(str(datetime_bin))
                    datetime_bin=roundTime(datetime_bin,roundTo=5*60)
                    counter=item['counter']
                    for item in counter:
                        temp=[location, datetime_bin, item['speed'], item['count']]
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
                
        else:
            #send_mail(email['to'], email['from'], 'WYS API Error','\n'.join(map(str, error_array)))  
            pass
        signs_iterator=signs_list
        
    try:    
        with conn.cursor() as cur:
            execute_values(cur, 'INSERT INTO wys.raw_data (api_id, datetime_bin, speed, count) VALUES %s', table)
            conn.commit()
    except psycopg2.Error as exc:
        logger.exception(exc)
        with conn:
                conn.rollback()
        sys.exit()
    except Exception as e:
        logger.critical(traceback.format_exc())


if __name__ == '__main__':
    cli()
