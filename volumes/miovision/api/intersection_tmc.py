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
import math
import logging
import configparser
import click
import traceback
from time import sleep

class MiovisionAPIException(Exception):
    """Base class for exceptions."""

class TimeoutException(Exception):
    """Exception if API gives a 504 error"""

class NotFoundError(Exception):
    """Exception for a 404 error."""

def logger():
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.INFO)
    formatter=logging.Formatter('%(asctime)s     	%(levelname)s    %(message)s', datefmt='%d %b %Y %H:%M:%S')
    stream_handler=logging.StreamHandler()
    stream_handler.setFormatter(formatter)
    logger.addHandler(stream_handler)
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
@click.option('--start_date', default=default_start, help='format is YYYY-MM-DD for start date')
@click.option('--end_date' , default=default_end, help='format is YYYY-MM-DD for end date & excluding the day itself')
@click.option('--path' , default='config_miovision_api_bot.cfg', help='enter the path/directory of the config.cfg file')
@click.option('--intersection' , default=[], multiple=True, help='enter the intersection_uid of the intersection')
@click.option('--pull' , is_flag=True, help='Data processing and gap finding will be skipped')
@click.option('--dupes' , is_flag=True, help='Script will fail if duplicates detected')

def run_api(start_date, end_date, path, intersection, pull, dupes):

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
    logger.info('Pulling from %s to %s' %(start_time,end_time))

    try:
        pull_data(conn, start_time, end_time, intersection, path, pull, key, dupes)
    except Exception as e:
        logger.critical(traceback.format_exc())
        sys.exit(1)


def get_movement(entrance, exit_dir):
    if (entrance == 'N' and exit_dir =='S'):
        return '1'
    elif entrance == 'S' and exit_dir =='N':
        return '1'
    elif entrance == 'W' and exit_dir =='E':
        return '1'
    elif entrance == 'E' and exit_dir =='W':
        return '1'
    elif entrance == 'S' and exit_dir =='W':
        return '2'
    elif entrance == 'N' and exit_dir =='E':
        return '2'
    elif entrance == 'W' and exit_dir =='N':
        return '2'
    elif entrance == 'E' and exit_dir =='S':
        return '2'
    elif entrance == 'S' and exit_dir =='E':
        return '3'
    elif entrance == 'E' and exit_dir =='N':
        return '3'
    elif entrance == 'N' and exit_dir =='W':
        return '3'
    elif entrance == 'W' and exit_dir =='S':
        return '3'
    return '4'

def get_crosswalk(item):
    if (item['direction'] == 'CW'):
        return '5'
    else:
        return '6'

def get_classification(veh_class):
    if (veh_class == 'Pedestrian'):
        return '6'
    elif veh_class == 'Light':
        return '1'
    elif veh_class == 'Bicycle':
        return '2'
    elif veh_class == 'Bus':
        return '3'
    elif veh_class == 'SingleUnitTruck':
        return '4'
    elif veh_class == 'ArticulatedTruck':
        return '5'
    elif veh_class == 'WorkVan':
        return '8'
    elif veh_class == 'MotorizedVehicle':
        return '9'
    raise ValueError("vehicle class {0} not recognized!".format(veh_class))

def get_intersection_tmc(table, start_time, end_iteration_time, intersection_id1, intersection_uid, key):
    headers={'Content-Type':'application/json','Authorization':key}
    params = {'endTime': end_iteration_time, 'startTime' : start_time}
    response=session.get(url+intersection_id1+tmc_endpoint, params=params,
                         headers=headers, proxies=session.proxies)
    if response.status_code==200:
        tmc=json.loads(response.content.decode('utf-8'))
        for item in tmc:

            item['classification']=get_classification(item['class'])
            item['volume']=item.pop('qty')
            item['movement']=get_movement(item['entrance'], item['exit'])
            item['leg']=item.pop('entrance')

            temp=[intersection_uid, item['timestamp'], item['classification'], item['leg'], item['movement'], item['volume']]
            table.append(temp)

        return table
    elif response.status_code==404:
        error=json.loads(response.content.decode('utf-8'))
        logger.error('Problem with tmc call for intersection %s', intersection_id1)
        logger.error(error['error'])
        raise NotFoundError
    elif response.status_code==400:
        logger.critical('Bad request error when pulling TMC data for intersection %s', intersection_id1)
        logger.critical('From %s until %s', start_time, end_iteration_time)
        error=json.loads(response.content.decode('utf-8'))
        logger.critical(error['error'])
        sys.exit(5)
    elif response.status_code==504:
        raise TimeoutException('Error'+str(response.status_code))
    logger.critical('Unknown error pulling tmcs for intersection %s', intersection_id1)
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
        error=json.loads(response.content.decode('utf-8'))
        logger.error('Problem with ped call for intersection %s', intersection_id1)
        logger.error(error['error'])
        raise NotFoundError
    elif response.status_code==400:
        logger.critical('Bad request error when pulling ped data for intersection %s', intersection_id1)
        logger.critical('From %s until %s', start_time, end_iteration_time)
        error=json.loads(response.content.decode('utf-8'))
        logger.critical(error['error'])
        sys.exit(5)
    elif response.status_code==504:
        raise TimeoutException('Error'+str(response.status_code))
    logger.critical('Unknown error pulling ped data for intersection %s', intersection_id1)
    raise MiovisionAPIException('Error'+str(response.status_code))

def process_data(conn, start_time, end_iteration_time):
    # UPDATE gapsize_lookup TABLE AND RUN find_gaps FUNCTION

    with conn:
        with conn.cursor() as cur:
            update_gaps="SELECT miovision_api.refresh_gapsize_lookup()"
            cur.execute(update_gaps)
    time_period = (start_time, end_iteration_time)
    with conn:
        with conn.cursor() as cur:
            invalid_gaps="SELECT miovision_api.find_gaps(%s::date, %s::date)"
            cur.execute(invalid_gaps, time_period)
            logger.info(conn.notices[-1])
    logger.info('Updated gapsize table and found gaps exceeding allowable size')

    # Aggregate to 15min tmc / 15min
    try:
        with conn:
            with conn.cursor() as cur:
                update="SELECT miovision_api.aggregate_15_min_tmc(%s::date, %s::date)"
                cur.execute(update, time_period)
                logger.info('Aggregated to 15 minute bins')

                atr_aggregation="SELECT miovision_api.aggregate_15_min(%s::date, %s::date)"
                cur.execute(atr_aggregation, time_period)
                logger.info('Completed data processing for %s', start_time)

    except psycopg2.Error as exc:
        logger.exception(exc)
        sys.exit(1)

    with conn:
        with conn.cursor() as cur:
            report_dates="SELECT miovision_api.report_dates(%s::date, %s::date)"
            cur.execute(report_dates, time_period)
            logger.info('report_dates done')

def insert_data(conn, start_time, end_iteration_time, table, dupes):
    time_period = (start_time, end_iteration_time)
    conn.notices=[]
    with conn:
        with conn.cursor() as cur:
            insert_data = '''INSERT INTO miovision_api.volumes(intersection_uid, datetime_bin, classification_uid,
                             leg,  movement_uid, volume) VALUES %s'''
            execute_values(cur, insert_data, table)
            if conn.notices != []:
                logger.warning(conn.notices[-1])
                if dupes:
                    sys.exit(2)

    with conn:
        with conn.cursor() as cur:
            api_log="SELECT miovision_api.api_log(%s::date, %s::date)"
            cur.execute(api_log, time_period)

    logger.info('Inserted into volumes and updated log')

    with conn:
        with conn.cursor() as cur:
            invalid_movements="SELECT miovision_api.find_invalid_movements(%s::date, %s::date)"
            cur.execute(invalid_movements, time_period)
            logger.info(conn.notices[-1])


def daterange(start_time, end_time, dt):
    """Generator for a sequence of regular time periods."""
    for i in range(math.ceil((end_time - start_time) / dt)):
        c_start_t = start_time + i * dt
        yield (c_start_t, c_start_t + dt)


def pull_data(conn, start_time, end_time, intersection, path, pull, key, dupes):

    time_delta = datetime.timedelta(hours=6)

    if intersection != []:
        with conn.cursor() as cur:
            wanted = tuple(intersection) # convert list into tuple
            string= '''SELECT * FROM miovision_api.intersections
                        WHERE intersection_uid IN %s
                        AND %s::date > date_installed
                        AND date_decommissioned IS NULL '''
            cur.execute(string, (wanted, start_time))

            intersection_list=cur.fetchall()
            logger.debug(intersection_list)
    else:
        with conn.cursor() as cur:
            string2= '''SELECT * FROM miovision_api.intersections
                        WHERE %s::date >= date_installed
                        AND date_decommissioned IS NULL'''
            cur.execute(string2, (start_time,))
            intersection_list=cur.fetchall()
            logger.debug(intersection_list)

    if len(intersection_list) == 0:
        logger.critical('No intersections found in miovision_api.intersections for the specified start time')
        sys.exit(3)

    for (c_start_t, c_end_t) in daterange(start_time, end_time, time_delta):

        table=[]

        for interxn in intersection_list:
            intersection_uid=interxn[0]
            intersection_id1=interxn[1]
            intersection_name=interxn[2]
            logger.info(intersection_name+'     '+str(c_start_t))
            for attempt in range(3):
                try:
                    table=get_intersection_tmc(table, c_start_t, c_end_t, intersection_id1, intersection_uid, key)
                    table=get_pedestrian(table, c_start_t, c_end_t, intersection_id1, intersection_uid, key)
                    break
                except exceptions.ProxyError as prox:
                    logger.error(prox)
                    logger.warning('Retrying in 2 minutes')
                    sleep(120)
                except exceptions.RequestException as err:
                    logger.error(err)
                    sleep(75)
                except NotFoundError:
                    break
                except TimeoutException as exc_504:
                    logger.error(exc_504)
                    sleep(60)
                except MiovisionAPIException as miovision_exc:
                    logger.error(miovision_exc)
                    break
            else:
                logger.error('Could not successfully pull data for this intersection')

        logger.info('Completed data pulling from {0:s} to {1:s}'
                    .format(c_start_t, c_end_t))
        try:
            insert_data(conn, c_start_t, c_end_t, table, dupes)
        except psycopg2.Error as exc:
            logger.exception(exc)
            sys.exit(1)

    if pull_data:
        logger.info('Skipping aggregating and processing volume data')
    else:
        process_data(conn, start_time, end_time)

    logger.info('Done')

if __name__ == '__main__':
    cli()
