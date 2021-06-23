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


class BreakingError(Exception):
    """Base class for exceptions that immediately halt API pulls."""


class MiovisionAPIException(BreakingError):
    """Base class for exceptions."""


class NotFoundError(BreakingError):
    """Exception for a 404 error."""


class RetryError(Exception):
    """Base class for exceptions that warrant a retry."""


class TimeoutException(RetryError):
    """Exception if API gives a 504 error"""


class ServerException(RetryError):
    """Exception if API gives a 500 error"""


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
@click.option('--intersection' , multiple=True, help='enter the intersection_uid of the intersection')
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

    start_time = dateutil.parser.parse(str(start_date))
    end_time = dateutil.parser.parse(str(end_date))
    logger.info('Pulling from %s to %s' %(start_time, end_time))

    try:
        pull_data(conn, start_time, end_time, intersection, path, pull, key, dupes)
    except Exception as e:
        logger.critical(traceback.format_exc())
        sys.exit(1)


def get_movement(entrance, exit_dir):
    if entrance == 'UNDEFINED' or exit_dir == 'UNDEFINED':
        return '-1'
    elif entrance == 'N' and exit_dir =='S':
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
    if veh_class == 'Light':
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


def get_crosswalk_class(cw_class):
    if cw_class == 'Pedestrian':
        return '6'
    elif cw_class == 'Bicycle':
        return '7'
    raise ValueError("crosswalk class {0} not recognized!".format(cw_class))


def get_intersection_tmc(start_time, end_iteration_time, intersection_id1,
                         intersection_uid, key):
    headers={'Content-Type':'application/json','Authorization':key}
    # Subtract 1 ms from endTime to handle hallucinated rows that occur at
    # exactly endTime (See bdit_data-sources/#374).
    params = {'endTime': (end_iteration_time
                          - datetime.timedelta(milliseconds=1)),
              'startTime': start_time}

    response=session.get(url+intersection_id1+tmc_endpoint, params=params,
                         headers=headers, proxies=session.proxies)
    if response.status_code==200:
        table = []
        tmc=json.loads(response.content.decode('utf-8'))
        for item in tmc:

            item['classification']=get_classification(item['class'])
            item['volume']=item.pop('qty')
            item['movement']=get_movement(item['entrance'], item['exit'])
            item['leg']=item.pop('entrance')

            temp=[intersection_uid, item['timestamp'], item['classification'], item['leg'], item['movement'], item['volume']]

            # Hack to drop any bike approach volumes, since we currently can't
            # handle them.
            if item['movement'] != '-1':
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
    elif response.status_code==500:
        raise ServerException('Error'+str(response.status_code))
    logger.critical('Unknown error pulling tmcs for intersection %s', intersection_id1)
    raise MiovisionAPIException('Error'+str(response.status_code))


def get_crosswalk_tmc(start_time, end_iteration_time, intersection_id1,
                      intersection_uid, key):
    headers={'Content-Type':'application/json','Authorization':key}
    # Subtract 1 ms from endTime to handle hallucinated rows that occur at
    # exactly endTime (See bdit_data-sources/#374).
    params = {'endTime': (end_iteration_time
                          - datetime.timedelta(milliseconds=1)),
              'startTime': start_time}

    response=session.get(url+intersection_id1+ped_endpoint, params=params,
                         headers=headers, proxies=session.proxies)
    if response.status_code==200:
        table = []
        ped=json.loads(response.content.decode('utf-8'))
        for item in ped:

            item['classification'] = get_crosswalk_class(item['class'])
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
    elif response.status_code==500:
        raise ServerException('Error'+str(response.status_code))
    logger.critical('Unknown error pulling ped data for intersection %s', intersection_id1)
    raise MiovisionAPIException('Error'+str(response.status_code))


def process_data(conn, start_time, end_iteration_time):
    # UPDATE gapsize_lookup TABLE AND RUN find_gaps FUNCTION

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
            report_dates="SELECT miovision_api.get_report_dates(%s::date, %s::date)"
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


def daterange(start_time, end_time, time_delta):
    """Generator for a sequence of regular time periods."""
    curr_time = start_time
    while curr_time < end_time:
        yield curr_time
        curr_time += time_delta


class Intersection:

    def __init__(self, uid, id1, name, date_installed,
                 date_decommissioned):
        self.uid = uid
        self.id1 = id1
        self.name = name
        self.date_installed = date_installed
        self.date_decommissioned = date_decommissioned

    def __repr__(self):
        return ("intersection_uid: {u}\n"
                "    intersection_id1: {id1}\n"
                "    name: {n}\n"
                "    date_installed: {di}\n"
                "    date_decommissioned: {ddc}\n"
                .format(u=self.uid, id1=self.id1,
                        n=self.name, di=self.date_installed,
                        ddc=self.date_decommissioned))

    def is_active(self, ctime):
        """Checks if an intersection's Miovision system is active.
        
        Parameters
        ----------
        ctime : datetime.datetime
            Time to check.
        """
        cdate = ctime.date()
        # Check if inputted time is at least 1 day after the activation date
        # of the station. If deactivation date exists, check that the time is
        # at least 1 day before.
        if self.date_decommissioned is not None:
            return ((cdate > self.date_installed)
                    & (cdate < self.date_decommissioned))
        return cdate > self.date_installed


def get_intersection_info(conn, intersection=()):

    with conn.cursor() as cur:
        sql_query = """SELECT intersection_uid,
                              id,
                              intersection_name,
                              date_installed,
                              date_decommissioned
                       FROM miovision_api.intersections"""
        if len(intersection) > 0:
            sql_query += """ WHERE intersection_uid IN %s"""
            cur.execute(sql_query, (intersection, ))
        else:
            cur.execute(sql_query)
        intersection_list = cur.fetchall()

    return [Intersection(*x) for x in intersection_list]


def pull_data(conn, start_time, end_time, intersection, path, pull, key, dupes):

    time_delta = datetime.timedelta(hours=6)

    intersections = get_intersection_info(conn, intersection=intersection)

    if len(intersections) == 0:
        logger.critical('No intersections found in '
                        'miovision_api.intersections for the specified '
                        'start time.')
        sys.exit(3)

    # So we don't make the comparison thousands of times below.
    user_def_intersection = len(intersection) > 0

    for c_start_t in daterange(start_time, end_time, time_delta):

        c_end_t = c_start_t + time_delta
        table = []

        for c_intersec in intersections:

            if c_intersec.is_active(c_start_t):
                logger.info(c_intersec.name + '     ' + str(c_start_t))

                for attempt in range(3):
                    try:
                        table_veh = get_intersection_tmc(
                            c_start_t, c_end_t, c_intersec.id1,
                            c_intersec.uid, key)
                        table_ped = get_crosswalk_tmc(
                            c_start_t, c_end_t,
                            c_intersec.id1, c_intersec.uid, key)
                        break
                    except (exceptions.ProxyError,
                            exceptions.RequestException, RetryError) as err:
                        logger.error(err)
                        logger.warning('Retrying in 2 minutes '
                                       'if tries remain.')
                        sleep(120)
                    except BreakingError as err:
                        logger.error(err)
                        table_veh = []
                        table_ped = []
                        break
                else:
                    logger.error('Could not successfully pull '
                                 'data for this intersection after 3 tries.')
                    table_veh = []
                    table_ped = []

                table.extend(table_veh)
                table.extend(table_ped)

                # Hack to slow down API hit rate.
                sleep(1)

            elif user_def_intersection:
                logger.info(c_intersec.name + ' not active on '
                            + str(c_start_t))

        logger.info('Completed data pulling from %s to %s'
                    %(c_start_t, c_end_t))

        try:
            insert_data(conn, c_start_t, c_end_t, table, dupes)
        except psycopg2.Error as exc:
            logger.exception(exc)
            sys.exit(1)

    if pull:
        logger.info('Skipping aggregating and processing volume data')
    else:
        process_data(conn, start_time, end_time)

    logger.info('Done')

if __name__ == '__main__':
    cli()
