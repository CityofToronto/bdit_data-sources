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
from collections import namedtuple

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


logger = logger()
logger.debug('Start')

time_delta = datetime.timedelta(days=1)
default_start = str(datetime.date.today()-time_delta)
default_end = str(datetime.date.today())

session = Session()
session.proxies = {}
url = 'https://api.miovision.com/intersections/'
tmc_endpoint = '/tmc'
ped_endpoint = '/crosswalktmc'


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


# Entrance-exit pairs.
EEPair = namedtuple('EEPair', ['entrance', 'exit'])


class MiovPuller:
    """Miovision API puller.

    Basic workflow is to initialize the class, then use `get_intersection` to
    pull TMC and crosswalk data for an intersection over one period of time.

    Parameters
    ----------
    int_id1 : str
        Intersection hex-ID (`id` in `miovision_api.intersections`).
    int_uid : str
        Intersection UID (`intersection_uid` in `miovision_api.intersections`)
    key : str
        Miovision API access key.

    """

    headers = {'Content-Type': 'application/json',
               'Authorization': ''}

    tmc_template = url + "{int_id1}" + tmc_endpoint
    ped_template = url + "{int_id1}" + ped_endpoint

    roaduser_class = {
        'Light': '1',
        'BicycleTMC': '2',
        'Bus': '3',
        'SingleUnitTruck': '4',
        'ArticulatedTruck': '5',
        'WorkVan': '8',
        'MotorizedVehicle': '9',
        'Bicycle': '10'
    }

    # Lookup table for all TMC movements that are not u-turns.
    tmc_movements_no_uturn = {
        EEPair(entrance='N', exit='S'): '1',
        EEPair(entrance='S', exit='N'): '1',
        EEPair(entrance='W', exit='E'): '1',
        EEPair(entrance='E', exit='W'): '1',
        EEPair(entrance='S', exit='W'): '2',
        EEPair(entrance='N', exit='E'): '2',
        EEPair(entrance='W', exit='N'): '2',
        EEPair(entrance='E', exit='S'): '2',
        EEPair(entrance='S', exit='E'): '3',
        EEPair(entrance='E', exit='N'): '3',
        EEPair(entrance='N', exit='W'): '3',
        EEPair(entrance='W', exit='S'): '3',
    }

    crosswalkuser_class = {
        'Pedestrian': '6',
        'Bicycle': '7'
    }

    crosswalk_mvmts = {
        'CW': '5',
        'CCW': '6'
    }

    def __init__(self, int_id1, int_uid, key):
        self.headers['Authorization'] = key
        self.int_id1 = int_id1
        self.intersection_uid = int_uid

    def get_response(self, apitype, start_time, end_iteration_time):
        """Requests data from API."""

        params = {'endTime': (end_iteration_time
                              - datetime.timedelta(milliseconds=1)),
                  'startTime': start_time}

        # Select the appropriate request URL depending on which API we're
        # pulling from.
        if apitype == 'crosswalk':
            request_url = self.ped_template.format(int_id1=self.int_id1)
        elif apitype == 'tmc':
            request_url = self.tmc_template.format(int_id1=self.int_id1)
        else:
            raise ValueError("apitype must be either 'tmc' or 'crosswalk'!")

        response = session.get(
            request_url, params=params, headers=self.headers,
            proxies=session.proxies)

        # Return if we get a success response code, or raise an error if not.
        if response.status_code == 200:
            return response
        elif response.status_code == 404:
            error = json.loads(response.content.decode('utf-8'))
            logger.error("Problem with ped call for intersection %s",
                         self.int_id1)
            logger.error(error['error'])
            raise NotFoundError
        elif response.status_code == 400:
            logger.critical(("Bad request error when pulling ped data for "
                             "intersection {0} from {1} until {2}")
                            .format(self.int_id1, start_time,
                                    end_iteration_time))
            error = json.loads(response.content.decode('utf-8'))
            logger.critical(error['error'])
            sys.exit(5)
        elif response.status_code == 504:
            raise TimeoutException('Error' + str(response.status_code))
        elif response.status_code == 500:
            raise ServerException('Error' + str(response.status_code))

        # If code isn't handled in the block above, throw a general error.
        logger.critical('Unknown error pulling ped data for intersection %s',
                        self.int_id1)
        raise MiovisionAPIException('Error' + str(response.status_code))

    def get_road_class(self, row):
        """Get road user class."""

        is_approach = ((row['entrance'] == 'UNDEFINED')
                       or (row['exit'] == 'UNDEFINED'))
        # Second check isn't strictly necessary but better safe than sorry.
        if not is_approach and (row['class'] == 'Bicycle'):
            ru_class = 'BicycleTMC'
        else:
            ru_class = row['class']

        try:
            return self.roaduser_class[ru_class]
        except KeyError:
            raise ValueError("vehicle class {0} not recognized!"
                             .format(row['class']))

    def get_road_leg_and_movement(self, row):
        """Get intersection leg and movement UID.

        Due to bike approach counts, these two data are
        coupled.
        """
        # Classes 7 and 8 are for bike approach volumes.
        if row['exit'] == 'UNDEFINED':
            return (row['entrance'], '7')
        if row['entrance'] == 'UNDEFINED':
            return (row['exit'], '8')
        if row['entrance'] == row['exit']:
            return (row['entrance'], '4')
        movement = self.tmc_movements_no_uturn[
            EEPair(entrance=row['entrance'], exit=row['exit'])]
        return (row['entrance'], movement)

    def get_crosswalk_class(self, row):
        """Get crosswalk road user class."""
        try:
            return self.crosswalkuser_class[row['class']]
        except KeyError:
            raise ValueError("crosswalk class {0} not recognized!"
                             .format(row['class']))

    def get_crosswalk_movement(self, row):
        """Get crosswalk movement."""
        try:
            return self.crosswalk_mvmts[row['direction']]
        except KeyError:
            raise ValueError("crosswalk movement {0} not recognized!"
                             .format(row['direction']))

    def process_tmc_row(self, row):
        """Process one row of TMC API output."""

        classification = self.get_road_class(row)
        (leg, movement) = self.get_road_leg_and_movement(row)

        # Return time, classification_uid, leg, movement, volume.
        return (row['timestamp'], classification, leg, movement, row['qty'])

    def process_crosswalk_row(self, row):
        """Process one row of crosswalk API output."""
        classification = self.get_crosswalk_class(row)
        movement = self.get_crosswalk_movement(row)

        # Return time, classification_uid, leg, movement, volume.
        return (row['timestamp'], classification, row['crosswalkSide'],
                movement, row['qty'])

    def process_response(self, apitype, response):
        """Process the output of self.get_response."""
        data = json.loads(response.content.decode('utf-8'))
        if apitype == 'crosswalk':
            return [(self.intersection_uid, )
                    + self.process_crosswalk_row(row) for row in data]
        return [(self.intersection_uid, )
                + self.process_tmc_row(row) for row in data]

    def get_intersection(self, start_time, end_iteration_time):
        """Get all data for one intersection between start and end time."""
        response_tmc = self.get_response('tmc', start_time, end_iteration_time)
        table_veh = self.process_response('tmc', response_tmc)
        response_crosswalk = self.get_response(
            'crosswalk', start_time, end_iteration_time)
        table_ped = self.process_response('crosswalk', response_crosswalk)

        return table_veh, table_ped

def process_data(conn, start_time, end_iteration_time, user_def_intersection, intersections):
    """Process Miovision into aggregate tables.
    
    Tables: volumes_15min_mvt, volumes_15min, unacceptable_gaps, volumes_daily, report_dates.
    """
    time_period = (start_time, end_iteration_time)
    aggregate_15_min_mvt(conn, time_period, user_def_intersection, intersections)
    aggregate_15_min(conn, time_period)
    find_gaps(conn, time_period)
    aggregate_volumes_daily(conn, time_period)
    get_report_dates(conn, time_period)
 
def find_gaps(conn, time_period):
    """Process aggregated miovision data from volumes_15min_mvt to identify gaps and insert
    into miovision_api.unacceptable_gaps. miovision_api.find_gaps function contains a delete clause."""
    try:
        with conn:
            with conn.cursor() as cur:
                invalid_gaps="SELECT miovision_api.find_gaps(%s::date, %s::date)"
                cur.execute(invalid_gaps, time_period)
                logger.info(conn.notices[-1])
                logger.info('Updated gapsize table and found gaps exceeding allowable size')
    except psycopg2.Error as exc:
        logger.exception(exc)

def aggregate_15_min_mvt(
        conn, time_period, user_def_intersection = False, intersections = None
):
    """Process data from raw volumes table into miovision_api.volumes_15min_mvt.
    
    First clears previous inserts.
    Separate branches for single intersection or all intersection data. 
    """
    try:
        with conn:
            with conn.cursor() as cur:
                query_params = time_period + ([x.uid for x in intersections], )
                delete_sql="SELECT miovision_api.clear_15_min_mvt(%s::timestamp, %s::timestamp, %s::integer[]);"
                cur.execute(delete_sql, query_params)
                
                #if intersections specified, aggregate this step one at a time
                # with different single intersection function
                if user_def_intersection:
                    for c_intersec in intersections:
                        query_params = time_period + (c_intersec.uid, )
                        update="""SELECT miovision_api.aggregate_15_min_mvt_single_intersection(
                                    %s::date, %s::date, %s::int);"""
                        cur.execute(update, query_params)
                        logger.info('Aggregated intersection %s to 15 minute movement bins',
                                    c_intersec.uid)
                else: 
                    update="SELECT miovision_api.aggregate_15_min_mvt(%s::date, %s::date);"
                    cur.execute(update, time_period)
                    logger.info('Aggregated to 15 minute movement bins')
    except psycopg2.Error as exc:
        logger.exception(exc)

def aggregate_15_min(conn, time_period):
    """Aggregate into miovision_api.volumes_15min.

    First clears previous inserts for the date range and
    sets processed column to null for corresponding values in
    volumes_15min_mvt. 
    """
    try:
        with conn:
            with conn.cursor() as cur:
                delete_sql="SELECT miovision_api.clear_volumes_15min(%s::timestamp, %s::timestamp);"
                cur.execute(delete_sql, time_period)
                atr_aggregation="SELECT miovision_api.aggregate_15_min(%s::date, %s::date)"
                cur.execute(atr_aggregation, time_period)
                logger.info('Completed data processing for %s to %s', time_period[0], time_period[1])
    except psycopg2.Error as exc:
        logger.exception(exc)

def aggregate_volumes_daily(conn, time_period):
    """Aggregate into miovision_api.volumes_daily.

    Data is cleared from volumes_daily prior to insert.
    """
    try:
        with conn:
            with conn.cursor() as cur:
                #this function includes a delete query preceeding the insert.
                daily_aggregation="SELECT miovision_api.aggregate_volumes_daily(%s::date, %s::date)"
                cur.execute(daily_aggregation, time_period)
                logger.info('Aggregation into daily volumes table complete')
    except psycopg2.Error as exc:
        logger.exception(exc)

def get_report_dates(conn, time_period):
    """Aggregate into miovision_api.report_Dates.

    First clears previous inserts for the date range.
    """
    try:
        with conn.cursor() as cur:
            delete_sql="""
                WITH deleted AS (
                    DELETE FROM miovision_api.report_dates
                    WHERE dt >= %s::date
                    AND dt < %s::date
                    RETURNING *
                )
                -- FOR NOTICE PURPOSES ONLY
                SELECT COUNT(*) INTO n_deleted
                FROM deleted;

                RAISE NOTICE 'Deleted %s rows from miovision_api.report_dates.', n_deleted;
            """
            cur.execute(delete_sql, time_period)
            report_dates="SELECT miovision_api.get_report_dates(%s::date, %s::date)"
            cur.execute(report_dates, time_period)
            logger.info('report_dates done')
    except psycopg2.Error as exc:
        logger.exception(exc)

def insert_data(conn, time_period, table, dupes, intersections):
    """Inserts new data into miovision_api.volumes, pulled from API.

    Clears table prior to insert.
    Separate branches for single intersection or many intersection case.
    """
    conn.notices=[]
    with conn:
        with conn.cursor() as cur:
            #delete the specified intersections
            delete_sql="SELECT miovision_api.clear_volumes(%s::timestamp, %s::timestamp, %s::integer[]);"
            query_params = time_period + ([x.uid for x in intersections], )
            cur.execute(delete_sql, query_params)
            insert_data = '''INSERT INTO miovision_api.volumes(intersection_uid, datetime_bin, classification_uid,
                             leg,  movement_uid, volume) VALUES %s'''
            execute_values(cur, insert_data, table)
            if conn.notices != []:
                logger.warning(conn.notices[-1])
                if dupes:
                    sys.exit(2)

    with conn:
        with conn.cursor() as cur:
            query_params = time_period + ([x.uid for x in intersections], )
            delete_sql="SELECT miovision_api.clear_api_log(%s::date, %s::date, %s::integer[]);"
            cur.execute(delete_sql, query_params)
            api_log="SELECT miovision_api.api_log(%s::date, %s::date, %s::integer[])"
            cur.execute(api_log, query_params)

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
    """Returns a list of specified intersections. 

    Defaults to all intersections from miovision_api.intersections if none specified.
    """
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

def check_dst(start_time, end_time):
    'check if fall back (EDT -> EST) occured between two timestamps.'
    tz = pytz.timezone("EST5EDT")
    tz_1 = start_time.astimezone(tz).tzname()
    tz_2 = end_time.astimezone(tz).tzname()
    if tz_1 == 'EDT' and tz_2 == 'EST':
        logger.info(f"EDT -> EST time change occured today.")
        return True
    return False

def pull_data(conn, start_time, end_time, intersection, path, pull, key, dupes):
    """Pulls data from Miovision API for the specified range and intersection(s) and inserts into volumes table.
    
    Optionally aggregates raw data into downstream aggregate tables if pull=True. 
    """
    time_delta = datetime.timedelta(hours=6)

    intersections = get_intersection_info(conn, intersection=intersection)

    if len(intersections) == 0:
        logger.critical('No intersections found in '
                        'miovision_api.intersections for the specified '
                        'start time.')
        sys.exit(3)

    # So we don't make the comparison thousands of times below.
    user_def_intersection = len(intersection) > 0

    check_active = []
    for c_start_t in daterange(start_time, end_time, time_delta):
        active_intersections = [x.is_active(c_start_t) for x in intersections]
        check_active.extend(active_intersections)

    if not(any(check_active)):
        if len(intersections) == 0:
            logger.critical('No intersections found.')
        elif user_def_intersection:
            logger.critical('None of the specified intersections are active '
                            'during the specified period.')
        else:
            logger.critical('No active intersections found in '
                            'miovision_api.intersections for the specified '
                            'period.')
        sys.exit(3)

    for c_start_t in daterange(start_time, end_time, time_delta):

        c_end_t = c_start_t + time_delta
        table = []

        for c_intersec in intersections:

            if c_intersec.is_active(c_start_t):
                logger.info(c_intersec.name + '     ' + str(c_start_t))
                miovpull = MiovPuller(c_intersec.id1, c_intersec.uid, key)

                for _ in range(3):
                    try:
                        table_veh, table_ped = miovpull.get_intersection(
                            c_start_t, c_end_t)
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

                #if edt -> est occured in the current range
                #discard the 2nd 1AM hour of data to avoid duplicates
                if check_dst(c_start_t, c_end_t):
                    logger.info('Deleting records for 1AM, UTC-05:00 to prevent duplicates.')
                    table_veh = [x for x in table_veh if
                        datetime.fromisoformat(x[1]).hour != 1 and
                        datetime.fromisoformat(x[1]).tzname() != 'UTC-05:00']
                    table_ped = [x for x in table_ped if
                        datetime.fromisoformat(x[1]).hour != 1 and
                        datetime.fromisoformat(x[1]).tzname() != 'UTC-05:00']
                    
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
            insert_data(conn, c_start_t, c_end_t, table, dupes, intersections)
        except psycopg2.Error as exc:
            logger.exception(exc)
            sys.exit(1)

    if pull:
        logger.info('Skipping aggregating and processing volume data')
    else:
        process_data(conn, start_time, end_time, user_def_intersection, intersections)

    logger.info('Done')

if __name__ == '__main__':
    cli()