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
default_n_days = 1
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
@click.option('--n_days', type=int, default=default_n_days,
              help=('number of days from the start date to process.'))
@click.option('--path' , default='config_miovision_csv_bot.cfg', help='enter the path/directory of the config.cfg file')
def run_api(start_date, n_days, path):

    CONFIG = configparser.ConfigParser()
    CONFIG.read(path)
    api_key=CONFIG['API']
    key=api_key['key']
    dbset = CONFIG['DBSETTINGS']
    conn = connect(**dbset)
    conn.autocommit = True
    logger.debug('Connected to DB')

    start_date= dateutil.parser.parse(str(start_date))
    start_time=local_tz.localize(start_date)
    logger.info('Processing from {0} for {1} days'
                .format(start_time.strftime("%Y-%m-%d"), n_days))

    try:
        process_data_loop(conn, start_time, n_days)
    except Exception as e:
        logger.critical(traceback.format_exc())
        sys.exit(1)


def process_data_june2020(conn, start_time, end_iteration_time):
    # UPDATE gapsize_lookup TABLE AND RUN find_gaps FUNCTION

    time_period = (start_time, end_iteration_time)

    # with conn:
    #     with conn.cursor() as cur:
    #         invalid_gaps="SELECT miovision_csv.find_gaps_2020(%s::date, %s::date)"
    #         cur.execute(invalid_gaps, time_period)
    #         logger.info(conn.notices[-1])
    # logger.info('Updated gapsize table and found gaps exceeding allowable size')

    # Aggregate to 15min tmc / 15min
    try:
        with conn:
            with conn.cursor() as cur:
                update="SELECT miovision_csv.aggregate_15_min_tmc_june2020(%s::date, %s::date)"
                cur.execute(update, time_period)
                logger.info('Aggregated to 15 minute bins')

                atr_aggregation="SELECT miovision_csv.aggregate_15_min_june2020(%s::date, %s::date)"
                cur.execute(atr_aggregation, time_period)
                logger.info('Completed data processing for %s', start_time)

    except psycopg2.Error as exc:
        logger.exception(exc)
        sys.exit(1)


def dayrange(start_time, n_days):
    """Generator for monthly time increments."""
    start_month = start_time.month
    start_year = start_time.year

    for day_i in range(n_days - 1):
        yield (start_time +  datetime.timedelta(days=day_i),
               start_time + datetime.timedelta(days=(day_i + 1)))


def process_data_loop(conn, start_time, n_months):

    today_date = datetime.datetime.combine(
        datetime.date.today(), datetime.datetime.min.time())

    with conn:
        for (c_start_t, c_end_t) in dayrange(start_time, n_months):
            # If the interval is in the future, stop processing.
            if today_date <= c_start_t:
                break
            # If the end of the interval exceeds the present day, set the end
            # to the present.
            elif today_date < c_end_t:
                c_end_t = today_date

            logger.info(
                "Processing dates " + c_start_t.strftime("%Y-%m-%d")
                + " - " + c_end_t.strftime("%Y-%m-%d"))

            process_data_june2020(conn, c_start_t, c_end_t)


if __name__ == '__main__':
    cli()
