import click
import psycopg2
import configparser
from psycopg2 import connect
import dateutil.parser
import traceback
import sys
import datetime
from requests import exceptions
from time import sleep
import math

# Hack to import from parent directory, so don't move this script...
import sys
sys.path.append("../")
import intersection_tmc as itmc


CONTEXT_SETTINGS = dict(
    default_map={'run_api': {'flag': 0}}
)

@click.group(context_settings=CONTEXT_SETTINGS)
def cli():
    pass

@cli.command()
@click.option('--csv', is_flag=True, default=True,
              help="If True (default), post-processes miovision_csv.")
@click.option('--start_date', default='2020-10-01',
              help='format is YYYY-MM-DD for start date')
@click.option('--end_date', default='2020-11-11',
              help=('format is YYYY-MM-DD for end date '
                    '& excluding the day itself'))
@click.option('--gapfinder_dt', default=30,
              help=('Number of days per block used by the gap finder.'))
@click.option('--path' , default='config_miovision_csv_bot.cfg',
              help='enter the path/directory of the config.cfg file')
def run_api(csv, start_date, end_date, gapfinder_dt, path):

    CONFIG = configparser.ConfigParser()
    CONFIG.read(path)
    dbset = CONFIG['DBSETTINGS']
    conn = connect(**dbset)
    conn.autocommit = True
    itmc.logger.debug('Connected to DB')

    start_date = dateutil.parser.parse(str(start_date))
    end_date = dateutil.parser.parse(str(end_date))
    start_time = itmc.local_tz.localize(start_date)
    end_time = itmc.local_tz.localize(end_date)
    itmc.logger.info('Processing from %s to %s' %(start_time, end_time))

    gdt = datetime.timedelta(days=gapfinder_dt)

    if csv:
        sqlf = {
            'gaps': "miovision_csv.find_gaps_2020",
            'tmc': "miovision_csv.aggregate_15_min_tmc_2020",
            'atr': "miovision_csv.aggregate_15_min_2020"
        }
        itmc.logger.info('Processing CSV data')
    else:
        sqlf = {
            'gaps': "miovision_api.find_gaps",
            'tmc': "miovision_api.aggregate_15_min_tmc",
            'atr': "miovision_api.aggregate_15_min"
        }
        itmc.logger.info('Processing CSV data')

    try:
        find_gaps_loop(conn, sqlf, start_time, end_time, gdt)
    except Exception as e:
        itmc.logger.critical(traceback.format_exc())
        sys.exit(1)

    try:
        aggregate_data_loop(conn, sqlf, start_time, end_time)
    except Exception as e:
        itmc.logger.critical(traceback.format_exc())
        sys.exit(1)


def find_gaps(conn, sqlf, start_time, end_iteration_time):
    """UPDATE gapsize_lookup TABLE AND RUN find_gaps FUNCTION"""
    time_period = (start_time, end_iteration_time)

    with conn:
        with conn.cursor() as cur:
            invalid_gaps = "SELECT %s(%s::date, %s::date)"
            print(invalid_gaps)
            #cur.execute(sqlf['gaps'], invalid_gaps, time_period)
            itmc.logger.info(conn.notices[-1])

    itmc.logger.info('Updated gapsize table and found '
                     'gaps exceeding allowable size from %s to %s'
                     %(start_time, end_iteration_time))


def daterange_cap(start_time, end_time, dt):
    """Generator for a sequence of regular time periods, with a shorter last
    period if dt does not divide evenly into end_time - start_time."""
    n_periods = math.ceil((end_time - start_time) / dt)
    for i in range(n_periods):
        c_start_t = start_time + i * dt
        if i + 1 == n_periods:
            yield (c_start_t, end_time)
        else:
            yield (c_start_t, c_start_t + dt)


def find_gaps_loop(conn, sqlf, start_time, end_time, gdt):

    today_date = datetime.datetime.combine(
        datetime.date.today(), datetime.datetime.min.time())

    with conn:
        for (c_start_t, c_end_t) in daterange_cap(
                start_time, end_time, gdt):
            # If the interval is in the future, stop processing.
            if today_date <= c_start_t:
                break
            # If the end of the interval exceeds the present day, set the end
            # to the present.
            elif today_date < c_end_t:
                c_end_t = today_date

            find_gaps(conn, sqlf, c_start_t, c_end_t)


def aggregate_data(conn, sqlf, start_time, end_iteration_time):
    """Aggregate to 15min tmc / 15min"""

    time_period = (start_time, end_iteration_time)

    try:
        with conn:
            with conn.cursor() as cur:
                update = "SELECT %s(%s::date, %s::date)"
                print(update)
                #cur.execute(sqlf['tmc'], update, time_period)
                itmc.logger.info('Aggregated to 15 minute bins')

                atr_aggregation = "SELECT %s(%s::date, %s::date)"
                print(update)
                #cur.execute(sqlf['atr'], atr_aggregation, time_period)
                itmc.logger.info('Completed data processing for %s',
                                 start_time)

    except psycopg2.Error as exc:
        itmc.logger.exception(exc)
        sys.exit(1)


def daterange(start_time, end_time, dt):
    """Generator for a sequence of regular time periods."""
    for i in range(round((end_time - start_time) / dt)):
        c_start_t = start_time + i * dt
        yield (c_start_t, c_start_t + dt)


def aggregate_data_loop(conn, sqlf, start_time, end_time):

    today_date = datetime.datetime.combine(
        datetime.date.today(), datetime.datetime.min.time())
    dt = datetime.timedelta(days=1)

    with conn:
        for (c_start_t, c_end_t) in daterange(
                start_time, end_time, dt):
            # If the interval is in the future, stop processing.
            if today_date <= c_start_t:
                break
            # If the end of the interval exceeds the present day, set the end
            # to the present.
            elif today_date < c_end_t:
                c_end_t = today_date

            itmc.logger.info(
                "Aggregating dates " + c_start_t.strftime("%Y-%m-%d")
                + " - " + c_end_t.strftime("%Y-%m-%d"))

            aggregate_data(conn, sqlf, c_start_t, c_end_t)


if __name__ == '__main__':
    cli()
