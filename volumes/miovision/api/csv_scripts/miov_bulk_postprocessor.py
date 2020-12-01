"""Miovision bulk postprocessor.

Performs the gap finding and masking, and 15-minute TMC and ATR aggregation, of
intersection_tmc.py on either API or CSV data. For API data it also updates
report dates.

Currently cannot perform selective aggregation on a subset of stations.

Examples
--------

The command

```
python miov_bulk_postprocessor.py --path ./config.cfg --start_date '2020-11-01' --end_date '2020-12-01'
```

runs a bulk-aggregation API data from 2020-11-01 to 2020-11-30 inclusive.

```
python miov_bulk_postprocessor.py --path ./config.cfg --csv --start_date '2020-01-01' --end_date '2020-12-01' &> log.txt
```

postprocesses CSV data from 2020-01-01 to 2020-11-30 inclusive and saves the
logger output as a text file.

```
python miov_bulk_postprocess.py --help
```

returns a list of optional arguments.

"""

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


@click.command()
@click.option('--csv', is_flag=True,
              help="If used, post-processes miovision_csv.")
@click.option('--start_date', default='2020-10-01',
              help='format is YYYY-MM-DD for start date')
@click.option('--end_date', default='2020-11-11',
              help=('format is YYYY-MM-DD for end date '
                    '& excluding the day itself'))
@click.option('--postproc_dt', default=30,
              help=('Number of days per block used by the postprocessor.'))
@click.option('--path', default='config_miovision_csv_bot.cfg',
              help='enter the path/directory of the config.cfg file')
def run_api(csv, start_date, end_date, postproc_dt, path):

    CONFIG = configparser.ConfigParser()
    CONFIG.read(path)
    dbset = CONFIG['DBSETTINGS']
    conn = connect(**dbset)
    conn.autocommit = True
    itmc.logger.debug('Connected to DB')

    start_time = dateutil.parser.parse(str(start_date))
    end_time = dateutil.parser.parse(str(end_date))
    itmc.logger.info('Processing from %s to %s' %(start_time, end_time))

    ppc_dt = datetime.timedelta(days=postproc_dt)

    if csv:
        itmc.logger.info('Processing CSV data')
    else:
        itmc.logger.info('Processing API data')

    try:
        aggregate_data_loop(conn, start_time, end_time, ppc_dt, csv)
    except Exception as e:
        itmc.logger.critical(traceback.format_exc())
        sys.exit(1)


def aggregate_data(conn, start_time, end_iteration_time, csv):
    """Aggregate raw data to 15-minute bins."""

    if csv:
        sqlf = {
            'gaps': "SELECT miovision_csv.find_gaps_2020(%s::date, %s::date)",
            'tmc': ("SELECT miovision_csv.aggregate_15_min_tmc_2020"
                    "(%s::date, %s::date)"),
            'atr': ("SELECT miovision_csv.aggregate_15_min_2020"
                    "(%s::date, %s::date)"),
        }
    else:
        sqlf = {
            'gaps': "SELECT miovision_api.find_gaps(%s::date, %s::date)",
            'tmc': ("SELECT miovision_api.aggregate_15_min_tmc"
                    "(%s::date, %s::date)"),
            'atr': ("SELECT miovision_api.aggregate_15_min"
                    "(%s::date, %s::date)"),
            'rep': "SELECT miovision_api.report_dates(%s::date, %s::date)",
        }

    time_period = (start_time.date(), end_iteration_time.date())

    with conn:
        with conn.cursor() as cur:
            cur.execute(sqlf['gaps'], time_period)
            itmc.logger.info(conn.notices[-1])

    itmc.logger.info('Updated gapsize table and found '
                     'gaps exceeding allowable size from %s to %s'
                     %(start_time, end_iteration_time))

    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute(sqlf['tmc'], time_period)
                itmc.logger.info('Aggregated to 15 minute bins')

                cur.execute(sqlf['atr'], time_period)
                itmc.logger.info('Completed data processing for %s',
                                 start_time)

    except psycopg2.Error as exc:
        itmc.logger.exception(exc)
        sys.exit(1)

    # miovision_api also requires we update `report_dates`.
    if not csv:
        with conn:
            with conn.cursor() as cur:
                cur.execute(sqlf['rep'], time_period)
                itmc.logger.info('report_dates done')


def aggregate_data_loop(conn, start_time, end_time, ppc_dt, csv):

    today_date = datetime.datetime.combine(
            datetime.date.today(), datetime.datetime.min.time())

    with conn:
        for c_start_t in itmc.daterange(start_time, end_time, ppc_dt):
            # If the interval is in the future, stop processing (this only
            # happens if the user is being silly).
            if today_date <= c_start_t:
                break
            c_end_t = min(end_time, c_start_t + ppc_dt)

            itmc.logger.info(
                "Aggregating dates " + c_start_t.strftime("%Y-%m-%d")
                + " - " + c_end_t.strftime("%Y-%m-%d"))

            aggregate_data(conn, c_start_t, c_end_t, csv)


if __name__ == '__main__':
    run_api()
