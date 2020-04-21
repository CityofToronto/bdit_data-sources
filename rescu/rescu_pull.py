import csv
import click
import datetime
import psycopg2
from psycopg2.extras import execute_values
from psycopg2 import connect, Error
import logging
import configparser
import traceback

def logger():
    
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.INFO)
    formatter=logging.Formatter('%(asctime)s     	%(levelname)s    %(message)s', datefmt='%d %b %Y %H:%M:%S')
    logger.handlers.clear()
    stream_handler=logging.StreamHandler()
    stream_handler.setFormatter(formatter)
    logger.addHandler(stream_handler)
    return logger

logger=logger()
logger.debug('Start')

# GET DEFAULT DATES (CHANGE WHEN FINALIZED)
default_start = datetime.date.today() - datetime.timedelta(days=1)
default_end = datetime.date.today() - datetime.timedelta(days=0)

# SET UP CLICK OPTION TO SPECIFY DATES
CONTEXT_SETTINGS = dict(
    default_map={'run_rescu': {'flag': 0}}
)

@click.group(context_settings=CONTEXT_SETTINGS)
def cli():
    pass
@cli.command()
@click.option('--start_date', default=default_start, help='format is YYYY-MM-DD for start date')
@click.option('--end_date' , default=default_end, help='format is YYYY-MM-DD for end date & excluding the day itself') 
@click.option('--path' , default=r'C:\Users\rdumas\Documents\rescu_bot.cfg', help='enter the path/directory of the config.cfg file')

# READ THE RIGHT FILE
def read(start_date, end_date, path):
    # Make them into the right datetime format if they are inputted
    start_str = datetime.datetime.strptime(str(start_date), '%Y-%m-%d').date()
    end_str = datetime.datetime.strptime(str(end_date), '%Y-%m-%d').date()
    time_delta = datetime.timedelta(days=1)
  
    logger.info('Pulling for dates: %s to %s' %(start_str, end_str))

    while True:

        #start_date in format %Y-%m-%d but we need %d-%b-%Y to read file
        start = start_str.strftime("%d-%b-%Y")
        start_input = str(start).upper()
        yr = start_str.strftime("%Y")

        file_path = "\\\\tssrv7\\FlowData\\TextFiles\\Rescu\\AggregatedData\\"+yr+"\\STN_15MINVOL_" + start_input + ".rpt"
        logger.info('Pulling data from filename = %s', file_path)

        #table_dt = datetime.datetime.strptime(start_date, '%d-%b-%Y').strftime('%Y-%m-%d')
        logger.info('Working on dt = %s', start_str)
        lst = [] 

        with open(file_path, "r") as fp:
            for line in fp:
                new_line = line.strip('\n')
                lst.append( ( str(start_str), new_line), )

        logger.debug(lst[0:5])

        try:
            insert(start_str, path, lst)
        except Exception:
            logger.critical(traceback.format_exc())

        start_str += time_delta
        if start_str >= end_str:
            break

# INSERT INTO THE TABLE
def insert(start_str, path, lst):
    CONFIG = configparser.ConfigParser()
    CONFIG.read(path)
    dbset = CONFIG['DBSETTINGS']
    conn = connect(**dbset)
    conn.autocommit = True
    logger.debug('Connected to DB')
    
    conn.notices=[]
    with conn:
        with conn.cursor() as cur:
            insert_data = '''INSERT INTO rescu.raw_15min (dt, raw_info) VALUES %s'''
            execute_values(cur, insert_data, lst)

            if conn.notices != []:
                logger.warning(conn.notices[-1])
    logger.info('Added raw data for dt = %s', start_str)

if __name__ == '__main__':
    cli()
    