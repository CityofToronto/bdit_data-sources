
# import csv
# from datetime import date as dt
# import datetime 

# date = input ("Enter date (DD-MMM-YYYY) : ") 

# #but for now the IF block is not working @@
# if date is None:
#     today = dt.today()
#     yesterday = today - datetime.timedelta(days=1)
#     final_date = yesterday.strftime("%d-%b-%Y")
#     print(final_date)
#     # depends on the format of the file though
#     file_path = '/home/jchew/local/rescu/rescu_files/STN_15MINVOL_' + str(final_date) + '.rpt'
# else:
#     file_path = '/home/jchew/local/rescu/rescu_files/STN_15MINVOL_' + date + '.rpt'

# with open(file_path, "r") as f:
#     reader = csv.reader(f, delimiter = ',')
#     lst = []
#     for row in reader:
#         lst.append(row[0])
#         # print (row[0])

# print (lst[0:5])

#********
import csv
import click
import datetime
import dateutil.parser
import psycopg2
from psycopg2.extras import execute_values
from psycopg2 import connect, Error
import logging
import configparser
import traceback

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

logger=logger()
logger.debug('Start')

# GET DATES
ytrday = datetime.date.today() - datetime.timedelta(days=3)
# put it into the right date format and then upper case them
start = ytrday.strftime("%d-%b-%Y")
default_start = str(start).upper()
logger.debug(default_start)

tday = datetime.date.today()
end = tday.strftime("%d-%b-%Y")
default_end = str(end).upper()
logger.debug(default_end)

# SET UP CLICK OPTION TO SPECIFY DATES
CONTEXT_SETTINGS = dict(
    default_map={'run_api': {'flag': 0}}
)

@click.group(context_settings=CONTEXT_SETTINGS)
def cli():
    pass

@cli.command()
@click.option('--start_date', default=default_start, help='format is DD-MMM-YYYY for start date')
@click.option('--end_date' , default=default_end, help='format is DD-MMM-YYYY for end date & excluding the day itself') 
@click.option('--path' , default='/home/jchew/local/db.cfg', help='enter the path/directory of the config.cfg file')

# READ THE RIGHT FILE
def read(start_date, end_date, path):
    # start_date= dateutil.parser.parse(str(start_date))
    # end_date= dateutil.parser.parse(str(end_date))
    logger.info('Pulling for dates: %s to %s' %(start_date, end_date))

    file_path = '/home/jchew/local/rescu/rescu_files/STN_15MINVOL_' + default_start + '.rpt'
    logger.info('Pulling data from filename = %s', file_path)

    with open(file_path, "r") as f:
        reader = csv.reader(f, delimiter = ',')
        lst = []
        for row in reader:
            lst.append(row[0]) # Having a list of strings instead of a list with list for each row
    logger.debug(lst[0:5])

    try:
        pull(start_date, end_date, path, lst)
    except Exception:
        logger.critical(traceback.format_exc())


# INSERT INTO THE TABLE
def pull(start_date, end_date, path, lst):
    CONFIG = configparser.ConfigParser()
    CONFIG.read(path)
    dbset = CONFIG['DBSETTINGS']
    conn = connect(**dbset)
    conn.autocommit = True
    logger.debug('Connected to DB')
    
    table_dt = datetime.datetime.strptime(start_date, '%d-%b-%Y').strftime('%Y-%m-%d')
    logger.debug('dt inserted is %s', str(table_dt))
    
    conn.notices=[]
    with conn:
        with conn.cursor() as cur:
            for each in lst[0:5]:
                # insert_dt = '''INSERT INTO jchew.rescu_raw_15min (dt) VALUES %s'''
                # cur.execute(insert_dt, str(table_dt))
                # #execute_values(cur, insert_dt, str(table_dt))
                # logger.debug('Added date')
                insert_data = '''INSERT INTO jchew.rescu_raw_15min (raw_info) VALUES %s'''
                execute_values(cur, insert_data, str(each))
                if conn.notices != []:
                    logger.warning(conn.notices[-1])
            logger.debug('Added data')

if __name__ == '__main__':
    cli()