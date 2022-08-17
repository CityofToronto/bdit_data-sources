from pathlib import Path
import configparser
from psycopg2 import connect
from psycopg2.extras import execute_values
import logging
CONFIG = configparser.ConfigParser()
CONFIG.read(str(Path.home().joinpath('db.cfg')))
dbset = CONFIG['DBSETTINGS']
con = connect(**dbset)
#Configure logging
FORMAT = '%(asctime)-15s %(message)s'
logging.basicConfig(level=logging.INFO, format=FORMAT)
LOGGER = logging.getLogger(__name__)

yyyy_list = ['2012', '2013', '2014', '2015', '2016', '2017', '2018']
mm_list = ['01', '02', '03', '04', '05', '06', '07', '08', '09', '10', '11', '12']

for y in yyyy_list:
    LOGGER.info('Begin processing year %s', y)
    for m in mm_list:
        LOGGER.info('Processing month %s of year %s', m, y)
        with con:
            with con.cursor() as cur:
                cur.execute("SELECT here.move_data(%s, %s);", (y,m))
        LOGGER.info('%s-%s Processed', y, m)
    LOGGER.info('Year %s processed', y)
    
    