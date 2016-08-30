#!/usr/bin/python3
'''Copying the csv datafiles to my PostgreSQL database'''
import os
from dbsettings import dbsetting
from psycopg2 import connect
from psycopg2.extensions import AsIs
import logging
import re

#Configure logging
FORMAT = '%(asctime)-15s %(message)s'
logging.basicConfig(level=logging.INFO, format=FORMAT)
logger = logging.getLogger(__name__)

logger.info('Connecting to host: %s database: %s as user %s', 
             dbsetting['host'],
             dbsetting['database'],
             dbsetting['user'])
con = connect(database=dbsetting['database'],host=dbsetting['host'],user=dbsetting['user'],password=dbsetting['password'])
cursor = con.cursor()

#Regex to find YYYYMM in filenames
regex_yyyymm = re.compile(r'\d{6}')
def getYYYYMM (filename):
    res = regex_yyyymm.findall(filename) 
    #Return last 6 digit match in filename
    return res[len(res)-1]

if __name__ = "__main__":
    for filename in os.listdir():
        if filename.endswith('.csv') and bool(regex_yyyymm.search(filename)): 
            yyyymm = getYYYYMM(filename)
            table = 'inrix.raw_data'+yyyymm
            logger.info('Creating table %(table)s', {'table': AsIs(table)})
            cursor.execute('CREATE TABLE IF NOT EXISTS %(table)s () INHERITS (inrix.raw_data)', {'table': AsIs(table)})
            #cursor.execute('ALTER TABLE {table} OWNER TO rdumas'.format(table=table))
            cursor.execute('ALTER TABLE %(table)s SET UNLOGGED', {'table': AsIs(table)})
            con.commit()
            datafile = open(filename)
            logger.info("Copying data from: {filename}".format(filename=datafile.name))
            #Performance tweaks to speed up copying allegedly.
            cursor.execute('SET LOCAL synchronous_commit=off;')
            #Creating a temporary table to COPY data to, and then fix timestamps when inserting into {table}
            cursor.execute('CREATE TEMP TABLE raw_data_import (LIKE inrix.raw_data);')
            cursor.copy_expert("COPY raw_data_import FROM STDIN WITH CSV",datafile)
            cursor.execute('TRUNCATE %(table)s ;', {'table': AsIs(table)})
            cursor.execute('INSERT INTO %(table)s '
                           'SELECT (tx AT TIME ZONE \'UTC\') AT TIME ZONE \'America/Toronto\' AS tx, tmc, speed, score '
                           'FROM raw_data_import;', {'table': AsIs(table)})
            con.commit()
            datafile.close()
            logger.info("Setting table status to logged")
            cursor.execute('ALTER TABLE %(table)s SET LOGGED', {'table': AsIs(table)})
            con.commit()
        