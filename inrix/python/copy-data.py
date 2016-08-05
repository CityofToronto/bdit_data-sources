#!/usr/bin/python3
'''Copying the csv datafiles to my PostgreSQL database'''
import os
from dbsettings import dbsetting
from psycopg2 import connect
import logging
import re

#Configure logging
FORMAT = '%(asctime)-15s %(message)s'
logging.basicConfig(level=logging.INFO, format=FORMAT)
logger = logging.getLogger(__name__)

logger.info('Connecting to host:{host} database: {database} with user {user}'.format(database=dbsetting['database'],host=dbsetting['host'],user=dbsetting['user']))
con = connect(database=dbsetting['database'],host=dbsetting['host'],user=dbsetting['user'],password=dbsetting['password'])
cursor = con.cursor()

#Regex to find YYYYMM in filenames
regex_yyyymm = re.compile(r'\d{6}')
def getYYYYMM (filename):
    res = regex_yyyymm.findall(filename) 
    #Return last 6 digit match in filename
    return res[len(res)-1]

for filename in os.listdir():
    if filename.endswith('.csv') and bool(regex_yyyymm.search(filename)): 
        yyyymm = getYYYYMM(filename)
        table = 'inrix.rawdata'+yyyymm
        logger.info('Creating table {table}'.format(table=table))
        cursor.execute('CREATE TABLE IF NOT EXISTS {table} () INHERITS (inrix.raw_data)'.format(table=table))
        #cursor.execute('ALTER TABLE {table} OWNER TO rdumas'.format(table=table))
        cursor.execute('ALTER TABLE {table} SET UNLOGGED'.format(table=table))
        con.commit()
        datafile = open(filename)
        logger.info("Copying data from: {filename}".format(filename=datafile.name))
        #Performance tweaks to speed up copying allegedly.
        cursor.execute('SET LOCAL synchronous_commit=off;')
        cursor.execute('TRUNCATE {table} ;'.format(table=table))
        cursor.copy_expert("COPY {table} FROM STDIN WITH CSV".format(table=table),datafile)
        con.commit()
        datafile.close()
        logger.info("Setting table status to logged")
        cursor.execute('ALTER TABLE {table} SET LOGGED'.format(table=table))
        con.commit()
        