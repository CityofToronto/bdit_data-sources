#!/usr/bin/python3
'''Copying the phase1 csv datafiles to the PostgreSQL database'''
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
    if filename.endswith('.csv'): 
        
        table = 'inrix.'+os.path.splitext(filename)[0]
        logger.info('Creating table {table}'.format(table=table))
        cursor.execute('CREATE UNLOGGED TABLE IF NOT EXISTS %(table)s (tx timestamp without time zone NOT NULL, tmc char(9) NOT NULL, speed int NOT NULL)', {'table': AsIs(table)})
        #cursor.execute('ALTER TABLE %(table)s OWNER TO rdumas', {'table': AsIs(table)})
        
        con.commit()
        datafile = open(filename)
        logger.info("Copying data from: {filename}".format(filename=datafile.name))
        #Performance tweaks to speed up copying allegedly.
        cursor.execute('SET LOCAL synchronous_commit=off;')
        cursor.execute('TRUNCATE %(table)s ;', {'table': AsIs(table)})
        cursor.copy_expert("COPY %(table)s (tx,tmc,speed) FROM STDIN WITH CSV", {'table': AsIs(table)},datafile)
        con.commit()
        datafile.close()
        
        #Update timestamps on table then copy data into master by pinging 
        logger.info('Updating timestamps on table %s', table)
        cursor.execute("UPDATE %(table)s "
                       "SET tx = (tx AT TIME ZONE 'UTC') AT TIME ZONE 'America/Toronto' ;"
                       , {'table': AsIs(table)})
        con.commit()
        
        #Insert into raw_data, since data are stored in funky tables, using a previously created rule 
        logger.info('Copying data from table %s, to the main table', table)
        cursor.execute("INSERT INTO inrix.raw_data "
                       "SELECT tx, tmc, speed, 30 FROM %(table)s ;"
                       , {'table': AsIs(table)})
        con.commit()
