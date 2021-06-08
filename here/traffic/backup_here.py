import psycopg2
import gzip
from psycopg2 import connect
import configparser
import tempfile
import logging
import psycopg2.sql as sql
from os import listdir
import subprocess
from os.path import isfile, join
import os
CONFIG=configparser.ConfigParser()
CONFIG.read('/home/nchan/db.cfg')
dbset=CONFIG['DBSETTINGS']
s3=CONFIG['S3']['path']
conn=connect(**dbset)
LOGGER = logging.getLogger(__name__)

def backup_to_s3_month(con, month, s3):
    '''Backup a month of ta data to specified s3

    con: pgadmin connect
    month: yyyymm (string) of targeted month to backup
    s3: path to s3's here/traffic folder
    '''
    with tempfile.TemporaryDirectory() as tempdir:
        #backup month data to compressed file
        copy_query = sql.SQL("COPY here.{} TO STDOUT WITH (FORMAT 'csv', HEADER TRUE)")
        #compress results of the copy command to a file in the compressed directory
        os.chdir(tempdir)
        data_file_path = 'here_{}.csv.gz'.format(month)
        with gzip.GzipFile(filename=data_file_path, mode='w') as data_file:
            with con.cursor() as cur:
                cur.copy_expert(copy_query.format(sql.Identifier('ta_'+month)), data_file)      
        #copy file to s3 bucket
        subprocess.check_call(['aws','s3','cp', data_file_path, s3])



month_list= ['201901','201902', '201903', '201904', '201905', '201906', '201907', '201908', '201909', '201910', '201911', '201912']   
for month in month_list:
    LOGGER.info('Processing %s', str(month))           
    backup_to_s3_month(conn, month, s3)  