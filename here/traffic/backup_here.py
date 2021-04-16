import psycopg2
import gzip
from psycopg2 import connect
import configparser
import tempfile
import psycopg2.sql as sql
from os import listdir
import subprocess
from os.path import isfile, join
import os
CONFIG=configparser.ConfigParser()
CONFIG.read('/home/nchan/db.cfg')
dbset=CONFIG['DBSETTINGS']
s3=CONFIG['S3SETTINGS']['path']
conn=connect(**dbset)


def backup_to_s3_month(con, month, s3):
    '''Backup a month of ta data to specified s3

    con: pgadmin connect
    month: yyyymm (string) of targeted month to backup
    s3: path to s3
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
        subprocess.check_call(['aws','s3','cp',data_file_path, s3])


month_list= ['201701', '201702', '201703', '201704', '201705', '201706', '201707', '201708', '201709', '201710', '201711', '201712', 
             '201801', '201802', '201803', '201804', '201805', '201806', '201807', '201808', '201809', '201810', '201811', '201812']
    
for i in month_list:       
    backup_to_s3_month(conn, i, s3)        