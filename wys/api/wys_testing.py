# WORK IN PROGRESS!!!
from __future__ import print_function
from google.oauth2 import service_account
from googleapiclient.discovery import build

import configparser
from psycopg2 import connect
from psycopg2.extras import execute_values
from psycopg2 import sql
from psycopg2 import Error

from datetime import datetime
import logging 

SCOPES = ['https://www.googleapis.com/auth/spreadsheets.readonly']

SERVICE_ACCOUNT_FILE = '/home/jchew/local/vz_key.json' 

credentials = service_account.Credentials.from_service_account_file(
        SERVICE_ACCOUNT_FILE, scopes=SCOPES)   

LOGGER = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

def read_masterlist(con, service):
    dict_table = {}
    ward_list = []
    with con:
        with con.cursor() as cur:
            read_table='''SELECT spreadsheet_id, range_name, schema_name, table_name FROM wys.ward_masterlist ORDER BY ward_no'''
            cur.execute(read_table)
            ward_list=cur.fetchall()
            LOGGER.debug(ward_list)

    for i in range(25):
        dict_table.update({i+1:ward_list[i]})
        LOGGER.info('Working on ward %s', i+1)
        pull_from_sheet(con, service, dict_table, dict_table[i+1])
    
    LOGGER.info('Completed')

def pull_from_sheet(con, service, dict_table, ward, *args):
    spreadsheet_id = str(ward[0])
    range_name = str(ward[1])
    schema_name = str(ward[2])
    table_name = str(ward[3])
    ward_no = (str(ward[3])).split('_',1)[1]
    
    sheet = service.spreadsheets()
    result = sheet.values().get(spreadsheetId=spreadsheet_id, range=range_name).execute()
    values = result.get('values', [])

    rows = []
    badrows = []
    if not values:
        LOGGER.warning('No data found.')
    else:
        for row in values:           
            try:             
                if row[6] and row[8] and row[10]:
                    try: 
                        installation = datetime.strptime(row[6], '%m/%d/%Y').date()
                        removal = datetime.strptime(row[8], '%m/%d/%Y').date()
                        i = (ward_no, row[0], row[1], row[2], row[3], installation, removal, row[10], row[11], row[13])
                        rows.append(i)
                        LOGGER.debug(row)
                    except:
                        i = (ward_no, row[0], row[1], row[2], row[3], row[6], row[8], row[10], row[11], row[13])
                        badrows.append(i)
                        LOGGER.warning('Date format is not MM/DD/YYYY')
                else:
                    LOGGER.debug('This row is not included: %s', row)
            except (IndexError, KeyError) as err:
                LOGGER.error('An error occurs at %s', row)
                LOGGER.error(err)
        LOGGER.warning('Please fix date formats for %s rows in %s at %s', len(badrows), table_name, badrows)

    insert = sql.SQL('''INSERT INTO {}.{} (ward_no, location, from_street, to_street, direction, installation_date, removal_date,
                       new_sign_number, comments, confirmed) VALUES %s 
                       ON CONFLICT (installation_date, new_sign_number) DO UPDATE SET 
                       removal_date=EXCLUDED.removal_date, new_sign_number=EXCLUDED.new_sign_number, comments=EXCLUDED.comments
                       ''').format(sql.Identifier(schema_name), sql.Identifier(table_name)) 
    LOGGER.info('Uploaded %s rows to PostgreSQL for %s', len(rows), table_name)
    LOGGER.debug(rows)

    try:
        with con:
            with con.cursor() as cur:
                execute_values(cur, insert, rows)
        LOGGER.info('Table %s is done', table_name)
    except Error as err2:
        LOGGER.error('Duplicates of primary keys in %s !!!', table_name)
        LOGGER.error(err2)
        

if __name__ == '__main__':
    CONFIG = configparser.ConfigParser()
    CONFIG.read(r'/home/jchew/local/db.cfg')
    dbset = CONFIG['DBSETTINGS']
    con = connect(**dbset)

    service = build('sheets', 'v4', credentials=credentials, cache_discovery=False)

    read_masterlist(con,service)
 