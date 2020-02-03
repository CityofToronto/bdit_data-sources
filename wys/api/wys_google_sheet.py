from __future__ import print_function
from google.oauth2 import service_account
from googleapiclient.discovery import build

import configparser
from psycopg2 import connect
from psycopg2.extras import execute_values
from psycopg2 import sql
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
    if not values:
        LOGGER.warning('No data found.')
    else:
        for row in values:           
            try:                   
                if row[6] and row[8] and row[10] :
                    i = (ward_no, row[0], row[1], row[2], row[3], row[6], row[8], row[10], row[11], row[13])
                    rows.append(i)
                    #LOGGER.info('Reading %s columns of data from Google Sheet', len(row))
                    LOGGER.debug(row)
                else:
                    LOGGER.info('This row is not included: %s', row)
            except (IndexError, KeyError) as err:
                LOGGER.error('An error occurs at %s', row)
                LOGGER.error(err)

    insert = sql.SQL('''INSERT INTO {}.{} (ward_no, location, from_street, to_street, direction, installation_date, removal_date,
                       new_sign_number, comments, confirmed) VALUES %s 
                       ON CONFLICT (installation_date, new_sign_number) DO UPDATE SET 
                       removal_date=EXCLUDED.removal_date, new_sign_number=EXCLUDED.new_sign_number, comments=EXCLUDED.comments
                       ''').format(sql.Identifier(schema_name), sql.Identifier(table_name)) 
    LOGGER.info('Uploading %s rows to PostgreSQL', len(rows))
    LOGGER.debug(rows)

    with con:
        with con.cursor() as cur:
            execute_values(cur, insert, rows)
    LOGGER.info('Table %s is done', table_name)

if __name__ == '__main__':
    CONFIG = configparser.ConfigParser()
    CONFIG.read(r'/home/jchew/local/db.cfg')
    dbset = CONFIG['DBSETTINGS']
    con = connect(**dbset)

    service = build('sheets', 'v4', credentials=credentials, cache_discovery=False)

    read_masterlist(con,service)
 