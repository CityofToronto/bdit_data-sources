import os
from __future__ import print_function
import json

from google.oauth2 import service_account
from googleapiclient.discovery import build, Resource
from googleapiclient.errors import HttpError

import configparser
from psycopg2 import connect
from psycopg2.extras import execute_values
from psycopg2 import sql
from psycopg2 import Error

from datetime import datetime
import logging 
from time import sleep

from airflow.exceptions import AirflowFailException
from airflow.providers.postgres.hooks.postgres import PostgresHook

LOGGER = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

SQL_DIR = os.path.join(os.path.dirname(os.path.abspath(os.path.dirname(__file__))), 'sql')

def read_masterlist(con, service):
    dict_table = {}
    ward_list = []
    with con.cursor() as cur:
        read_table='''SELECT spreadsheet_id, range_name, schema_name, table_name FROM wys.ward_masterlist ORDER BY ward_no'''
        cur.execute(read_table)
        ward_list=cur.fetchall()
        LOGGER.debug(ward_list)

    empty_wards = []
    for i in range(25):
        dict_table.update({i+1:ward_list[i]})
        LOGGER.info('Working on ward %s', i+1)
        if not pull_from_sheet(con, service, dict_table[i+1]):
            empty_wards.append(i+1)

    LOGGER.info('Completed')
    return empty_wards

def pull_from_sheet(
    con: PostgresHook,
    service: Resource,
    ward: list
) -> bool:
    """Pulls WYS data of a ward and insert into the database
    
    This function calls the Google Sheets API, pulls values from the Sheet using ``service``
    and pushes them into the postgres table ``wys.ward_<ward_no>`` using ``con``.

    Args:
        con: Connection details to bigdata database.
        service: Credentials to access google sheets.
        ward: Ward's details of spreadsheet_id, range_name, schema_name, and table_name

    Returns:
        True if it succeeds to pull/insert data for the given ward; otherwise, False.
    """
    spreadsheet_id = str(ward[0])
    range_name = str(ward[1])
    ward_no = ward[3]
    table_name = f"ward_{ward_no}"
    
    values = None
    for _ in range(3):
        try:
            sheet = service.spreadsheets()
            request = sheet.values().get(spreadsheetId=spreadsheet_id, range=range_name)
            result = request.execute()
            values = result.get('values', [])
        except TimeoutError:
            LOGGER.error("Cannot access: " + json.loads(request.to_json())["uri"])
        except HttpError as err3:
            sleep(120)
        else:
            break
    else: 
        LOGGER.warning('Attempts exceeded.')

    rows = []
    badrows = []
    if not values:
        LOGGER.warning(f"No data was found for ward {ward_no}")
        return False
    else:
        for row in values:           
            try:             
                #check installation_date and new_sign_number
                if row[6] and row[10]: 
                    try: 
                        installation = datetime.strptime(row[6], '%m/%d/%Y').date()
                        #change: add records even without removal date. 
                        if row[8]:
                            removal = datetime.strptime(row[8], '%m/%d/%Y').date()
                        else:
                            removal = None
                        i = (
                            ward_no, row[0], row[1], row[2], row[3], installation,
                            removal, row[10], row[11], int(row[7]), row[13])
                        rows.append(i)
                        LOGGER.debug(row)
                    except:
                        i = (ward_no, row[0], row[1], row[2], row[3], row[6], row[8], row[10], row[11], row[13])
                        badrows.append(i)
                        LOGGER.warning('Date format is not MM/DD/YYYY')
                else:
                    LOGGER.debug('This row is not included: %s', row)
            except (IndexError, KeyError) as err1:
                LOGGER.error('An error occurs at %s', row)
                LOGGER.error(err1)
        if len(badrows) > 0:
            LOGGER.warning('Please fix date formats for %s rows in %s at %s', len(badrows), table_name, badrows)
        else:
            LOGGER.info('%s does not have any row with different date format', table_name)
    
    fpath = os.path.join(SQL_DIR, 'insert-mobile_sign_installations.sql')
    with open(fpath, 'r', encoding='utf-8') as file:
        insert = sql.SQL(file.read())
    
    LOGGER.debug(rows)

    try:
        with con.cursor() as cur:
            execute_values(cur, insert, rows)
        LOGGER.info('Table %s is done', table_name)
    except Error as err2:
        LOGGER.error('There was an error inserting into %s', table_name)
        LOGGER.error(err2)
        LOGGER.error(rows)
        return False
    
    LOGGER.info('Uploaded %s rows to PostgreSQL for %s', len(rows), table_name)
    return True

if __name__ == '__main__':
    CONFIG = configparser.ConfigParser()
    CONFIG.read(r'/home/jchew/local/db.cfg')
    dbset = CONFIG['DBSETTINGS']
    con = connect(**dbset)
        
    SCOPES = ['https://www.googleapis.com/auth/spreadsheets.readonly']

    SERVICE_ACCOUNT_FILE = '/home/jchew/local/vz_key.json' 

    credentials = service_account.Credentials.from_service_account_file(
        SERVICE_ACCOUNT_FILE, scopes=SCOPES)   

    service = build('sheets', 'v4', credentials=credentials, cache_discovery=False)

    read_masterlist(con,service)
 
