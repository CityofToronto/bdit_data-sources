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

sheets = {1: {'spreadsheet_id' : '1_dQVQfQ-vs7LBp11ErlAuhDK7ALhZB1eTXJboTbbQCk', 
                 'range_name' : 'Sign Location Requests!A10:L300',
                 'schema_name': 'wys',
                 'table_name' : 'ward_1'},
          2: {'spreadsheet_id' : '1UIAU4QaI7oZmVY4za5roaUqdiRo_Niuql7vA5S-XnUA', 
                 'range_name' : 'Sign Location Requests!A10:L300', 
                 'schema_name': 'wys',
                 'table_name' : 'ward_2'},
          3: {'spreadsheet_id' : '1VRZO5CWYlE7gqudcDDMAgpHhAP1oDf5HmKXCn1E9_NM', 
                 'range_name' : 'Sign Location Requests!A10:L300', 
                 'schema_name': 'wys',
                 'table_name' : 'ward_3'},
          4:{'spreadsheet_id' : '1Cj5otzB63p7IJH4_l-jpymtXqaOxFPQGFTtVECnOZpA', 
                 'range_name' : 'Sign Location Requests!A10:L300', 
                 'schema_name': 'wys',
                 'table_name' : 'ward_4'},
          5: {'spreadsheet_id' : '1XgYjQ1Rq20TAq-t1JFDb7by9-sq8Y6ZFISqnqAQ2Auk', 
                 'range_name' : 'Sign Location Requests!A10:L300',
                 'schema_name': 'wys',
                 'table_name' : 'ward_5'},
          6: {'spreadsheet_id' : '1rSo2LeXk-FPoESpJQvlAnTAHpCt_cYk5ACYzT6jgQRU', 
                 'range_name' : 'Sign Location Requests!A10:L300', 
                 'schema_name': 'wys',
                 'table_name' : 'ward_6'},
          7: {'spreadsheet_id' : '1QMx0BwW6LQvWhPfWGUvhOjlgg-6AL0CU06lR27ZCHwY', 
                 'range_name' : 'Sign Location Requests!A10:L300', 
                 'schema_name': 'wys',
                 'table_name' : 'ward_7'},
          8:{'spreadsheet_id' : '1k69pGuM8bXNA1L4RrEDA86HO8lhvr0GHvkBZm0iJ8_E', 
                 'range_name' : 'Sign Location Requests!A10:L300', 
                 'schema_name': 'wys',
                 'table_name' : 'ward_8'},
          9: {'spreadsheet_id' : '1AJdtjT2lL9FwC6UH1i-5FqjW9PHFl5gY0GVA8oBTeLM', 
                 'range_name' : 'Sign Location Requests!A10:L300',
                 'schema_name': 'wys',
                 'table_name' : 'ward_9'},
          10: {'spreadsheet_id' : '188IImXWyfnuoKm4TkDwOUSWNaraJmdhC7R0xiiASNE0', 
                 'range_name' : 'Sign Location Requests!A10:L300', 
                 'schema_name': 'wys',
                 'table_name' : 'ward_10'},
          11: {'spreadsheet_id' : '1i8GlfAc-WeU8LCALoosjeLjG-U12hYh68nbCvx3HKy0', 
                 'range_name' : 'Sign Location Requests!A10:L300', 
                 'schema_name': 'wys',
                 'table_name' : 'ward_11'},
          12:{'spreadsheet_id' : '12ZFBbUKB3Ny8N0mk5yGAIbjqvpTvw6Ot5sWJ-9QxUBM', 
                 'range_name' : 'Sign Location Requests!A10:L300', 
                 'schema_name': 'wys',
                 'table_name' : 'ward_12'},
          13: {'spreadsheet_id' : '1NvqM3mhPZWAr_P1kgv0UK2jGBCFXaJLyWMDqV2_K8jc', 
                 'range_name' : 'Sign Location Requests!A10:L300',
                 'schema_name': 'wys',
                 'table_name' : 'ward_13'},
          14: {'spreadsheet_id' : '1o-Z_FXLEwVsQv3pCztwLCuro8f652-9NxM2dO8O4Dwo', 
                 'range_name' : 'Sign Location Requests!A10:L300', 
                 'schema_name': 'wys',
                 'table_name' : 'ward_14'},
          15: {'spreadsheet_id' : '1EPzG6OFU5j5dSh9XBG-IEDinRB1Rh5NLYrxnkC_6ZCo', 
                 'range_name' : 'Sign Location Requests!A10:L300', 
                 'schema_name': 'wys',
                 'table_name' : 'ward_15'},
          16:{'spreadsheet_id' : '1_oqUJIVckROGOdSkkn_AgjYyBzAyKZ-1svD8bu_ZWSE', 
                 'range_name' : 'Sign Location Requests!A10:L300', 
                 'schema_name': 'wys',
                 'table_name' : 'ward_16'},
          17: {'spreadsheet_id' : '1YlOKwZsHGMsBtv5OX3umeP1bskjOsk15V28Iu_ihs-Y', 
                 'range_name' : 'Sign Location Requests!A10:L300',
                 'schema_name': 'wys',
                 'table_name' : 'ward_17'},
          18: {'spreadsheet_id' : '1qT-zzCT06g8H-WmBoiVVFcppfbJRqrZou2YRLQrWEfc', 
                 'range_name' : 'Sign Location Requests!A10:L300', 
                 'schema_name': 'wys',
                 'table_name' : 'ward_18'},
          19: {'spreadsheet_id' : '18HbYGrqSbsOslk2Fme7bufSxiwCEdiip8XzP1LLlrUo', 
                 'range_name' : 'Sign Location Requests!A10:L300', 
                 'schema_name': 'wys',
                 'table_name' : 'ward_19'},
          20:{'spreadsheet_id' : '1-J77aCd6XJafqB5Xn3o0QizjUjUrHR6B2aj7z2aEqIg', 
                 'range_name' : 'Sign Location Requests!A10:L300', 
                 'schema_name': 'wys',
                 'table_name' : 'ward_20'},
          21: {'spreadsheet_id' : '1DgZFBm4fJUlEwbNWurt0QfzVUQZdpI55R-CTTAw36R8', 
                 'range_name' : 'Sign Location Requests!A10:L300',
                 'schema_name': 'wys',
                 'table_name' : 'ward_21'},
          22: {'spreadsheet_id' : '14ziPqIdsGMJcuU448H5luoHvYP4AZVabLjEBPTsxNOI', 
                 'range_name' : 'Sign Location Requests!A10:L300', 
                 'schema_name': 'wys',
                 'table_name' : 'ward_22'},
          23: {'spreadsheet_id' : '11U1CPp_3JHcKvbrOu2-etDCFvB3RR3iYN-ALpG8FOx0', 
                 'range_name' : 'Sign Location Requests!A10:L300', 
                 'schema_name': 'wys',
                 'table_name' : 'ward_23'},
          24:{'spreadsheet_id' : '12l6FB8x7yck9Uxo_I5v2uTkB_m9fO02qA0vfAYuZlJw', 
                 'range_name' : 'Sign Location Requests!A10:L300', 
                 'schema_name': 'wys',
                 'table_name' : 'ward_24'},
          25:{'spreadsheet_id' : '1LDZXeKvgfEkeYhEPeB10Ro6yzvjQ-v1ZGsAgA4M6WN0', 
                 'range_name' : 'Sign Location Requests!A10:L300', 
                 'schema_name': 'wys',
                 'table_name' : 'ward_25'}}


LOGGER = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

def pull_from_sheet(con, service, ward, *args):
    sheet = service.spreadsheets()
    result = sheet.values().get(spreadsheetId=sheets[ward]['spreadsheet_id'],
                                range=sheets[ward]['range_name']).execute()
    values = result.get('values', [])

    rows = []
    if not values:
        LOGGER.warning('No data found.')
    else:
        for row in values:           
            try:                   
                i = (row[0], row[1], row[2], row[3], row[6], row[8], row[10], row[11])
                rows.append(i)
                LOGGER.info('Reading %s columns of data from Google Sheet', len(row))
                LOGGER.debug(row)
            except (IndexError, KeyError) as err:
                LOGGER.error('An error occurs at %s', row)
                LOGGER.error(err)
    
    schema = sheets[ward]['schema_name']
    table = sheets[ward]['table_name']

    truncate = sql.SQL('''TRUNCATE TABLE {}.{}''').format(sql.Identifier(schema),sql.Identifier(table))
    LOGGER.info('Truncating existing table %s', table)

    query = sql.SQL('''INSERT INTO {}.{} (location, from_street, to_street, direction, installation_date, removal_date,
                       new_sign_number, comments) VALUES %s''').format(sql.Identifier(schema), sql.Identifier(table))                                                                                             
    LOGGER.info('Uploading %s rows to PostgreSQL', len(rows))
    LOGGER.debug(rows)

    with con:
        with con.cursor() as cur:
            cur.execute(truncate)
            execute_values(cur, query, rows)
    LOGGER.info('Table %s is done', table)

if __name__ == '__main__':
    CONFIG = configparser.ConfigParser()
    CONFIG.read(r'/home/jchew/local/db.cfg')
    dbset = CONFIG['DBSETTINGS']
    con = connect(**dbset)

    service = build('sheets', 'v4', credentials=credentials, cache_discovery=False)

    for i in range(25):
        pull_from_sheet(con, service, i+1)