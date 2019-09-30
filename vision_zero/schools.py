from __future__ import print_function
from google.oauth2 import service_account
from googleapiclient.discovery import build

import configparser
from psycopg2 import connect
from psycopg2.extras import execute_values
from psycopg2 import sql
import logging 

# If modifying these scopes, delete the file token.pickle.
SCOPES = ['https://www.googleapis.com/auth/spreadsheets.readonly']

SERVICE_ACCOUNT_FILE = '/home/jchew/bdit_data-sources/vision_zero/key.json' 

credentials = service_account.Credentials.from_service_account_file(
        SERVICE_ACCOUNT_FILE, scopes=SCOPES)   

# The ID, range and table name of the spreadsheet.
sheets = {2018: {'spreadsheet_id' : '16ZmWa6ZoIrJ9JW_aMveQsBM5vuGWq7zH0Vw_rvmSC7A', 
                 'range_name' : 'Master List!A4:AC112',
                 'schema_name': 'vz_safety_programs_staging',
                 'table_name' : 'school_safety_zone_2018_raw'},
          2019: {'spreadsheet_id' : '19JupdNNJSnHpO0YM5sHJWoEvKumyfhqaw-Glh61i2WQ', 
                 'range_name' : '2019 Master List!A3:AC94', 
                 'schema_name': 'vz_safety_programs_staging',
                 'table_name' : 'school_safety_zone_2019_raw'}}

LOGGER = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

def pull_from_sheet(con, service, year, *args):
    """Shows basic usage of the Sheets API.
    Prints values from a sample spreadsheet.
    """
    # Call the Sheets API
    sheet = service.spreadsheets()
    # .batchGet instead of .get to read multiple ranges, range= to ranges= instead, values to valueRanges instead
    result = sheet.values().get(spreadsheetId=sheets[year]['spreadsheet_id'],
                                range=sheets[year]['range_name']).execute()
    values = result.get('values', [])

    rows = []
    if not values:
        LOGGER.warning('No data found.')
    else:
        for row in values:
            #Only append schools with School Coordinate
            try:
                if row[24] != '':
                    # Print columns A, B, E, F, Y, Z, AA, AB which correspond to indices 0, 1, 4, 5, 24, 25, 26, 27.
                    i = (row[0], row[1], row[4], row[5], row[24], row[25], row[26], row[27])
                    rows.append(i)
                    LOGGER.info('Reading %s columns of data from Google Sheet', len(row))
                    LOGGER.debug(row)
                else:
                    LOGGER.info('School coordinate is not given')
            except (IndexError, KeyError) as err:
                LOGGER.error('An error occurs at %s', row)
                LOGGER.error(err)
    
    schema = sheets[year]['schema_name']
    table = sheets[year]['table_name']

    truncate = sql.SQL('''TRUNCATE TABLE {}.{}''').format(sql.Identifier(schema),sql.Identifier(table))
    LOGGER.info('Truncating existing table %s', table)

    query = sql.SQL('''INSERT INTO {}.{} (school_name, address, work_order_fb, work_order_wyss, locations_zone, final_sign_installation,
                       locations_fb, locations_wyss) VALUES %s''').format(sql.Identifier(schema), sql.Identifier(table))                                                                                             
    LOGGER.info('Uploading %s rows to PostgreSQL', len(rows))
    LOGGER.debug(rows)

    with con:
        with con.cursor() as cur:
            #truncate the tables so that the data wont keep getting added to the table
            cur.execute(truncate)
            #reading google sheets and putting the data into the schemas
            execute_values(cur, query, rows)
    LOGGER.info('Table %s is done', table)

if __name__ == '__main__':
    CONFIG = configparser.ConfigParser()
    CONFIG.read(r'/home/jchew/local/google_api/db.cfg')
    dbset = CONFIG['DBSETTINGS']
    con = connect(**dbset)

    service = build('sheets', 'v4', credentials=credentials, cache_discovery=False)

    pull_from_sheet(con, service, 2018)
    pull_from_sheet(con, service, 2019)