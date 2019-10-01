""" This script is made to read two Vision Zero google spreadsheets ('2018 School Safety Zone' and '2019 School Safety Zone')
and put them into two postgres tables ('school_safety_zone_2018_raw' and 'school_safety_zone_2019_raw')
using Google Sheet API.

Note
----
This script is auotamated on Airflow and is run daily."""

from __future__ import print_function
from google.oauth2 import service_account
from googleapiclient.discovery import build

import configparser
from psycopg2 import connect
from psycopg2.extras import execute_values
from psycopg2 import sql
import logging 

"""The following access credentials from key.json (a file created from the google account used to read the sheets) 
and read the spreadsheets.

Note
----
If the scopes is modified from readonly, delete the file token.pickle."""
SCOPES = ['https://www.googleapis.com/auth/spreadsheets.readonly']

SERVICE_ACCOUNT_FILE = '/home/jchew/bdit_data-sources/vision_zero/key.json' 

credentials = service_account.Credentials.from_service_account_file(
        SERVICE_ACCOUNT_FILE, scopes=SCOPES)   

"""The following defines the details of the spreadsheets read and details of the table used to store the data.
They are put into a dict based on the year.
The details of the spreadsheets are ID and range whereas the details of the table are name of schema and table."""
sheets = {2018: {'spreadsheet_id' : '16ZmWa6ZoIrJ9JW_aMveQsBM5vuGWq7zH0Vw_rvmSC7A', 
                 'range_name' : 'Master List!A4:AC112',
                 'schema_name': 'vz_safety_programs_staging',
                 'table_name' : 'school_safety_zone_2018_raw'},
          2019: {'spreadsheet_id' : '19JupdNNJSnHpO0YM5sHJWoEvKumyfhqaw-Glh61i2WQ', 
                 'range_name' : '2019 Master List!A3:AC94', 
                 'schema_name': 'vz_safety_programs_staging',
                 'table_name' : 'school_safety_zone_2019_raw'}}

"""The following provides information about the code when it is running and will print out the log messages 
if they are of logging level equal to or greater than INFO"""
LOGGER = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

def pull_from_sheet(con, service, year, *args):
    """This function is to call the Sheets API, pull values from the Sheet using service and push them into the postgres table using con.
    Only include schools with School Coordinate (column Y in Sheet) in the table. Only information from columns A, B, E, F, Y, Z, AA, AB 
    (which correspond to indices 0, 1, 4, 5, 24, 25, 26, 27) are transposed to the table.
    The existing table on postgres will be truncated first prior to inserting data into it.

    Note
    ----
    Can only read a single range of a spreadsheet with this function.

    Parameters
    ----------
    con :
        Connection to bigdata database.
    service :
        Credentials to access google sheets.
    year : int
        Indicates which year of data is being worked on.
    *args 
        Variable length argument list.

    Raises
    ------
    IndexError
        If list out of index range which happens due to empty cells at the end of a row.
    KeyError
        If there are totally/mostly empty row of cells in the sheet.
    """
    sheet = service.spreadsheets()
    result = sheet.values().get(spreadsheetId=sheets[year]['spreadsheet_id'],
                                range=sheets[year]['range_name']).execute()
    values = result.get('values', [])

    rows = []
    if not values:
        LOGGER.warning('No data found.')
    else:
        for row in values:           
            try:
                if row[24] != '':                    
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
            cur.execute(truncate)
            execute_values(cur, query, rows)
    LOGGER.info('Table %s is done', table)

if __name__ == '__main__':
    """The following connects to the database, establish connection to the sheets and execute function based on the year of data required."""
    CONFIG = configparser.ConfigParser()
    CONFIG.read(r'/home/jchew/local/google_api/db.cfg')
    dbset = CONFIG['DBSETTINGS']
    con = connect(**dbset)

    service = build('sheets', 'v4', credentials=credentials, cache_discovery=False)

    pull_from_sheet(con, service, 2018)
    pull_from_sheet(con, service, 2019)