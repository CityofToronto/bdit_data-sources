""" This script reads 4 Vision Zero google spreadsheets ('2018 School Safety Zone', '2019 School Safety Zone',
2020 School Safety Zone, and 2021 School Safety Zone)
and puts them into 4 postgres tables ('school_safety_zone_2018_raw', 'school_safety_zone_2019_raw',
'school_safety_zone_2020_raw', 'school_safety_zone_2021_raw') using the Google Sheet API.

Note
----
This script is automated on Airflow and is run daily."""

from __future__ import print_function
from google.oauth2 import service_account
from googleapiclient.discovery import build

import configparser
from psycopg2 import connect
from psycopg2.extras import execute_values
from psycopg2 import sql
import logging 

import json
from airflow import DAG
from airflow.models import Variable
dag_config = Variable.get('ssz_spreadsheet_ids', deserialize_json=True)
# ssz2018 = dag_config['ssz2018']
# ssz2019 = dag_config['ssz2019']
# ssz2020 = dag_config['ssz2020']
# ssz2021 = dag_config['ssz2021']

"""The following accesses credentials from key.json (a file created from the google account used to read the sheets) 
and read the spreadsheets.

Note
----
If the scopes is modified from readonly, delete the file token.pickle."""
SCOPES = ['https://www.googleapis.com/auth/spreadsheets.readonly']

"""The following defines the details of the spreadsheets read and details of the table used to store the data. They are put into a dict based on year. 
The range for both sheets is set from the beginning up to line 180 to include rows of schools which might be added later on.
Details of the spreadsheets are ID and range whereas details of the table are name of schema and table.
The ID is the value between the "/d/" and the "/edit" in the URL of the spreadsheet.
"""
sheets = {
           2018: {'spreadsheet_id' : '16ZmWa6ZoIrJ9JW_aMveQsBM5vuGWq7zH0Vw_rvmSC7A', 
                  'range_name' : 'Master List!A4:AC180',
                  'schema_name': 'vz_safety_programs_staging',
                  'table_name' : 'school_safety_zone_2018_raw'},
           2019: {'spreadsheet_id' : '19JupdNNJSnHpO0YM5sHJWoEvKumyfhqaw-Glh61i2WQ', 
                  'range_name' : '2019 Master List!A3:AC180', 
                  'schema_name': 'vz_safety_programs_staging',
                  'table_name' : 'school_safety_zone_2019_raw'},
           2020: {'spreadsheet_id' : '1pJipqKLSuAoYvxiUXHHhdSwTalrag5cbTGxBl1kDSsg', 
                  'range_name' : 'Master Sheet!A3:AC180', 
                  'schema_name': 'vz_safety_programs_staging',
                  'table_name' : 'school_safety_zone_2020_raw'},
           2021: {'spreadsheet_id' : '11NfFwVFAZQNrXnqS065eTgGLEuRf8FBmhFUnhTq3X6A', 
                  'range_name' : 'Master Sheet!A3:AC180', 
                  'schema_name': 'vz_safety_programs_staging',
                  'table_name' : 'school_safety_zone_2021_raw'}
         }


"""The following provides information about the code when it is running and prints out the log messages 
if they are of logging level equal to or greater than INFO"""
LOGGER = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

def pull_from_sheet(con, service, year, *args):
    """This function is to call the Google Sheets API, pull values from the Sheet using service
    and push them into the postgres table using con.
    Only information from columns A, B, E, F, Y, Z, AA, AB (which correspond to indices 0, 1, 4,
    5, 24, 25, 26, 27) are transposed to the table.
    Rows with empty cells at the beginning or end of the row or just an entire row of empty cells
    are not included in the postgres table.
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
        If list index out of range which happens due to the presence of empty cells at the end of row or on the entire row.
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
                i = (row[0], row[1], row[4], row[5], row[24], row[25], row[26], row[27])
                rows.append(i)
                LOGGER.info('Reading %s columns of data from Google Sheet', len(row))
                LOGGER.debug(row)
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
    """The following connects to the database, establishes connection to the sheets 
    and executes function based on the year of data required.
    Note: this part is not run by airflow.

    """

    SERVICE_ACCOUNT_FILE = '/home/jchew/bdit_data-sources/vision_zero/key.json' 

    credentials = service_account.Credentials.from_service_account_file(
        SERVICE_ACCOUNT_FILE, scopes=SCOPES)   

    CONFIG = configparser.ConfigParser()
    CONFIG.read(r'/home/cnangini/googlesheets_db.cfg')
    dbset = CONFIG['DBSETTINGS']
    con = connect(**dbset)

    service = build('sheets', 'v4', credentials=credentials, cache_discovery=False)

    pull_from_sheet(con, service, 2018)
    pull_from_sheet(con, service, 2019)
    pull_from_sheet(con, service, 2020)
    pull_from_sheet(con, service, 2021)
