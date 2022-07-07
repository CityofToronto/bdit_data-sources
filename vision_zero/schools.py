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

import re
from dateutil.parser import parse
from datetime import datetime
import geopandas as gpd

from airflow.models import Variable
dag_config = Variable.get('ssz_spreadsheet_ids', deserialize_json=True)
ssz2018 = dag_config['ssz2018']
ssz2019 = dag_config['ssz2019']
ssz2020 = dag_config['ssz2020']
ssz2021 = dag_config['ssz2021']
ssz2022 = dag_config['ssz2022']

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
           2018: {'spreadsheet_id' : ssz2018, 
                  'range_name' : 'Master List!A4:AC180',
                  'schema_name': 'vz_safety_programs_staging',
                  'table_name' : 'school_safety_zone_2018_raw'},
           2019: {'spreadsheet_id' : ssz2019, 
                  'range_name' : '2019 Master List!A3:AC180', 
                  'schema_name': 'vz_safety_programs_staging',
                  'table_name' : 'school_safety_zone_2019_raw'},
           2020: {'spreadsheet_id' : ssz2020, 
                  'range_name' : 'Master Sheet!A3:AC180', 
                  'schema_name': 'vz_safety_programs_staging',
                  'table_name' : 'school_safety_zone_2020_raw'},
           2021: {'spreadsheet_id' : ssz2021, 
                  'range_name' : 'Master Sheet!A3:AC180', 
                  'schema_name': 'vz_safety_programs_staging',
                  'table_name' : 'school_safety_zone_2021_raw'},
           2022: {'spreadsheet_id' : ssz2022,
                  'range_name' : 'Master Sheet!A3:AC180',
                  'schema_name': 'vz_safety_programs_staging',
                  'table_name' : 'school_safety_zone_2022_raw'}
         }


"""The following provides information about the code when it is running and prints out the log messages 
if they are of logging level equal to or greater than INFO"""
LOGGER = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

#--------------------------------------------------------------------------------------------------

def validate_school_info(row):
    """ This function tests the data format validity of one row of data pulled from the Google Sheet.
    Each field must pass the validation check for the entire school entry to be upserted into the database
    """
    
    # Flashing Beacon W/O
    if not is_int(row[2]):
        return False
      
    # WYSS W/O
    if not is_int(row[3]):
        return False
    
    # School Coordinate (X,Y)
    if not within_toronto(row[4]):
        return False
    
    # Final Sign Installation Date
    try:
        row[5] = enforce_date_format(row[5])
    except ValueError:
        return False
    
    # FB Locations (X,Y)
    if not within_toronto(row[6]):
        return False
    
    # WYS Locations (X,Y)
    if not within_toronto(row[7]):
        return False
    
    return True

def within_toronto(coords):
    """
    Returns whether all the coordinate pairs in the string are within the boundaries of Toronto.

    :param coords: str, string that is either empty or contains at least one pair of coords
    """
    if coords is None:
        return True
    elif bool(re.match(r'^Cancel', coords)):
        return True
    else:
        try:
            # Get a list of lat/long pairs as tuples of floats
            pairs_str = coords.split(';')
            pairs = [tuple(map(float, i.split(','))) for i in pairs_str]
            
            filepath = "~/bdit_data-sources/vision_zero/toronto_boundaries.shp"
            to_boundary = gpd.read_file(filepath)
            
            for pair in pairs:
                point = Point(pair[1], pair[0]) # GeoPandas uses long-lat coordinate order
                if not point.within(to_boundary):
                    return False
            return True
        except ValueError:
            return False

def enforce_date_format(date, fuzzy=False):
    """
    If the string can be interpreted as a date, modify its format for PGSQL compatibility.
    Else, an exception will be caught in the parent function.

    :param date: str, string to check for date
    :param fuzzy: bool, ignore unknown tokens in string if True
    """
    if date is None:
        return None
    else:
        parsed_date = parse(date, fuzzy=fuzzy)
        formatted_date = parsed_date.strftime("%B %-d, %Y")
        return formatted_date

def is_int(n):
    """
    Returns whether a string can be converted into an integer.
    
    """
    if n is None:
        return True
    else:
        try:
            int(n)
        except ValueError:
            return False
        return True

#--------------------------------------------------------------------------------------------------

def pull_from_sheet(con, service, year, *args):
    """This function is to call the Google Sheets API, pull values from the Sheet using service
    and push them into the postgres table using con.
    Only information from columns A, B, E, F, Y, Z, AA, AB (which correspond to indices 0, 1, 4,
    5, 24, 25, 26, 27) are transposed to the table.
    Rows with empty cells at the beginning or end of the row or just an entire row of empty cells
    are not included in the postgres table.
    The existing table on postgres will be truncated first prior to inserting data into it.
    
    UPDATE: The new function will be doing upsert on the existing table, based on school_name.
            There will be no truncating.

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
                i = [row[0], row[1], row[4], row[5], row[24], row[25], row[26], row[27]]
                
                if validate_school_info(i): # return true or false
                    rows.append(i)
                    LOGGER.info('Reading %s columns of data from Google Sheet', len(row))
                    LOGGER.debug(row)
                else:
                    LOGGER.error('An error occurs at %s', ','.join(i)) #double check
            except (IndexError, KeyError) as err:
                LOGGER.error('An error occurs at %s', row)
                LOGGER.error(err)
    
  '''  
    [s, g, s,a, s, d, s] [g, h, w, d, ]...
    function(row)
    row[1] check content
  '''  
    schema = sheets[year]['schema_name']
    table = sheets[year]['table_name']
    
    """
    truncate = sql.SQL('''TRUNCATE TABLE {}.{}''').format(sql.Identifier(schema),sql.Identifier(table))
    LOGGER.info('Truncating existing table %s', table)
    """
    
    #change query to upsert, check Sarah's lightning talk
    # take school_name as the primary key, but can double check with Raph
    query = sql.SQL('''INSERT INTO {}.{} (school_name, address, work_order_fb, work_order_wyss, locations_zone, final_sign_installation,
                       locations_fb, locations_wyss) VALUES %s''').format(sql.Identifier(schema), sql.Identifier(table))
    
    LOGGER.info('Uploading %s rows to PostgreSQL', len(rows))
    LOGGER.debug(rows)

    with con:
        with con.cursor() as cur:
            #cur.execute(truncate)
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
    pull_from_sheet(con, service, 2022)
