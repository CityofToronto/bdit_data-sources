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
from shapely.geometry import Polygon, LineString, Point

from airflow.exceptions import AirflowFailException

"""The following accesses credentials from key.json (a file created from the google account used to read the sheets) 
and read the spreadsheets.

Note
----
If the scopes is modified from readonly, delete the file token.pickle."""
SCOPES = ['https://www.googleapis.com/auth/spreadsheets.readonly']




"""The following provides information about the code when it is running and prints out the log messages 
if they are of logging level equal to or greater than INFO"""
LOGGER = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

#--------------------------------------------------------------------------------------------------

def validate_school_info(con, row):
    """ This function tests the data format validity of one row of data pulled from the Google Sheet.
    Each field must pass the validation check for the entire school entry to be upserted into the database
    
    Parameters
    ----------
    con :
        Connection to bigdata database.
    row : list
        a single row from the spreadsheet.
    """
    
    ret_val = True
    
    # Flashing Beacon W/O
    #if not is_int(row[2]):
    #    ret_val = False
      
    # WYSS W/O
    #if not is_int(row[3]):
    #    ret_val = False
    
    # School Coordinate (X,Y)
    if not within_toronto(con, row[4]):
        LOGGER.error('Invalid school coordinates "{}"'.format(row[4]))
        ret_val = False
    
    # Final Sign Installation Date
    try:
        row[5] = enforce_date_format(row[5])
    except ValueError:
        LOGGER.error('"{}" cannot be transformed into a date'.format(row[5]))
        ret_val = False
    
    # FB Locations (X,Y)
    if not within_toronto(con, row[6]):
        LOGGER.error('Invalid FB coordinates "{}"'.format(row[6]))
        ret_val = False
    
    # WYS Locations (X,Y)
    if not within_toronto(con, row[7]):
        LOGGER.error('Invalid WYSS coordinates "{}"'.format(row[7]))
        ret_val = False
    
    return ret_val

def within_toronto(con, coords):
    """
    Returns whether all the coordinate pairs in the string are within the boundaries of Toronto.

    Parameters
    ----------
    con :
        Connection to bigdata database.
    coords: str
        string that is either empty or contains at least one pair of coords
    """
    if coords is '':
        return True
    elif bool(re.match(r'^Cancel', coords)):
        return True
    else:
        try:
            # Get a list of lat/long pairs as tuples of floats
            pairs_str = coords.split(';')
            pairs = [tuple(map(float, i.split(','))) for i in pairs_str]
            
            to_boundary_sql = "SELECT * FROM gis.toronto_boundary"
            to_boundary = gpd.GeoDataFrame.from_postgis(to_boundary_sql, con).to_crs(epsg=2952).buffer(50).to_crs(epsg=4326)
            
            for pair in pairs:
                point = Point(pair[1], pair[0]) # GeoPandas uses long-lat coordinate order
                if not to_boundary.contains(point)[0]:
                    return False
            return True
        except Exception as e:
            LOGGER.error(e)
            return False

def enforce_date_format(date, fuzzy=False):
    """
    If the string can be interpreted as a date, modify its format for PGSQL compatibility.
    Else, an exception will be caught in the parent function.

    :param date: str, string to check for date
    :param fuzzy: bool, ignore unknown tokens in string if True
    """
    if date is '':
        return date
    else:
        parsed_date = parse(date, fuzzy=fuzzy)
        formatted_date = parsed_date.strftime("%B %-d, %Y")
        return formatted_date

def is_int(n):
    """
    Returns whether a string can be converted into an integer.
    
    :param n: str, string to check for if it's an integer
    """
    if n is '':
        return True
    else:
        try:
            int(n)
        except ValueError:
            return False
        return True

#--------------------------------------------------------------------------------------------------

def pull_from_sheet(con, service, year, spreadsheet, **kwargs):
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
    any_error = False
    sheet = service.spreadsheets()
    result = sheet.values().get(spreadsheetId=spreadsheet['spreadsheet_id'],
                                range=spreadsheet['range_name']).execute()
    values = result.get('values', [])

    rows = []
    if not values:
        LOGGER.warning('No data found.')
    else:
        for ind, row in enumerate(values, 1):           
            try:                   
                if len(row) < 28:
                    # skip incomplete rows
                    LOGGER.warning('Skipping incomplete row #%i: %s', ind, row)
                    continue
                i = [row[0], row[1], row[4], row[5], row[24], row[25], row[26], row[27]]
                
                if validate_school_info(con, i): # return true or false
                    rows.append(i)
                    LOGGER.info('Reading row #%i', ind)
                    LOGGER.debug(row)
                else:
                    LOGGER.error('at row #%i: %s', ind, row)
                    any_error = True
            except (IndexError, KeyError) as err:
                LOGGER.error('An error occurred at row #%i: %s', ind, row)
                LOGGER.error(err)
                any_error = True
    
    schema = spreadsheet['schema_name']
    table = spreadsheet['table_name']
    
    
    truncate = sql.SQL('''TRUNCATE TABLE {}.{}''').format(sql.Identifier(schema),sql.Identifier(table))
    LOGGER.info('Truncating existing table %s', table)
    
    
    query = sql.SQL('''INSERT INTO {}.{} (school_name, address, work_order_fb, work_order_wyss, locations_zone, final_sign_installation,
                       locations_fb, locations_wyss) VALUES %s''').format(sql.Identifier(schema), sql.Identifier(table))
    
    LOGGER.info('Uploading %s rows to PostgreSQL', len(rows))
    LOGGER.debug(rows)
    
    if not rows: # Only insert if there is at least one valid row to be inserted
        LOGGER.warning('There is no valid data for year: %i', year)
        return
    with con:
        with con.cursor() as cur:
            cur.execute(truncate)  
            execute_values(cur, query, rows)
    LOGGER.info('Table %s is done', table)
    task_instance = kwargs['task_instance']
    if any_error:
        # Custom slack message upon finding some (not all) invalid records
        # `invalid_rows` is being pulled in the failure callback method (slack notification)
        task_instance.xcom_push('invalid_rows', True)
        raise AirflowFailException('Invalid rows found. See errors documented above.')
    else:
        task_instance.xcom_push('invalid_rows', False)
    