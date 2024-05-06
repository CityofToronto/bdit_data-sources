#!/data/airflow/airflow_venv/bin/python3
# -*- coding: utf-8 -*-
"""The School Safety Zones Data Loader

This script loads School Safety Zones (SSZ) data from multiple google sheets,
where each sheet contains data of a single year. It is being called by a daily
DAG and it can also be run from the CLI. Run the script with `--help` for more
details.
"""

from __future__ import print_function
import json

import logging 

import re
from typing import Optional
from dateutil.parser import parse
import geopandas as gpd
from shapely.geometry import Point

import click
import configparser
from sqlalchemy import create_engine, Table, Column, MetaData, delete
from sqlalchemy.engine import URL
from airflow.providers.google.common.hooks.base_google import GoogleBaseHook
from googleapiclient.discovery import build
    

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
    if coords == '':
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
    date = date.strip()
    if date == '':
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
    if n == '':
        return True
    else:
        try:
            int(n)
        except ValueError:
            return False
        return True


def pull_from_sheet(
    engine,
    service,
    year,
    spreadsheet_id,
    spreadsheet_range,
    schema,
    table
):
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

    Args:
        engine: Connection to bigdata database.
        service: Credentials to access google sheets.
        year: Indicates which year of data is being worked on.
        spreadsheet_id: The iD iof the Google spreadsheet containing raw data.
        spreadsheet_range: The range of data within the spreadsheet.
        schema: PostgreSQL schema to load the data into.
        table: PostgreSQL table to load the data into.
    
    Returns:
        A boolean status of ``True``, if completed without any problems;
            Otherwise, ``False``.

    Raises:
        IndexError: If list index out of range which happens due to the presence of empty cells at the end of row or on the entire row.
        TimeoutError: If the connection to Google sheets timed out.
    """
    status = True
    try:
        sheet = service.spreadsheets()
        request = sheet.values().get(spreadsheetId=spreadsheet_id,
                                    range=spreadsheet_range)
        result = request.execute()
        values = result.get('values', [])
    except TimeoutError as err:
        LOGGER.error("Cannot access: " + json.loads(request.to_json())["uri"])
        raise err

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
                
                if validate_school_info(engine, i): # return true or false
                    rows.append(i)
                    LOGGER.info('Reading row #%i', ind)
                    LOGGER.debug(row)
                else:
                    LOGGER.error('at row #%i: %s', ind, row)
                    status = False
            except (IndexError, KeyError) as err:
                LOGGER.error('An error occurred at row #%i: %s', ind, row)
                LOGGER.error(err)
                status = False
    
    
    if not rows: # Only insert if there is at least one valid row to be inserted
        LOGGER.warning('There is no valid data for year: %i', year)
        return False
    
    metadata = MetaData(schema = schema)
    t = Table(
        table,
        metadata,
        *[Column(c) for c in [
            "school_name",
            "address",
            "work_order_fb",
            "work_order_wyss",
            "locations_zone",
            "final_sign_installation",
            "locations_fb",
            "locations_wyss"
            ]
        ]
    )
    truncate_stmt = delete(t)
    insert_stmt = t.insert().values(rows)
    with engine.connect() as conn:
        LOGGER.info('Truncating existing table %s', table)
        conn.execute(truncate_stmt)
        LOGGER.info('Uploading %s rows to PostgreSQL', len(rows))
        LOGGER.debug(rows)
        conn.execute(insert_stmt)

    LOGGER.info('Table %s is done', table)
    return status

@click.command()
@click.argument("db-config")
@click.option(
    "--year",
    "-y",
    type=int,
    required=True,
    help="The year to pull its data",
)
@click.option(
    "--spreadsheet-id",
    "-d",
    required=True,
    help=(
        "The Id of the spreadsheet to pull the data from. "
        "Check the Airflow variable `ssz_spreadsheets` for more info."
    ),
)
@click.option(
    "--spreadsheet-range",
    "-r",
    required=True,
    help=(
        "The range of data within the spreadsheet. "
        "Check the Airflow variable `ssz_spreadsheets` for more info."
    ),
)
@click.option(
    "--schema",
    "-s",
    default="vz_safety_programs_staging",
    help="The schema to load the data into",
)
@click.option(
    "--table",
    "-t",
    default=None,
    help="The table to load the data into",
)
def pull_from_sheet_cli(
    db_config: str,
    year: int,
    spreadsheet_id: str,
    spreadsheet_range: str,
    schema: Optional[str] = "vz_safety_programs_staging",
    table: Optional[str] = None,
) -> None:
    """Loads SSZ data from Google Spreadsheets into the database.

    Args:
        db_config: A configurations file containing the database details.

    Examples:
        gis/school_safety_zones/schools.py ~/.db.cfg -y 2023 -d '1_eQXrilU' -r 'Master Sheet!A3:AC180'
    """
    logging.basicConfig(level=logging.ERROR)

    config = configparser.ConfigParser()
    config.read(db_config)
    connect_url = URL.create("postgresql+psycopg2", **config['DBSETTINGS'])
    engine = create_engine(connect_url)

    google_cred = GoogleBaseHook('vz_api_google').get_credentials()

    if table is None:
        table = f"school_safety_zone_{year}_raw"

    if pull_from_sheet(
        engine = engine,
        service=build('sheets', 'v4', credentials=google_cred, cache_discovery=False),
        year=year,
        spreadsheet_id=spreadsheet_id,
        spreadsheet_range=spreadsheet_range,
        schema=schema,
        table=table,
    ):
        print(f"Successfully pulled {year} data into {schema}.{table}.")
    else:
        print(f"Failed to pull {year} data into {schema}.{table}.")

if __name__ == "__main__":
    # pylint: disable-next=no-value-for-parameter
    pull_from_sheet_cli()