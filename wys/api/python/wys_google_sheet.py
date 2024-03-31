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

def read_masterlist(con, service, **kwargs):
    dict_table = {}
    ward_list = []
    with con:
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
    
    if empty_wards == []:
        LOGGER.info('Completed')
    else:
        task_instance = kwargs.get('task_instance', None)
        if task_instance:
            task_instance.xcom_push('empty_wards', empty_wards)
        raise AirflowFailException(
            "Failed to pull/load the data of the following wards: " + 
            ", ".join(map(str, empty_wards))
        )

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
    schema_name = str(ward[2])
    table_name = str(ward[3])
    ward_no = (str(ward[3])).split('_',1)[1]
    
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

    insert = sql.SQL('''
        WITH new_data (ward_no, location, from_street, to_street, direction, 
                    installation_date, removal_date, new_sign_number, comments, 
                    work_order, confirmed) 
                    AS (
                VALUES %s) 
        , dupes AS(
            /*Identify rows that will violate the unique constraint in the 
            upsert*/
            INSERT INTO wys.mobile_sign_installations_dupes
            SELECT ward_no::INT, location, from_street, to_street, direction, 
            installation_date, removal_date::date, new_sign_number, comments, work_order
            FROM new_data
            NATURAL JOIN (SELECT new_sign_number, installation_date
                    FROM new_data
                    GROUP BY new_sign_number, installation_date
                    HAVING COUNT(*)> 1) dupes
            ON CONFLICT (location, from_street, to_street, direction, 
                        installation_date, removal_date, new_sign_number, 
                        comments)
            /*do update required here because do nothing does not return dupes
            on conflict. */
            DO UPDATE SET 
                location=EXCLUDED.location,
                from_street=EXCLUDED.from_street,
                to_street=EXCLUDED.to_street,
                direction=EXCLUDED.direction,
                removal_date=EXCLUDED.removal_date,
                comments=EXCLUDED.comments
            RETURNING new_sign_number, installation_date
        )
        INSERT INTO wys.mobile_sign_installations AS existing (
            ward_no, location, from_street, to_street, direction, installation_date,
            removal_date, new_sign_number, comments, confirmed, work_order) 
        SELECT new_data.ward_no::INT, location, from_street, to_street, 
                direction, installation_date, removal_date::DATE, 
                new_sign_number, comments, confirmed, work_order
        FROM new_data
        LEFT JOIN dupes USING (new_sign_number, installation_date)
        --Don't try to insert dupes
        WHERE dupes.new_sign_number IS NULL
        ON CONFLICT (ward_no, installation_date, new_sign_number)
        DO UPDATE SET 
            removal_date=EXCLUDED.removal_date,
            comments=EXCLUDED.comments,
            direction=EXCLUDED.direction,
            location=EXCLUDED.location,
            from_street=EXCLUDED.from_street,
            to_street=EXCLUDED.to_street,
            work_order=EXCLUDED.work_order
        -- Prevent unnecessary update if data are unchanged
        WHERE  (existing.removal_date IS NULL OR 
            existing.removal_date!=EXCLUDED.removal_date )
            OR (existing.comments IS NULL OR 
                existing.comments!=EXCLUDED.comments)
            OR (existing.direction IS NULL OR 
                existing.direction!=EXCLUDED.direction)
            OR (existing.location IS NULL OR 
                existing.location!=EXCLUDED.location)
            OR (existing.from_street IS NULL OR 
                existing.from_street!=EXCLUDED.from_street)
            OR (existing.to_street IS NULL OR 
                existing.to_street!=EXCLUDED.to_street)
            OR (existing.work_order IS NULL OR 
                existing.work_order!=EXCLUDED.work_order)
        ''')
    
    LOGGER.debug(rows)

    try:
        with con:
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
 
