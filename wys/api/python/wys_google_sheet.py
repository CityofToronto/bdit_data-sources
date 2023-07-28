from __future__ import print_function
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

import configparser
from psycopg2 import connect
from psycopg2.extras import execute_values
from psycopg2 import sql
from psycopg2 import Error

from datetime import datetime
import logging 
from time import sleep
import socket

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
    
    for attempt in range(3):
        try:
            sheet = service.spreadsheets()
            result = sheet.values().get(spreadsheetId=spreadsheet_id, range=range_name).execute()
            values = result.get('values', [])
        except socket.timeout:
            sleep(120)
        except HttpError as err3:
            sleep(120)
        else:
            break
    else: 
        LOGGER.warning('Attempts exceeded.')

    rows = []
    badrows = []
    if not values:
        LOGGER.warning('No data found.')
    else:
        for row in values:           
            try:             
                if row[6] and row[10]:
                    try: 
                        installation = datetime.strptime(row[6], '%m/%d/%Y').date()
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
            installation_date, removal_date, new_sign_number, comments, work_order
            FROM new_data
            NATURAL JOIN (SELECT new_sign_number, installation_date
                    FROM new_data
                    GROUP BY new_sign_number, installation_date
                    HAVING COUNT(*)> 1) dupes
            ON CONFLICT (work_order)
            DO NOTHING
            RETURNING new_sign_number, installation_date
        )
        INSERT INTO {}.{} AS existing (ward_no, location, from_street, to_street, 
                           direction, installation_date, removal_date, 
                           new_sign_number, comments, confirmed, work_order) 
        SELECT new_data.ward_no::INT, location, from_street, to_street, 
                direction, installation_date, removal_date, 
                new_sign_number, comments, confirmed, work_order
        FROM new_data
        LEFT JOIN dupes USING (new_sign_number, installation_date)
        --Don't try to insert dupes
        WHERE dupes.new_sign_number IS NULL
        ON CONFLICT (installation_date, new_sign_number)
        DO UPDATE SET 
            removal_date=EXCLUDED.removal_date,
            comments=EXCLUDED.comments,
            direction=EXCLUDED.direction,
            location=EXCLUDED.location,
            from_street=EXCLUDED.from_street,
            to_street=EXCLUDED.to_street,
            work_order=EXCLUDED.work_order
        -- Prevent unnecessary update if data are unchanged
        WHERE existing.removal_date!=EXCLUDED.removal_date 
            OR existing.comments!=EXCLUDED.comments
            OR existing.direction!=EXCLUDED.direction
            OR existing.location!=EXCLUDED.location
            OR existing.from_street!=EXCLUDED.from_street
            OR existing.to_street!=EXCLUDED.to_street
            OR existing.work_order!=EXCLUDED.work_order
        ''').format(sql.Identifier(schema_name), sql.Identifier(table_name)) 
    
    LOGGER.debug(rows)

    try:
        with con:
            with con.cursor() as cur:
                execute_values(cur, insert, rows)
        LOGGER.info('Table %s is done', table_name)
    except Error as err2:
        LOGGER.error('There was an error inserting into %s', table_name)
        LOGGER.error(err2)
    
    LOGGER.info('Uploaded %s rows to PostgreSQL for %s', len(rows), table_name)

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
 
