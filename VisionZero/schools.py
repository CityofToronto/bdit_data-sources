from __future__ import print_function
import pickle
import os.path
from googleapiclient.discovery import build
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request

import configparser
from psycopg2 import connect
from psycopg2.extras import execute_values
import logging 

# If modifying these scopes, delete the file token.pickle. (readonly or remove that?)
SCOPES = ['https://www.googleapis.com/auth/spreadsheets.readonly']

# The ID and range of the spreadsheet.
SAMPLE_SPREADSHEET_ID_FIRST = '16ZmWa6ZoIrJ9JW_aMveQsBM5vuGWq7zH0Vw_rvmSC7A'
SAMPLE_RANGE_NAME_FIRST = 'Master List!A4:AB91'
SAMPLE_SPREADSHEET_ID_SECOND = '19JupdNNJSnHpO0YM5sHJWoEvKumyfhqaw-Glh61i2WQ'
SAMPLE_RANGE_NAME_SECOND = '2019 Master List!A3:AC95'

LOGGER = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

def first(con, *args):
    """Shows basic usage of the Sheets API.
    Prints values from a sample spreadsheet.
    """
    creds = None
    # The file token.pickle stores the user's access and refresh tokens, and is
    # created automatically when the authorization flow completes for the first
    # time.
    if os.path.exists('token.pickle'):
        with open('token.pickle', 'rb') as token:
            creds = pickle.load(token)
    # If there are no (valid) credentials available, let the user log in.
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(
                'credentials.json', SCOPES)
            creds = flow.run_console()
        # Save the credentials for the next run
        with open('token.pickle', 'wb') as token:
            pickle.dump(creds, token)

    service = build('sheets', 'v4', credentials=creds)

    # Call the Sheets API
    sheet = service.spreadsheets()
    result = sheet.values().get(spreadsheetId=SAMPLE_SPREADSHEET_ID_FIRST,
                                range=SAMPLE_RANGE_NAME_FIRST).execute()
    values = result.get('values', [])

    rows = []
    if not values:
        LOGGER.warning('No data found.')
    else:
        for row in values:
            # Print columns A, B, E, F, X, Y, Z, AA which correspond to indices 0, 1, 4, 5, 23, 24, 25, 26.
            i = (row[0], row[1], row[4], row[5], row[23], row[24], row[25], row[26])
            rows.append(i)
            LOGGER.info('Reading %s columns of data from Google Sheet', len(row))
            LOGGER.debug(row)

    sql='INSERT INTO vz_safety_programs_staging.school_safety_zone_2018_raw (school_name, address, work_order_fb, work_order_wyss,\
    locations_zone, final_sign_installation, locations_fb, locations_wyss) VALUES %s'

    LOGGER.info('Uploading %s rows to PostgreSQL', len(rows))
    LOGGER.debug(rows)

    with con:
        with con.cursor() as cur:
            execute_values(cur, sql, rows)  
    
    LOGGER.info('2018 School Safety Zone is done')

def second(con, *args):
    """Shows basic usage of the Sheets API.
    Prints values from a sample spreadsheet.
    """
    creds = None
    # The file token.pickle stores the user's access and refresh tokens, and is
    # created automatically when the authorization flow completes for the first
    # time.
    if os.path.exists('token.pickle'):
        with open('token.pickle', 'rb') as token:
            creds = pickle.load(token)
    # If there are no (valid) credentials available, let the user log in.
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(
                'credentials.json', SCOPES)
            creds = flow.run_console()
        # Save the credentials for the next run
        with open('token.pickle', 'wb') as token:
            pickle.dump(creds, token)

    service = build('sheets', 'v4', credentials=creds)

    # Call the Sheets API
    sheet = service.spreadsheets()
    result = sheet.values().get(spreadsheetId=SAMPLE_SPREADSHEET_ID_SECOND,
                                range=SAMPLE_RANGE_NAME_SECOND).execute()
    values = result.get('values', [])

    rows = []

    if not values:
        LOGGER.warning('No data found.')
    else:
        for row in values:
            # Print columns A, B, E, F, Y, Z, AA, AB which correspond to indices 0, 1, 4, 5, 24, 25, 26, 27.
            i = (row[0], row[1], row[4], row[5], row[24], row[25], row[26], row[27])
            rows.append(i)
            LOGGER.info('Reading %s columns of data from Google Sheet', len(row))
            LOGGER.debug(row)

    sql='INSERT INTO vz_safety_programs_staging.school_safety_zone_2019_raw (school_name, address, work_order_fb, work_order_wyss,\
    locations_zone, final_sign_installation, locations_fb, locations_wyss) VALUES %s'

    LOGGER.info('Uploading %s rows to PostgreSQL', len(rows))
    LOGGER.debug(rows)
    with con:
        with con.cursor() as cur:
            execute_values(cur, sql, rows)  

    LOGGER.info('2019 School Safety Zone is done')

if __name__ == '__main__':
    CONFIG = configparser.ConfigParser()
    CONFIG.read(r'/home/jchew/google_api/db.cfg')
    dbset = CONFIG['DBSETTINGS']
    con = connect(**dbset)
    first(con)
    second(con)
