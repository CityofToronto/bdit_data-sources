import requests
import logging
from configparser import ConfigParser
from psycopg2 import connect
from psycopg2.extras import execute_values
from datetime import datetime, timedelta
from airflow.exceptions import AirflowFailException, AirflowException

default_start = datetime.now().replace(hour = 0, minute = 0, second = 0, microsecond = 0)-timedelta(days=1)
default_end = datetime.now().replace(hour = 0, minute = 0, second = 0, microsecond = 0)

LOGGER = logging.getLogger(__name__)

URL = 'https://apieco.eco-counter-tools.com'

# get an authentication token for accessing the API
def getToken(url: str, login: str, pw: str, secret: str):
    response = requests.post(
        f'{url}/token',
        headers={
            'Authorization': 'Basic ' + secret
        },
        data={
            'grant_type': 'password',
            'username': login,
            'password': pw
        }
    )
    return response.json()['access_token']

# get a list of all sites from the API
def getSites(token: str, sites: any = ()):
    response = requests.get(
        f'{URL}/api/site',
        headers={'Authorization': f'Bearer {token}'}
    )
    if response.status_code!=200:
        raise AirflowFailException(f"{response.status_code}: {response.reason}")
    if sites == ():
        return response.json()
    
    #otherwise filter sites using optional param.        
    result = []
    for site in response.json():
        if site['id'] in sites:
            result.append(site)
    return result        

# get all of a flows ("channel") data from the API
def getFlowData(token: str, flow_id: int, startDate: datetime, endDate: datetime):
    requestChunkSize = timedelta(days=100)
    requestStart = startDate
    data = []
    while requestStart < endDate:
        requestEnd = min(requestStart + requestChunkSize, endDate)
        response = requests.get(
            f'{URL}/api/data/site/{flow_id}',
            headers={'Authorization': f'Bearer {token}'},
            params={
                'begin': requestStart.isoformat(timespec='seconds'),
                'end':  requestEnd.isoformat(timespec='seconds'),
                'complete': 'false',
                'step': '15m'
            }
        )
        if response.status_code==200:
            data += response.json()
            requestStart += requestChunkSize
        elif response.status_code==401:
            raise AirflowFailException(f"{response.status_code}: {response.reason}")
        else:
            raise AirflowFailException(f"{response.status_code}: {response.reason}")
    return data

def getKnownSites(conn: any):
    with conn.cursor() as cur:
        cur.execute('SELECT site_id FROM ecocounter.sites_unfiltered;')
        sites = cur.fetchall()
        return [site[0] for site in sites]

def getKnownFlows(conn: any, site: int):
    with conn.cursor() as cur:
        cur.execute('SELECT flow_id FROM ecocounter.flows_unfiltered WHERE site_id = %s;',
                    (site, )
        )
        flows = cur.fetchall()
        return [flow[0] for flow in flows]

# do we have a record of this site in the database?
def siteIsKnownToUs(site_id: int, conn: any):
    with conn.cursor() as cursor:
        cursor.execute(
            "SELECT 1 FROM ecocounter.sites_unfiltered WHERE site_id = %s;",
            (site_id,)
        )
        return cursor.rowcount > 0

# do we have a record of this flow in the database?
def flowIsKnownToUs(flow_id: int, conn: any):
    with conn.cursor() as cursor:
        cursor.execute(
            "SELECT 1 FROM ecocounter.flows_unfiltered WHERE flow_id = %s;",
            (flow_id,)
        )
        return cursor.rowcount > 0

# DANGER delete all count data for a given flow
def truncateFlowSince(flow_id: int, conn: any, startDate: datetime, endDate: datetime):
    with conn.cursor() as cursor:
        cursor.execute(
            """DELETE FROM ecocounter.counts_unfiltered
            WHERE flow_id = %s
            AND datetime_bin >= %s
            AND datetime_bin < %s""",
            (flow_id, startDate, endDate)
        )

# insert records
def insertFlowCounts(conn: any, volume: any):
    insert_query="INSERT INTO ecocounter.counts_unfiltered (flow_id, datetime_bin, volume) VALUES %s"
    with conn.cursor() as cur:
        execute_values(cur, insert_query, volume)
    return cur.query

# insert new site record
def insertSite(conn: any, site_id: int, site_name: str, lon: float, lat: float):
    insert_query="""
    INSERT INTO ecocounter.sites_unfiltered (site_id, site_description, geom, validated)
    VALUES (
        %s::numeric,
        %s::text,
        ST_SetSRID(ST_MakePoint(%s, %s), 4326),
        null::boolean --not validated
    )
    """
    with conn.cursor() as cur:
        cur.execute(insert_query, (site_id, site_name, lon, lat))

# insert new flow record
def insertFlow(conn: any, flow_id: int, site_id: int, flow_name: str, bin_size: int):
    insert_query="""
    INSERT INTO ecocounter.flows_unfiltered (flow_id, site_id, flow_direction, bin_size, validated)
    VALUES (
        %s::numeric,
        %s::numeric,
        coalesce(lower(substring(%s::text, '(West|East|North|South)'))||'bound', 'unknown'),
        (%s::text || ' minutes')::interval,
        null::boolean --not validated
    )
    """
    with conn.cursor() as cur:
        cur.execute(insert_query, (flow_id, site_id, flow_name, bin_size))

#for testing/pulling data without use of airflow.
def run_api(
        start_date: datetime = default_start,
        end_date: datetime = default_end,
        sites: any = ()
):
    CONFIG_PATH = 'volumes/ecocounter/.api-credentials.config'
    config = ConfigParser()
    config.read(CONFIG_PATH)
    conn = connect(**config['DBSETTINGS'])
    conn.autocommit = True
    token = getToken(
        URL,
        config['API']['username'],
        config['API']['password'],
        config['API']['secret_api_hash']
    )
    for site in getSites(token, sites=sites): #optionally specify site_ids here. 
        # only update data for sites / flows in the database
        # but announce unknowns for manual validation if necessary
        if not siteIsKnownToUs(site['id'], conn):
            print('unknown site', site['id'], site['name'])
            continue
        pull_ecocounter_counts(start_date, end_date, site['id'], conn, token)
        

def pull_ecocounter_counts(start_date, end_date, site_id, conn, token):
    
    LOGGER.info(f'Pulling data from {start_date} to {end_date}.')
    total_rows = 0
    with conn.get_conn() as con:
        for flow_id in getKnownFlows(con, site_id):
            LOGGER.debug(f'Starting on flow {flow_id} for site {site_id}.')
            # empty the count table for this flow
            truncateFlowSince(flow_id, con, start_date, end_date)          
            # and fill it back up!
            LOGGER.debug(f'Fetching data for flow {flow_id}.')
            counts = getFlowData(token, flow_id, start_date, end_date)
            #convert response into a tuple for inserting
            volume=[]
            for count in counts:
                row=(flow_id, count['date'], count['counts'])
                volume.append(row)
            if len(volume) == 0:
                LOGGER.info(f'{len(volume)} rows fetched for flow {flow_id} of site {site_id}.')
            insertFlowCounts(con, volume)
            total_rows += len(volume)
        LOGGER.info(f'{len(volume)} rows inserted for site {site_id}.')
        if total_rows == 0:
            raise AirflowException(f'{len(volume)} rows inserted for site {site_id}.')