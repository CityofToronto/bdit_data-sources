import requests
import logging
from configparser import ConfigParser
from psycopg2 import connect
from psycopg2.extras import execute_values
from datetime import datetime, timedelta
#from tqdm import tqdm # this is a progress bar created by simply wrapping an iterable

default_start = datetime.now().replace(hour = 0, minute = 0, second = 0, microsecond = 0)-timedelta(days=1)
default_end = datetime.now().replace(hour = 0, minute = 0, second = 0, microsecond = 0)

url = 'https://apieco.eco-counter-tools.com'

LOGGER = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)
LOGGER.setLevel('DEBUG')

# get an authentication token for accessing the API
def getToken(api_config_path):
    config = ConfigParser()
    config.read(api_config_path)
    response = requests.post(
        f'{url}/token',
        headers={
            'Authorization': 'Basic ' + config['API']['secret_api_hash']
        },
        data={
            'grant_type': 'password',
            'username': config['API']['username'],
            'password': config['API']['password']
        }
    )
    return response.json()['access_token']

# get a list of all sites from the API
def getSites(token, sites=()):
    response = requests.get(
        f'{url}/api/site',
        headers={'Authorization': f'Bearer {token}'}
    )
    result = []
    if not sites == ():
        for site in response.json():
            if site['id'] in sites:
                result.append(site)
        return result
    else:
        return response.json()

# get all of a channel/flow's data from the API
def getChannelData(token, channel_id: int, startDate: datetime, endDate: datetime):
    requestChunkSize = timedelta(days=100)
    requestStartDate = startDate
    data = []
    while requestStartDate < endDate:
        response = requests.get(
            f'{url}/api/data/site/{channel_id}',
            headers={'Authorization': f'Bearer {token}'},
            params={
                'begin': requestStartDate.isoformat(timespec='seconds'),
                'end':  min(requestStartDate + requestChunkSize, endDate).isoformat(timespec='seconds'),
                'complete': 'false',
                'step': '15m'
            }
        )
        data += response.json()
        requestStartDate += requestChunkSize
    return data

def getKnownSites(conn):
    with conn.cursor() as cur:
        cur.execute('SELECT site_id FROM gwolofs.sites;')
        sites = cur.fetchall()
        return [site[0] for site in sites]

def getKnownChannels(conn, site):
    with conn.cursor() as cur:
        cur.execute('SELECT flow_id FROM gwolofs.flows WHERE site_id = %s;',
                    (site, )
        )
        channels = cur.fetchall()
        return [channel[0] for channel in channels]

# do we have a record of this site in the database?
def siteIsKnownToUs(site_id: int, conn):
    with conn.cursor() as cursor:
        cursor.execute(
            "SELECT 1 FROM gwolofs.sites WHERE site_id = %s;",
            (site_id,)
        )
        return cursor.rowcount > 0

# do we have a record of this flow in the database?
def flowIsKnownToUs(flow_id: int, conn):
    with conn.cursor() as cursor:
        cursor.execute(
            "SELECT 1 FROM gwolofs.flows WHERE flow_id = %s;",
            (flow_id,)
        )
        return cursor.rowcount > 0

# DANGER delete all count data for a given flow
def truncateFlowSince(flow_id: int, conn: any, startDate: datetime, endDate: datetime):
    with conn.cursor() as cursor:
        cursor.execute(
            """DELETE FROM gwolofs.counts
            WHERE flow_id = %s
            AND datetime_bin >= %s
            AND datetime_bin < %s""",
            (flow_id, startDate, endDate)
        )

# insert records
def insertFlowCounts(conn, volume):
    insert_query="INSERT INTO gwolofs.counts (flow_id, datetime_bin, volume) VALUES %s"
    with conn.cursor() as cur:
        execute_values(cur, insert_query, volume)
    return cur.query

# insert new site record
def insertSite(conn, site_id, site_name, lon, lat):
    insert_query="""
    INSERT INTO gwolofs.sites (site_id, site_description, geom, validated)
    VALUES (
        %s::numeric,
        %s::text,
        ST_SetSRID(ST_MakePoint(%s, %s), 4326),
        False::boolean --not validated
    )
    """
    with conn.cursor() as cur:
        cur.execute(insert_query, (site_id, site_name, lon, lat))

# insert new flow record
def insertFlow(conn, flow_id, site_id, flow_name, bin_size):
    insert_query="""
    INSERT INTO gwolofs.flows (flow_id, site_id, flow_direction, bin_size, validated)
    VALUES (
        %s::numeric,
        %s::numeric,
        coalesce(lower(substring(%s::text, '(West|East|North|South)'))||'bound', 'unknown'),
        (%s::text || ' minutes')::interval,
        False::boolean --not validated
    )
    """
    with conn.cursor() as cur:
        cur.execute(insert_query, (flow_id, site_id, flow_name, bin_size))

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
    token = getToken(CONFIG_PATH)
    for site in getSites(token, sites=sites): #optionally specify site_ids here. 
        # only update data for sites / channels in the database
        # but announce unknowns for manual validation if necessary
        if not siteIsKnownToUs(site['id'], conn):
            print('unknown site', site['id'], site['name'])
            continue
        for channel in site['channels']:
            if not flowIsKnownToUs(channel['id'], conn):
                print('unknown flow', channel['id'])
                continue
            # we do have this site and channel in the database; let's update its counts
            channel_id = channel['id']
            print(f'starting on flow {channel_id}')
            # empty the count table for this flow
            truncateFlowSince(channel_id, conn, start_date, end_date)
            # and fill it back up!
            print(f'fetching data for flow {channel_id}')
            counts = getChannelData(token, channel_id, start_date, end_date)
            print(f'inserting data for flow {channel_id}')
            volume=[]
            for count in counts:
                row=(channel_id, count['date'], count['counts'])
                volume.append(row)
            insertFlowCounts(conn, volume)