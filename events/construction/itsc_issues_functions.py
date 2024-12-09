import os
import logging
import pandas as pd
from numpy import nan
import struct
import json
from io import BytesIO
from psycopg2 import sql, Error
from psycopg2.extras import execute_values

from airflow.providers.postgres.hooks.postgres import PostgresHook

#fpath = '/data/home/gwolofs/bdit_data-sources/events/rodars/itsc_issues_functions.py'
#SQL_DIR = os.path.join(os.path.abspath(os.path.dirname(fpath)), 'sql')
SQL_DIR = os.path.join(os.path.abspath(os.path.dirname(__file__)), 'sql')

LOGGER = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

def coordinates_from_binary(br):
    # Read longitude and latitude as doubles (8 bytes each)
    longitude, latitude = struct.unpack('dd', br.read(16))
    return (longitude, latitude)

def coordinates_to_geomfromtext(l):
    geom_type = 'POINT' if len(l) == 1 else 'LINESTRING'
    coords = ', '.join([f"{x[0]} {x[1]}" for x in l])
    return (f"{geom_type}({coords})")

def geometry_from_bytes(geo_bytes):
    # Initialize a stream to read binary data from the byte array
    coordinates_list = []
    with BytesIO(geo_bytes) as ms:
        # Read the first 4 bytes = length
        try:
            len_val = struct.unpack('i', ms.read(4))[0]
        except struct.error:
            #struct.error: unpack requires a buffer of 4 bytes
            return None
        # Iterate and unpack each pair of doubles as coordinates
        for _ in range(len_val):
            coordinates = coordinates_from_binary(ms)
            coordinates_list.append(coordinates)
        return coordinates_list

def process_lanesaffected(json_str):
    if (json_str == 'Unknown') | (json_str is None):
        return None
    try:
        lanesaffected = json.loads(json_str)
    except json.decoder.JSONDecodeError as e:
        LOGGER.debug("Json str not parsed: %s", json_str)
        LOGGER.debug(e)
        return None
    #expand laneapproach nested json
    try:
        lanes = pd.json_normalize(lanesaffected, 'LaneApproaches')
    except TypeError as e:
        LOGGER.debug("Json str not parsed: %s", lanesaffected)
        LOGGER.debug(e)
        return None
    lanes = lanes.rename(columns={
        'FeatureId': 'centreline_id',
        'RoadId': 'linear_name_id'
    })
    #add extra, non-nested variables
    keys = list(lanesaffected.keys())
    keys.remove('LaneApproaches')
    if keys is not None:
        for key in keys:
            lanes.insert(0, f"{key}_toplevel", lanesaffected[key])
    return lanes

def fetch_and_insert_issue_data(
    select_conn = PostgresHook('itsc_postgres'),
    insert_conn = PostgresHook('vds_bot'),
    start_date = None
):
    select_fpath = os.path.join(SQL_DIR, 'select-itsc_issues.sql')
    with open(select_fpath, 'r', encoding="utf-8") as file:
        select_query = sql.SQL(file.read()).format(
            start = sql.Literal(start_date)
        )
    try:
        with select_conn.get_conn() as con, con.cursor() as cur:
            LOGGER.info("Fetching RODARS data.")
            cur.execute(select_query)
            data = cur.fetchall()
            df = pd.DataFrame(data)
            df.columns=[x.name for x in cur.description]
    except Error as exc:
        LOGGER.critical("Error fetching RODARS data.")
        LOGGER.critical(exc)
        raise Exception()
    
    #transform values for inserting
    df_final = df.replace({pd.NaT: None, nan: None})
    df_final = [tuple(x) for x in df_final.to_numpy()]
    
    insert_fpath = os.path.join(SQL_DIR, 'insert-itsc_issues.sql')
    with open(insert_fpath, 'r', encoding="utf-8") as file:
        insert_query = sql.SQL(file.read())
        
    with insert_conn.get_conn() as con, con.cursor() as cur:
        execute_values(cur, insert_query, df_final)

def fetch_and_insert_location_data(
    select_conn = PostgresHook('itsc_postgres'),
    insert_conn = PostgresHook('vds_bot'),
    start_date = None
):
    #generic function to pull and insert data using different connections and queries.
    select_fpath = os.path.join(SQL_DIR, 'select-itsc_issue_locations.sql')
    with open(select_fpath, 'r', encoding="utf-8") as file:
        select_query = sql.SQL(file.read()).format(
            start = sql.Literal(start_date)
        )
    try:
        with select_conn.get_conn() as con, con.cursor() as cur:
            LOGGER.info("Fetching RODARS data.")
            cur.execute(select_query)
            data = cur.fetchall()
            df = pd.DataFrame(data)
            df.columns=[x.name for x in cur.description]
    except Error as exc:
        LOGGER.critical("Error fetching RODARS data.")
        LOGGER.critical(exc)
        raise Exception()
    
    pkeys = ['divisionid', 'issueid', 'timestamputc', 'locationindex']
    
    geom_data = df['geometry'].map(geometry_from_bytes)
    valid_geoms = [not(x is None) for x in geom_data]
    geoms_df = df[pkeys][valid_geoms]
    geoms_df.insert(3, 'geom_text', geom_data[valid_geoms].map(coordinates_to_geomfromtext))
    df_no_geom = pd.merge(df.drop('geometry', axis = 1), geoms_df, on = pkeys)
    df_no_geom = df_no_geom

    expanded_list = []
    for row in df_no_geom.iterrows():
        expanded = process_lanesaffected(row[1]['lanesaffected'])
        # Add primary key columns to the expanded data
        if expanded is None:
            continue
        for col in pkeys:
            expanded[col] = row[1][col]
        expanded_list.append(expanded)
    df_expanded = pd.concat(expanded_list, ignore_index=True)
    
    df = pd.merge(df, df_expanded, on = pkeys)
        
    #transform values for inserting
    df_no_geom = df_no_geom.replace({pd.NaT: None, nan: None})
    
    #check if there are extra columns unnested from the json
    cols_to_insert = [
        'divisionid', 'issueid', 'timestamputc', 'locationindex', 'mainroadname', 'fromroadname',
        'toroadname', 'direction_toplevel', 'lanesaffected', 'streetnumber', 'locationtype', 'groupid',
        'groupdescription', 'LocationBlockLevel_toplevel', 'RoadClosureType_toplevel',
        'EncodedCoordinates_toplevel', 'LocationDescription_toplevel', 'Direction', 'RoadName',
        'centreline_id', 'linear_name_id', 'LanesAffectedPattern', 'LaneBlockLevel',
        'RoadClosureType', 'geom_text'
    ]
    extra_cols = [col for col in df_no_geom.columns if col not in cols_to_insert]
    if extra_cols != []:
        LOGGER.warning(f'There are extra columns unpacked from json not being inserted: %s', extra_cols)
    missing_cols = [col for col in cols_to_insert if col not in df_no_geom.columns]
    if missing_cols != []:
        for col in missing_cols:
            df_no_geom.insert(0, col, None)

    #arrange columns for inserting
    df_no_geom = df_no_geom[cols_to_insert]
    df_no_geom = [tuple(x) for x in df_no_geom.to_numpy()]
    
    insert_fpath = os.path.join(SQL_DIR, 'insert-itsc_issues_locations.sql')
    with open(insert_fpath, 'r', encoding="utf-8") as file:
        insert_query = sql.SQL(file.read())
        
    with insert_conn.get_conn() as con, con.cursor() as cur:
        execute_values(cur, insert_query, df_no_geom)

#fetch_and_insert_data()