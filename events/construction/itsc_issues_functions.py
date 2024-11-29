import os
import logging
import pandas as pd
from numpy import nan
import struct
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

def fetch_and_insert_data(
    select_conn = PostgresHook('itsc_postgres'),
    insert_conn = PostgresHook('vds_bot'),
    start_date = None
):
    #generic function to pull and insert data using different connections and queries.
    select_fpath = os.path.join(SQL_DIR, 'select-itsc_issues.sql')
    with open(select_fpath, 'r', encoding="utf-8") as file:
        select_query = sql.SQL(file.read()).format(
            start = sql.Literal(start_date)
        )
    try:
        with select_conn.get_conn() as con, con.cursor() as cur:
            LOGGER.info(f"Fetching RODARS data.")
            cur.execute(select_query)
            data = cur.fetchall()
            df = pd.DataFrame(data)
            df.columns=[x.name for x in cur.description]
    except Error as exc:
        LOGGER.critical(f"Error fetching RODARS data.")
        LOGGER.critical(exc)
        raise Exception()
    
    #older rodars data doesn't have this value?
    df['locationindex'] = df['locationindex'].replace({nan: 0})
    
    geom_data = df['geometry'].map(geometry_from_bytes)
    valid_geoms = [not(x is None) for x in geom_data]
    
    geoms_df = df[['issueid', 'divisionid']][valid_geoms]
    geoms_df.insert(2, 'geom_text', geom_data[valid_geoms].map(coordinates_to_geomfromtext))
    geoms_df = geoms_df.replace({nan: None})
    geoms_df = [tuple(x) for x in geoms_df.to_numpy()]
    
    #transform values for inserting
    df_no_geom = df.drop('geometry', axis = 1)
    df_no_geom = df_no_geom.replace({pd.NaT: None, nan: None})
    df_no_geom = [tuple(x) for x in df_no_geom.to_numpy()]
    
    insert_fpath = os.path.join(SQL_DIR, 'insert-itsc_issues.sql')
    with open(insert_fpath, 'r', encoding="utf-8") as file:
        insert_query = sql.SQL(file.read())
        
    with insert_conn.get_conn() as con, con.cursor() as cur:
        execute_values(cur, insert_query, df_no_geom)
        
    geom_update_fpath = os.path.join(SQL_DIR, 'update-itsc_issues_geometry.sql')
    with open(geom_update_fpath, 'r', encoding="utf-8") as file:
        geom_update_query = sql.SQL(file.read())
        
    with insert_conn.get_conn() as con, con.cursor() as cur:
        execute_values(cur, geom_update_query, geoms_df)
        
#fetch_and_insert_data()