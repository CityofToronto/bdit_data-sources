import configparser
import requests
import datetime
from psycopg2 import connect
from psycopg2 import sql
from psycopg2.extras import execute_values
import logging
from time import sleep
import click
from pathlib import Path
import configparser
from psycopg2 import connect
CONFIG = configparser.ConfigParser()
CONFIG.read(str(Path.home().joinpath('db.cfg'))) #Creates a path to your db.cfg file
dbset = CONFIG['DB_SETTINGS']
con = connect(**dbset)

LOGGER = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

from gcc_puller_functions import (mapserver_name, get_tablename, get_fieldtype, to_time, find_limit, get_geometry)

def create_table(output_table, return_json, schema_name, con):
    """
    Function to create a new table in postgresql for the layer (for audited tables only)

    Parameter
    ---------
    output_table : string
        Table name for postgresql, returned from function get_tablename

    return_json : json
        Resulted json response from calling the api, returned from function get_data
    
    schema_name : string
        The schema in which the table will be inserted into
        
    primary_key : string
        Primary key for this layer, returned from dictionary pk_dict
    
    con: Airflow Connection
        Could be the connection to bigdata or to on-prem server

    Returns
    --------
    insert_columm : SQL composed
        Composed object of column name and types use for creating a new postgresql table
    
    excluded_column : SQL composed
        Composed object that is similar to insert_column, but has 'EXCLUDED.' attached before each column name, used for UPSERT query
    """
    
    fields = return_json['fields']
    insert_column_list = [sql.Identifier((field['name'].lower()).replace('.', '_')) for field in fields]
    insert_column_list.append(sql.Identifier('geom'))
    insert_column = sql.SQL(',').join(insert_column_list)
    
    # Since this is a temporary table, name it '_table' as opposed to 'table' for now
    temp_table_name = '_' + output_table
    
    with con:
        with con.cursor() as cur:
            
            col_list = [sql.Identifier((field['name'].lower()).replace('.', '_')) + sql.SQL(' ') + sql.SQL(get_fieldtype(field["type"])) for field in fields]
            col_list.append(sql.Identifier('geom') + sql.SQL(' ') + sql.SQL('geometry'))
            col_list_string = sql.SQL(',').join(col_list)
            
            LOGGER.info(col_list_string.as_string(con))
            create_sql = sql.SQL("CREATE TABLE IF NOT EXISTS {schema_table} ({columns})").format(schema_table = sql.Identifier(schema_name, temp_table_name),
                                                                      columns = col_list_string)
            LOGGER.info(create_sql.as_string(con))
            cur.execute(create_sql)

    return insert_column

def get_data(mapserver, layer_id, max_number = None, record_max = None):
    """
    Function to retreive layer data from GCCView rest api

    Parameters
    -----------
    mapserver : string
        The name of the mapserver we are accessing, returned from function mapserver_name

    layer_id : integer
        Unique layer id that represent a single layer in the mapserver

    max_number : integer
        Number for parameter `resultOffset` in the query, indicating the number of rows this query is going to skip

    record_max : integer
        Number for parameter `resultRecordCount` in the query, indicating the number of rows this query is going to fetch

    Returns
    --------
    return_json : json
        Resulted json response from calling the GCCView rest api
    """
    return_json = None
    base_url = "https://insideto-gis.toronto.ca/arcgis/rest/services/{}/MapServer/{}/query".format(mapserver, layer_id)
    
    query = {"where":"1=1",
             "outFields": "*",
             "outSR": '4326',
             "returnGeometry": "true",
             "returnTrueCurves": "false",
             "returnIdsOnly": "false",
             "returnCountOnly": "false",
             "returnZ": "false",
             "returnM": "false",
             "orderByFields": "OBJECTID", 
             "returnDistinctValues": "false",
             "returnExtentsOnly": "false",
             "resultOffset": "{}".format(max_number),
             "resultRecordCount": "{}".format(record_max),
             "f":"json"}
    
    for retry in range(3):
        try:
            r = requests.get(base_url, params = query, verify = False, timeout = 300)
            r.raise_for_status()
        except requests.exceptions.HTTPError as err_h:
            LOGGER.error("Invalid HTTP response: ", err_h)
        except requests.exceptions.ConnectionError as err_c:
            LOGGER.error("Network problem: ", err_c)
            sleep(10)
        except requests.exceptions.Timeout as err_t:
            LOGGER.error("Timeout: ", err_t)
        except requests.exceptions.RequestException as err:
            LOGGER.error("Error: ", err)
        else:
            return_json = r.json()
            break
    
    return return_json

def insert_data(output_table, insert_column, return_json, schema_name, con):
    """
    Function to insert data to our postgresql database, the data is inserted into a temp table (for audited tables)

    Parameters
    ----------
    output_table : string
        Table name for postgresql, returned from function get_tablename

    insert_column : SQL composed
        Composed object of column name and types use for creating a new postgresql table

    return_json : json
        Resulted json response from calling the api, returned from function get_data
    
    schema_name : string
        The schema in which the table will be inserted into
    
    con: Airflow Connection
        Could be the connection to bigdata or to on-prem server
    """
    rows = []
    features = return_json['features']
    fields = return_json['fields']
    trials = [[field['name'], field['type']] for field in fields]

    for feature in features:
        geom = feature['geometry']
        geometry_type = return_json['geometryType']
        geometry = get_geometry(geometry_type, geom)
        
        row = []
        for trial in trials:
            if trial[1] == 'esriFieldTypeDate' and feature['attributes'][trial[0]] != None:
                row.append(to_time(feature['attributes'][trial[0]]))
            else:
                row.append(feature['attributes'][trial[0]])

        row.append(geometry)
        
        rows.append(row)
    
    # Since this is a temporary table, name it '_table' as opposed to 'table' for now (for audited tables)
    temp_table_name = '_' + output_table
    
    insert=sql.SQL("INSERT INTO {schema_table} ({columns}) VALUES %s").format(
        schema_table = sql.Identifier(schema_name, temp_table_name), 
        columns = insert_column
    )
    with con:
        with con.cursor() as cur:
               execute_values(cur, insert, rows)
    LOGGER.info('Successfully inserted %d records into %s', len(rows), output_table)


def get_layer(mapserver_n, layer_id, schema_name, con = None):
    """
    This function calls to the GCCview rest API and inserts the outputs to the output table in the postgres database.

    Parameters
    ----------
    mapserver : int
        The name of the mapserver that host the desired layer

    layer_id : int
        The id of desired layer
    
    schema_name : string
        The schema in which the table will be inserted into

    con: connection to database
        Connection object that can connect to a particular database
        Expects a valid con object if using command prompt
    """
    successful_task_run = True

    # At this point, there should must be a con now
    if con is None:
        LOGGER.error("Unable to establish connection to the database, please pass in a valid con")
        return
    
    mapserver = mapserver_name(mapserver_n)
    output_table = get_tablename(mapserver, layer_id)
    if output_table is None:
        LOGGER.error("Invalid mapserver and/or layer Id")
        return
    #--------------------------------
    keep_adding = True
    counter = 0
    
    while keep_adding == True:
        
        if counter == 0:
            return_json = get_data(mapserver, layer_id)
            insert_column = create_table(output_table, return_json, schema_name, con)
        
            features = return_json['features']
            record_max=(len(features))
            max_number = record_max

            insert_data(output_table, insert_column, return_json, schema_name, con)

            counter += 1
            keep_adding = find_limit(return_json)
            if keep_adding == False:
                LOGGER.info('All records from [mapserver: %s, layerID: %d] have been inserted into %s', mapserver, layer_id, output_table)
        else:
            return_json = get_data(mapserver, layer_id, max_number = max_number, record_max = record_max)
            insert_data(output_table, insert_column, return_json, schema_name, con)
            
            counter += 1
            keep_adding = find_limit(return_json)
            if keep_adding == True:
                max_number = max_number + record_max
            else:
                LOGGER.info('All records from [mapserver: %s, layerID: %d] have been inserted into %s', mapserver, layer_id, output_table)
    
@click.command()
@click.option('--mapserver', '-m', type = int, required = True, 
                help = 'Mapserver number, e.g. cotgeospatial_2 will be 2')
@click.option('--layer-id', '-l', type = int, required = True
                , help = 'Layer id')
@click.option('--schema-name', '-s', type = str, required = True
                , help = 'Name of destination schema')
def manual_get_layer(mapserver, layer_id, schema_name, is_audited, con):
    """
    This script pulls a GIS layer from GCC servers into the databases of the Data and Analytics Unit.
    
    Example:

    python gcc_puller_functions.py --mapserver 28 --layer-id 28
    --schema-name gis --con db.cfg
    """
    CONFIG.read(con)
    dbset = CONFIG['DBSETTINGS']
    connection_obj = connect(**dbset)
    # get_layer function
    get_layer(mapserver, layer_id, schema_name, con=connection_obj)

if __name__ == '__main__':
    manual_get_layer()
