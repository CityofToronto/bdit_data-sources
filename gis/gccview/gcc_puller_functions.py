# The official new GCC puller functions file
import configparser
import requests
import datetime
from psycopg2 import connect
from psycopg2 import sql
from psycopg2.extras import execute_values
import logging
import click
CONFIG = configparser.ConfigParser()

"""The following provides information about the code when it is running and prints out the log messages 
if they are of logging level equal to or greater than INFO"""
LOGGER = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

def mapserver_name(mapserver_n):
    """
    Function to return the mapserver name from integer
    
    Parameters
    ------------
    mapserver_n : numeric
        The number of mapserver we will be accessing. 0 for 'cot_geospatial'
    
    Returns
    --------
    mapserver_name : string
        The name of the mapserver
    """
    
    if mapserver_n == 0:
        mapserver_name = 'cot_geospatial'
    else:
        mapserver_name = 'cot_geospatial' + str(mapserver_n)
    
    return(mapserver_name)

def get_tablename(mapserver, layer_id):
    """
    Function to return the name of the layer

    Parameters
    -----------
    mapserver: string
        The name of the mapserver we are accessing, returned from function mapserver_name
    
    layer_id: integer
        Unique layer id that represent a single layer in the mapserver
    
    Returns
    --------
    output_name
        The table name of the layer in database
    """
    
    url = 'https://insideto-gis.toronto.ca/arcgis/rest/services/'+mapserver+'/MapServer/layers?f=json'
    try:
        r = requests.get(url, verify = False, timeout = 20)
        r.raise_for_status()
    except requests.exceptions.HTTPError as err_h:
        LOGGER.error("Invalid HTTP response: ", err_h)
    except requests.exceptions.ConnectionError as err_c:
        LOGGER.error("Network problem: ", err_c)
    except requests.exceptions.Timeout as err_t:
        LOGGER.error("Timeout: ", err_t)
    except requests.exceptions.RequestException as err:
        LOGGER.error("Error: ", err)
    else:
        ajson = r.json()
        layers = ajson['layers']
        for layer in layers:
            if layer['id'] == layer_id:
                output_name = (layer['name'].lower()).replace(' ', '_')
        return output_name

def get_fieldtype(field):
    if field == 'esriFieldTypeInteger' or field == 'esriFieldTypeSingle' or field == 'esriFieldTypeInteger' or field=='esriFieldTypeOID' or field == 'esriFieldTypeSmallInteger' or field =='esriFieldGlobalID':
        fieldtype = 'integer'
    elif field == 'esriFieldTypeString':
        fieldtype = 'text'
    elif field == 'esriFieldTypeDouble':
        fieldtype = 'numeric'
    elif field == 'esriFieldTypeDate':
        fieldtype = 'timestamp without time zone'
    return fieldtype

def create_audited_table(output_table, return_json, schema_name, primary_key, con):
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
    
    # For audited tables only
    excluded_column_list = [sql.SQL('EXCLUDED.') + sql.Identifier((field['name'].lower()).replace('.', '_')) for field in fields]
    excluded_column_list.append(sql.SQL('EXCLUDED.') + sql.Identifier('geom'))
    excluded_column = sql.SQL(',').join(excluded_column_list)
    
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

            # owner_sql = sql.SQL("ALTER TABLE IF EXISTS {schema_table} OWNER to gis_admins").format(schema_table = sql.Identifier(schema_name, temp_table_name))
            # cur.execute(owner_sql)
    
    # Add a pk
    with con:
        with con.cursor() as cur:
            cur.execute(sql.SQL("ALTER TABLE {schema_table} ADD PRIMARY KEY ({pk})").format(schema_table = sql.Identifier(schema_name, temp_table_name),
                                                                                               pk = sql.Identifier(primary_key)))
    return insert_column, excluded_column

def create_partitioned_table(output_table, return_json, schema_name, con):
    """
    Function to create a new table in postgresql for the layer (for partitioned tables only)

    Parameter
    ---------
    output_table : string
        Table name for postgresql, returned from function get_tablename

    return_json : json
        Resulted json response from calling the api, returned from function get_data
    
    schema_name : string
        The schema in which the table will be inserted into
    
    con: Airflow Connection
        Could be the connection to bigdata or to on-prem server

    Returns
    --------
    insert_columm : SQL composed
        Composed object of column name and types use for creating a new postgresql table
    
    output_table_with_date : string
        Table name with date attached at the end, for partitioned tables in postgresql 
    """
    
    fields = return_json['fields']
    insert_column_list = [sql.Identifier((field['name'].lower()).replace('.', '_')) for field in fields]
    insert_column_list.insert(0, sql.Identifier('version_date'))
    insert_column_list.append(sql.Identifier('geom'))
    insert_column = sql.SQL(',').join(insert_column_list)
    
    # Date format YYYY-MM-DD, for the SQL query
    today_string = datetime.date.today().strftime('%Y-%m-%d')
    # Date format _YYYYMMDD, to be attached at the end of output_table name
    date_attachment = datetime.date.today().strftime('_%Y%m%d')
    output_table_with_date = output_table + date_attachment
    index_name = output_table_with_date + '_idx'
    
    with con:
        with con.cursor() as cur:
            
            create_sql = sql.SQL("CREATE TABLE IF NOT EXISTS {schema_child_table} PARTITION OF {schema_parent_table} FOR VALUES IN (%s)").format(schema_child_table = sql.Identifier(schema_name, output_table_with_date),
                                                                                                                                            schema_parent_table = sql.Identifier(schema_name, output_table))
            cur.execute(create_sql, (today_string, ))

            index_sql = sql.SQL("CREATE INDEX {idx_name} ON {schema_child_table} USING gist (geom)").format(idx_name=sql.Identifier(index_name),
                                                                                                                schema_child_table=sql.Identifier(schema_name, output_table_with_date))
            cur.execute(index_sql)
            
    return insert_column, output_table

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
    
    con: Connection
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
    
    with con:
        with con.cursor() as cur:
            
            col_list = [sql.Identifier((field['name'].lower()).replace('.', '_')) + sql.SQL(' ') + sql.SQL(get_fieldtype(field["type"])) for field in fields]
            col_list.append(sql.Identifier('geom') + sql.SQL(' ') + sql.SQL('geometry'))
            col_list_string = sql.SQL(',').join(col_list)
            
            LOGGER.info(col_list_string.as_string(con))
            create_sql = sql.SQL("CREATE TABLE IF NOT EXISTS {schema_table} ({columns})").format(schema_table = sql.Identifier(schema_name, output_table),
                                                                      columns = col_list_string)
            LOGGER.info(create_sql.as_string(con))
            cur.execute(create_sql)

    return insert_column

# Geometry Switcher 
def line(geom):
    return 'SRID=4326;LineString('+','.join(' '.join(str(x) for x in tup) for tup in geom['paths'][0]) +')'
def polygon(geom):
    return 'SRID=4326;MultiPolygon((('+','.join(' '.join(str(x) for x in tup) for tup in geom['rings'][0]) +')))'
def point(geom):
    return 'SRID=4326;Point('+(str(geom['x']))+' '+ (str(geom['y']))+')'  
def get_geometry(geometry_type, geom):
    switcher = {
        'esriGeometryLine':line,
        'esriGeometryPolyline': line,
        'esriGeometryPoint': point,
        'esriGeometryMultiPolygon': polygon,
        'esriGeometryPolygon': polygon
    }
    func = switcher.get(geometry_type)
    geometry = (func(geom)) 
    return geometry

def to_time(input):
    """
    Convert epoch time to postgresql timestamp without time zone

    Parameters
    -----------
    input : string
        Epoch time attribute in return_json

    Returns
    --------
    time : string
        Time in the type of postgresql timestamp without time zone
    """
    
    time = datetime.datetime.fromtimestamp(abs(input)/1000).strftime('%Y-%m-%d %H:%M:%S')
    return time

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
    
    # If the data we want to get is centreline
    if mapserver == 'cot_geospatial' and layer_id == 2:
        query = {"where": "\"FEATURE_CODE_DESC\" IN ('Collector','Collector Ramp','Expressway','Expressway Ramp','Local','Major Arterial','Major Arterial Ramp','Minor Arterial','Minor Arterial Ramp','Pending', 'Other')",
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
    else:
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
        except requests.exceptions.Timeout as err_t:
            LOGGER.error("Timeout: ", err_t)
        except requests.exceptions.RequestException as err:
            LOGGER.error("Error: ", err)
        else:
            return_json = r.json()
            break
    
    return return_json

def find_limit(return_json):
    """
    Function to check if last query return all rows

    Parameters
    -----------
    return_json : json
        Resulted json response from calling the api, returned from function get_data

    Returns
    --------
    keep_adding : Boolean
        boolean 'keep_adding' indicating if last query returned all rows in the layer
    """
    
    if return_json.get('exceededTransferLimit', False) == True:
        keep_adding = True
    else:
        keep_adding = False
    return keep_adding

def insert_data(output_table, insert_column, return_json, schema_name, con, is_audited, is_partitioned):
    rows = []
    features = return_json['features']
    fields = return_json['fields']
    trials = [[field['name'], field['type']] for field in fields]
    today_string = datetime.date.today().strftime('%Y-%m-%d')
    
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
                
        if is_partitioned:
            row.insert(0, today_string)
        row.append(geometry)
        
        rows.append(row)
    
    if is_audited:
        output_table = '_' + output_table

    insert=sql.SQL("INSERT INTO {schema_table} ({columns}) VALUES %s").format(
        schema_table = sql.Identifier(schema_name, output_table), 
        columns = insert_column
    )
        
    with con:
        with con.cursor() as cur:
               execute_values(cur, insert, rows)
    LOGGER.info('Successfully inserted %d records into %s', len(rows), output_table)

def update_table(output_table, insert_column, excluded_column, primary_key, schema_name, con):
    """
    Function to find differences between existing table and the newly created temp table, then UPSERT,
    the temp table will be dropped in the end (for audited tables only)

    Parameters
    ----------
    output_table : string
        Table name for postgresql, returned from function get_tablename

    insert_column : SQL composed
        Composed object of column name and types use for creating a new postgresql table
    
    excluded_column : SQL composed
        Composed object that is similar to insert_column, but has 'EXCLUDED.' attached before each column name, used for UPSERT query
    
    primary_key : string
        primary key for this layer, returned from dictionary pk_dict
    
    schema_name : string
        The schema in which the table will be inserted into
    
    con: Airflow Connection
        Could be the connection to bigdata or to on-prem server
    
    Returns
    --------
    successful_execution : Boolean
        whether any error had occured during UPSERT process
    """

    # Boolean to return, whether any error had occured during UPSERT process
    successful_execution = True

    # Name the temporary table '_table' as opposed to 'table' for now
    temp_table_name = '_' + output_table
    
    date = datetime.date.today().strftime('%Y-%m-%d')
    
    # Find if old table exists
    with con:
        with con.cursor() as cur:
            
            cur.execute(sql.SQL("SELECT COUNT(1) FROM information_schema.tables WHERE table_schema = %s AND table_name = %s"), (schema_name, output_table))
            result = cur.fetchone()
            # If table exists
            if result[0] == 1:
            
                try:
                    # Delete rows that no longer exist in the new table
                    cur.execute(sql.SQL("DELETE FROM {schema_tablename} WHERE {pk} IN (SELECT {pk} FROM {schema_tablename} EXCEPT SELECT {pk} FROM {schema_temp_table})").format(
                                                                                schema_tablename = sql.Identifier(schema_name, output_table), 
                                                                                pk = sql.Identifier(primary_key), 
                                                                                schema_temp_table = sql.Identifier(schema_name, temp_table_name)))

                    # And then upsert stuff
                    upsert_string = "INSERT INTO {schema_tablename} ({cols}) SELECT {cols} FROM {schema_temp_table} ON CONFLICT ({pk}) DO UPDATE SET ({cols}) = ({excl_cols}); COMMENT ON TABLE {schema_tablename} IS 'last updated: {date}'"
                    cur.execute(sql.SQL(upsert_string).format(schema_tablename = sql.Identifier(schema_name, output_table),
                                                              schema_temp_table = sql.Identifier(schema_name, temp_table_name),
                                                              pk = sql.Identifier(primary_key),
                                                              cols = insert_column,
                                                              excl_cols = excluded_column,
                                                              date = sql.Identifier(date)))
                    LOGGER.info('Updated table %s', output_table)
                except Exception:
                    # pass exception to function
                    LOGGER.exception("Failed to UPSERT")
                    # rollback the previous transaction before starting another
                    con.rollback()
                    successful_execution = False
            
            # if table does not exist -> create a new one and add to audit list
            else:
                try:
                    cur.execute(sql.SQL("ALTER TABLE {schema_temp_table} RENAME TO {tablename}; COMMENT ON TABLE {schema_tablename} IS 'last updated: {date}'").format(
                                                schema_temp_table = sql.Identifier(schema_name, temp_table_name), 
                                                tablename = sql.Identifier(output_table),
                                                schema_tablename = sql.Identifier(schema_name, output_table), 
                                                date = sql.Identifier(date)))

                    
                    # Make schema_name and output_table into a single string
                    target_audit_table = sql.Literal(schema_name + '.' + output_table)
                    cur.execute(sql.SQL("SELECT {schema}.audit_table({schematable})").format(schema = sql.Identifier(schema_name), 
                                                                                            schematable = target_audit_table))
                    LOGGER.info('New table %s created and added to audit table list', output_table)
                except Exception:
                    # pass exception to function
                    LOGGER.exception("Failed to create new table")
                    # rollback the previous transaction before starting another
                    con.rollback()
                    successful_execution = False
            
            # And then drop the temp table (if exists)
            cur.execute(sql.SQL("DROP TABLE IF EXISTS {schema_temp_table}").format(schema_temp_table = sql.Identifier(schema_name, temp_table_name)))
    return successful_execution
#-------------------------------------------------------------------------------------------------------
# base main function, also compatible with Airflow
def get_layer(mapserver_n, layer_id, schema_name, is_audited, cred = None, con = None, primary_key = None, is_partitioned = False):
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
    
    is_audited: Boolean
        Whether we want to have the table be audited (true) or be partitioned (false)
    
    cred: Airflow PostgresHook
        Contains credentials to enable a connection to a database
        Expects a valid cred input when running Airflow DAG
    
    con: connection to database
        Connection object that can connect to a particular database
        Expects a valid con object if using command prompt
    """
        
    # For Airflow DAG
    if cred is not None:
        con = cred.get_conn()
    
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
    #--------------------------------
    if is_audited and primary_key is None:
            LOGGER.error("Audited tables should have a primary key.")
    if not(is_audited) and primary_key is not None:
        LOGGER.error("Non-audited tables do not use the primary key.")
    #--------------------------------
    while keep_adding == True:
        if counter == 0:
            return_json = get_data(mapserver, layer_id)
            if is_audited:
                (insert_column, excluded_column) = create_audited_table(output_table, return_json, schema_name, primary_key, con)
            elif is_partitioned:
                (insert_column, output_table) = create_partitioned_table(output_table, return_json, schema_name, con)
            else:
                insert_column = create_table(output_table, return_json, schema_name, con)
            features = return_json['features']
            record_max=(len(features))
            max_number = record_max
        else:
            return_json = get_data(mapserver, layer_id, max_number = max_number, record_max = record_max)
        
        insert_data(output_table, insert_column, return_json, schema_name, con, is_audited, is_partitioned)
    
        counter += 1
        keep_adding = find_limit(return_json)
        
        if keep_adding:
            max_number += record_max
        else:
            LOGGER.info('All records from [mapserver: %s, layerID: %d] have been inserted into %s', mapserver, layer_id, output_table)
    
    if is_audited:
        try:
            update_table(output_table, insert_column, excluded_column, primary_key, schema_name, con)
        except Exception as err:
            LOGGER.exception("Unable to update table %s", err)
    
        
@click.command()
@click.option('--mapserver', '-ms', type = int, required = True, 
                help = 'Mapserver number, e.g. cotgeospatial_2 will be 2')
@click.option('--layer-id', '-ly', type = int, required = True
                , help = 'Layer id')
@click.option('--schema-name', '-s', type = str, required = True
                , help = 'Name of destination schema')
@click.option('--is-audited', '-a', is_flag=True, show_default=True, default=False, 
                help = 'Whether the table is supposed to be audited (T) or non-audited(F)')
@click.option('--primary-key', '-pk', type = str, default=None, required = False,
                help = 'Primary key. Only include if table is audited.')
@click.option('--con', '-c', type = str, required = True, 
                help = 'The path to the credential config file')
@click.option('--is-partitioned', '-p', is_flag=True, show_default=True, default=False, 
                help = 'Whether the table is supposed to be partitioned (T) or not partitioned (F)')
def manual_get_layer(mapserver, layer_id, schema_name, is_audited, primary_key, con, is_partitioned=True):
    """
    This script pulls a GIS layer from GCC servers into the databases of
    the Data and Analytics Unit.
    
    Example:

    python gcc_puller_functions.py --mapserver 28 --layer-id 28
    --schema-name gis --is-audited --con db.cfg
    """
    CONFIG.read(con)
    dbset = CONFIG['DBSETTINGS']
    connection_obj = connect(**dbset)
    # get_layer function
    get_layer(
        mapserver_n = mapserver,
        layer_id = layer_id,
        schema_name = schema_name,
        is_audited = is_audited,
        primary_key = primary_key,
        con=connection_obj,
        is_partitioned = is_partitioned
    )

if __name__ == '__main__':
    manual_get_layer()