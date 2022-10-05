# The official new GCC puller DAG file
import configparser
import requests
import datetime
from psycopg2 import connect
from psycopg2 import sql
from psycopg2.extras import execute_values
import logging
from time import sleep
import click
CONFIG = configparser.ConfigParser()
CONFIG.read('/home/bqu/db_ec2.cfg')
#CONFIG.read('/home/bqu/db_morbius.cfg')
dbset = CONFIG['DBSETTINGS']
con = connect(**dbset)

"""The following provides information about the code when it is running and prints out the log messages 
if they are of logging level equal to or greater than INFO"""
LOGGER = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

#-------------------------------------------------------------------------------------------------------
from airflow import DAG
from airflow.operators.python_operator import PythonOperator


DEFAULT_ARGS = {
 'owner': 'natalie',
 'depends_on_past': False,
 'start_date': datetime(2022, 10, 5),
 'email_on_failure': False, 
 'email': ['natalie.chan@toronto.ca'], 
 'retries': 0,
 'on_failure_callback': task_fail_slack_alert
}

#-------------------------------------------------------------------------------------------------------
def mapserver_name(mapserver_n):
    """
    Function to return the mapserver name from integer.
    
    Parameters
    ------------
    mapserver_n : numeric
        The number of mapserver we will be accessing. 0 for 'cot_geospatial'
    
    Returns
    --------
    mapserver_name : string
        The name of the mapserver
    """
    
    switcher ={
        0 : 'cot_geospatial',
        2 : 'cot_geospatial2',
        3 : 'cot_geospatial3',
        5 : 'cot_geospatial5',
        6 : 'cot_geospatial6',
        7 : 'cot_geospatial7',
        8 : 'cot_geospatial8',
        10 : 'cot_geospatial10',
        11 : 'cot_geospatial11',
        12 : 'cot_geospatial12',
        13 : 'cot_geospatial13',
        14 : 'cot_geospatial14',
        15 : 'cot_geospatial15',
        16 : 'cot_geospatial16',
        17 : 'cot_geospatial17',
        18 : 'cot_geospatial18',
        19 : 'cot_geospatial19',
        20 : 'cot_geospatial20',
        21 : 'cot_geospatial21',
        22 : 'cot_geospatial22',
        23 : 'cot_geospatial23',
        24 : 'cot_geospatial24',
        25 : 'cot_geospatial25',
        26 : 'cot_geospatial26',
        27 : 'cot_geospatial27',
        28 : 'cot_geospatial28'
        }
    mapserver_name = switcher.get(mapserver_n)
    return(mapserver_name)

def get_tablename(mapserver, layer_id, is_audited):
    """
    Function to retrieve the name of the layer

    Parameters
    -----------
    mapserver: string
        The mapserver that host the layer
    layer_id: integer
        The id of the layer
    is_audited: Boolean
        Whether we want to have the table be audited (true) or be partitioned (false)
    
    Returns
    --------
    output_name
        The table name of the layer in database
    """
    
    url = 'https://insideto-gis.toronto.ca/arcgis/rest/services/'+mapserver+'/MapServer/layers?f=json'
    r = requests.get(url, verify = False)
    ajson = r.json()
    layers = ajson['layers']
    for layer in layers:
        if layer['id'] == layer_id:
            output_name = (layer['name'].lower()).replace(' ', '_')
        else:
            continue

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

def create_audited_table(output_table, return_json, schema_name, primary_key):
    '''Create a new table in postgresql for the layer'''
    
    fields = return_json['fields']
    insert_column_list = [sql.Identifier((field['name'].lower()).replace('.', '_')) for field in fields]
    insert_column_list.append(sql.Identifier('geom'))
    insert_column = sql.SQL(',').join(insert_column_list)
    
    # For audited tables only
    excluded_column_list = [sql.SQL('EXCLUDED.') + sql.Identifier((field['name'].lower()).replace('.', '_')) for field in fields]
    excluded_column_list.append(sql.SQL('EXCLUDED.') + sql.Identifier('geom'))
    excluded_column = sql.SQL(',').join(excluded_column_list)
    
    print(excluded_column.as_string(con))
    
    print('insert_column ' + insert_column.as_string(con))
    
    # Since this is a temporary table, name it '_table' as opposed to 'table' for now
    temp_table_name = '_' + output_table
    
    with con:
        with con.cursor() as cur:
            
            col_list = [sql.Identifier((field['name'].lower()).replace('.', '_')) + sql.SQL(' ') + sql.SQL(get_fieldtype(field["type"])) for field in fields]
            col_list.append(sql.Identifier('geom') + sql.SQL(' ') + sql.SQL('geometry'))
            col_list_string = sql.SQL(',').join(col_list)
            
            create_sql = sql.SQL("CREATE TABLE IF NOT EXISTS {schema}.{table} ({columns})").format(schema = sql.Identifier(schema_name),
                                                                      table = sql.Identifier(temp_table_name),
                                                                      columns = col_list_string)
            print('create_sql' + create_sql.as_string(con))
            cur.execute(create_sql)
    
    # Add a pk
    with con:
        with con.cursor() as cur:
            cur.execute(sql.SQL("ALTER TABLE {schema}.{table} ADD PRIMARY KEY ({pk})").format(schema = sql.Identifier(schema_name),
                                                                                               table = sql.Identifier(temp_table_name),
                                                                                               pk = sql.Identifier(primary_key)))
    return insert_column, excluded_column

def create_partitioned_table(output_table, return_json, schema_name):
    fields = return_json['fields']
    insert_column_list = [sql.Identifier((field['name'].lower()).replace('.', '_')) for field in fields]
    insert_column_list.insert(0, sql.Identifier('version_date'))
    insert_column_list.append(sql.Identifier('geom'))
    insert_column = sql.SQL(',').join(insert_column_list)
    
    print(insert_column.as_string(con))
    
    # Date format YYYY-MM-DD, for the SQL query
    today_string = datetime.date.today().strftime('%Y-%m-%d')
    # Date format _YYYYMMDD, to be attached at the end of output_table name
    date_attachment = datetime.date.today().strftime('_%Y%m%d')
    output_table_with_date = output_table + date_attachment
    
    with con:
        with con.cursor() as cur:
            
            create_sql = sql.SQL("CREATE TABLE IF NOT EXISTS {child_table} PARTITION OF {schema}.{parent_table} FOR VALUES IN (%s)").format(child_table = sql.Identifier(output_table_with_date),
                                                                                                                                            schema = sql.Identifier(schema_name),
                                                                                                                                            parent_table = sql.Identifier(output_table))
            
            print('create_sql' + create_sql.as_string(con))
            cur.execute(create_sql, (today_string, ))
            
    return insert_column, output_table_with_date

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
    '''Convert epoch time to postgresql timestamp without time zone'''    
    time = datetime.datetime.fromtimestamp(abs(input)/1000).strftime('%Y-%m-%d %H:%M:%S')
    return time

def get_data(mapserver, layer_id, max_number = None, record_max = None):
    '''Get data from gcc view rest api'''        
    base_url = "https://insideto-gis.toronto.ca/arcgis/rest/services/{}/MapServer/{}/query".format(mapserver, layer_id)
    
    """ Added stuff """
    
    # If the data we want to get is centreline
    if mapserver == 'cot_geospatial' and layer_id == 2:
        query = {"where": "\"FEATURE_CODE_DESC\" IN ('Collector','Collector Ramp','Expressway','Expressway Ramp','Local','Major Arterial','Major Arterial Ramp','Minor Arterial','Minor Arterial Ramp','Pending')",
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
        print(query["where"])
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
    
    while True:
        try :
            r = requests.get(base_url, params = query, verify = False)
        except requests.exceptions.ConnectionErrors:
            sleep(10)
            continue
        else:
            return_json = r.json()
            break
    return return_json

def find_limit(return_json):
    '''Check if last query return all rows'''   
    if return_json.get('exceededTransferLimit', False) == True:
        keep_adding = True
    else:
        keep_adding = False
    return keep_adding

def insert_audited_data(output_table, insert_column, return_json, schema_name):
    '''Send data to postgresql'''   
    rows = []
    features = return_json['features']
    fields = return_json['fields']
    trials = [[field['name'], field['type']] for field in fields]
    for feature in features:
        geom = feature['geometry']
        geometry_type = return_json['geometryType']
        geometry = get_geometry(geometry_type, geom)
        row = [feature['attributes'][trial[0]] if trial[1] != 'esriFieldTypeDate' or feature['attributes'][trial[0]] == None else to_time(feature['attributes'][trial[0]]) for trial in trials]
        row.append(geometry)
        
        rows.append(row)
    
    # Since this is a temporary table, name it '_table' as opposed to 'table' for now (for audited tables)
    temp_table_name = '_' + output_table
    
    insert=sql.SQL("INSERT INTO {schema}.{table} ({columns}) VALUES %s").format(
        schema = sql.Identifier(schema_name), 
        table = sql.Identifier(temp_table_name), 
        columns = insert_column
    )
    with con:
        with con.cursor() as cur:
               execute_values(cur, insert, rows)
    LOGGER.info('Successfully inserted %d records into %s', len(rows), output_table)

def insert_partitioned_data(output_table_with_date, insert_column, return_json, schema_name):
    '''Send data to postgresql'''   
    
    today_string = datetime.date.today().strftime('%Y-%m-%d')
    
    rows = []
    features = return_json['features']
    fields = return_json['fields']
    trials = [[field['name'], field['type']] for field in fields]
    for feature in features:
        geom = feature['geometry']
        geometry_type = return_json['geometryType']
        geometry = get_geometry(geometry_type, geom)
        row = [feature['attributes'][trial[0]] if trial[1] != 'esriFieldTypeDate' or feature['attributes'][trial[0]] == None else to_time(feature['attributes'][trial[0]]) for trial in trials]
        
        row.insert(0, today_string)
        row.append(geometry)
        
        rows.append(row)

    
    insert=sql.SQL("INSERT INTO {schema}.{table} ({columns}) VALUES %s").format(
        schema = sql.Identifier(schema_name), 
        table = sql.Identifier(output_table_with_date), 
        columns = insert_column
    )
    with con:
        with con.cursor() as cur:
               execute_values(cur, insert, rows)
    LOGGER.info('Successfully inserted %d records into %s', len(rows), output_table_with_date)

pk_dict = {
	"city_ward": "area_id",
    "census_tract": "area_id",
    "neighbourhood_improvement_area": "area_id",
    "priority_neighbourhood_for_investment": "area_id",
    "ibms_district": "area_id",
    "ibms_grid": "area_id",
    "bikeway": "centreline_id",
    "traffic_camera": "objectid",
    "permit_parking_area": "objectid",
    "prai_transit_shelter": "objectid",
    "traffic_bylaw_point": "objectid",
    "traffic_bylaw_line": "objectid",
    "loop_detector": "objectid",
    "electrical_vehicle_charging_station": "objectid",
    "day_care_centre": "objectid",
    "middle_childcare_centre": "objectid",
    "business_improvement_area": "objectid",
    "proposed_business_improvement_area": "objectid",
    "film_permit_all": "objectid",
    "film_permit_parking_all": "objectid",
    "hotel": "objectid",
    "convenience_store": "objectid",
    "supermarket": "objectid",
    "place_of_worship": "objectid",
    "ymca": "objectid",
    "aboriginal_organization": "objectid",
    "attraction": "objectid",
    "dropin": "objectid",
    "early_years_centre": "objectid",
    "family_resource_centre": "objectid",
    "food_bank": "objectid",
    "longterm_care": "objectid",
    "parenting_family_literacy": "objectid",
    "retirement_home": "objectid",
    "senior_housing": "objectid",
    "shelter": "objectid",
    "social_housing": "objectid",
    "private_road": "objectid",
    "school": "objectid",
    "library": "objectid",
	}

def update_table(output_table, insert_column, excluded_column, primary_key, schema_name):
    """Function to find differences between existing table and the newly created table.

    Parameters
    ----------
    output_table : string
        Table name for postgresql, returned from function get_tablename

    insert_column : string
        String of column name and types use for creating a new postgresql table

    primary_key : string
        primary key for this layer, returned from function get_info

    """
    
    # Name the temporary table '_table' as opposed to 'table' for now
    temp_table_name = '_' + output_table
    
    now = datetime.datetime.now()
    date = (str(now.year)+str(now.month)+str(now.day))
    
    
    """
    insert_column1 = str(insert_column).split(",")
    
    print(insert_column1)
    
    except_column = '('
    for i in insert_column1:
            except_value = "EXCLUDED.{}".format(i)
            except_column = except_column + except_value +','
            
    print('except_column: ' + except_column)
    
    excluded_column = except_column[:-1]+')'
    print('excluded_column' + excluded_column)
    """
    # Find if old table exists
    with con:
        with con.cursor() as cur:
            
            cur.execute(sql.SQL("select count(1) from information_schema.tables where table_schema = %s and table_name = %s"), (schema_name, output_table))
            result = cur.fetchone()
            # If table exists
            if result[0] == 1:
                # Delete rows that no longer exists in the new table
                cur.execute(sql.SQL("delete from {schema}.{tablename} where {pk} = (select {pk} from {schema}.{tablename} except select {pk} from {schema}.{temp_table})").format(schema = sql.Identifier(schema_name),
                                                                                                                                                                                          tablename = sql.Identifier(output_table),
                                                                                                                                                                                          pk = sql.Identifier(primary_key),
                                                                                                                                                                                          temp_table = sql.Identifier(temp_table_name)))
                                                                                                                                                                                          
                # And then upsert stuff
                upsert_string = "insert into {schema}.{tablename} select * from {schema}.{temp_table} on conflict (" + primary_key + ") do update set ({cols}) = ({excl_cols}); comment on table {schema}.{tablename} is 'last updated: {date}'"
                cur.execute(sql.SQL(upsert_string).format(schema = sql.Identifier(schema_name),
                                                          tablename = sql.Identifier(output_table),
                                                          temp_table = sql.Identifier(temp_table_name),
                                                          cols = insert_column,
                                                          excl_cols = excluded_column,
                                                          date = sql.Identifier(date)))
                # And then drop the temp table
                cur.execute(sql.SQL("drop table if exists {schema}.{temp_table}").format(schema = sql.Identifier(schema_name),
                                                                                                 temp_table = sql.Identifier(temp_table_name)))
                LOGGER.info('Updated table %s', output_table)           
            # if table does not exist -> create a new one and add to audit list
            else:
                cur.execute(sql.SQL("alter table {schema}.{temp_table} rename to {tablename}; comment on table {schema}.{tablename} is 'last updated: {date}'").format(schema = sql.Identifier(schema_name),
                                                                                                                                                                               temp_table = sql.Identifier(temp_table_name),
                                                                                                                                                                               tablename = sql.Identifier(output_table),
                                                                                                                                                                               date = sql.Identifier(date)))
                
                cur.execute(sql.SQL("select {schema}.audit_table({schema}.{tablename})").format(schema = sql.Identifier(schema_name),
                                                                                                 tablename = sql.Identifier(output_table)))
                LOGGER.info('New table %s created and added to audit table list', output_table)
                
# Added 'schema_name' to the function
def get_layer(mapserver_n, layer_id, schema_name, is_audited):
    
    """
    This function calls to the GCCview rest API and inserts the outputs to the output table in the postgres database.

    Parameters
    ----------
    mapserver : int
        The name of the mapserver that host the desire layer

    layer_id : int
        The id of desire layer
        
    """  
    mapserver = mapserver_name(mapserver_n)
    output_table = get_tablename(mapserver, layer_id, is_audited)
    #--------------------------------
    if is_audited:
        primary_key = pk_dict.get(output_table)
    #--------------------------------
    keep_adding = True
    counter = 0
    
    while keep_adding == True:
        
        if counter == 0:
            return_json = get_data(mapserver, layer_id)
            if is_audited:
                (insert_column, excluded_column) = create_audited_table(output_table, return_json, schema_name, primary_key)
            else:
                (insert_column, output_table_with_date) = create_partitioned_table(output_table, return_json, schema_name)
            
            features = return_json['features']
            record_max=(len(features))
            max_number = record_max
            
            if is_audited:
                insert_audited_data(output_table, insert_column, return_json, schema_name)
            else:
                insert_partitioned_data(output_table_with_date, insert_column, return_json, schema_name)
            
            counter += 1
            keep_adding = find_limit(return_json)
            if keep_adding == False:
                LOGGER.info('All records from [mapserver: %s, layerID: %d] have been inserted into %s', mapserver, layer_id, output_table)
        else:
            return_json = get_data(mapserver, layer_id, max_number = max_number, record_max = record_max)
            if is_audited:
                insert_audited_data(output_table, insert_column, return_json, schema_name)
            else:
                insert_partitioned_data(output_table_with_date, insert_column, return_json, schema_name)
            
            counter += 1
            keep_adding = find_limit(return_json)
            if keep_adding == True:
                max_number = max_number + record_max
            else:
                LOGGER.info('All records from [mapserver: %s, layerID: %d] have been inserted into %s', mapserver, layer_id, output_table)
    
    if is_audited:
        update_table(output_table, insert_column, excluded_column, primary_key, schema_name)

#-------------------------------------------------------------------------------------------------------
vfh32_layers = {"city_ward": [0, 0, 'bqu', True],
                "centreline": [0, 2, 'bqu', False],
                "intersection": [12, 42, 'bqu', False],
                "ibms_grid": [11, 25, 'bqu', True]
}

gcc_layers_dag = DAG(
    'pull_gcc_layers',
    default_args=DEFAULT_ARGS,
    schedule_interval='@daily'
)

for layer in vfh32_layers:
    pull_layer = PythonOperator(
        task_id = 'Task_'+ str(layer),
        python_callable = get_layer,
        op_args = vfh32_layers[layer]
    )