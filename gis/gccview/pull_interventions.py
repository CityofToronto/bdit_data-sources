
import configparser
import requests
from pathlib import Path
import datetime
from psycopg2 import connect
from psycopg2 import sql
from psycopg2.extras import execute_values
from time import sleep
import click
CONFIG = configparser.ConfigParser()
CONFIG.read(r'/home/nchan/db.cfg')
dbset = CONFIG['DBSETTINGS']
con = connect(**dbset)

def create_table(output_table, return_json, primary_key):
    """Function to create a new table in postgresql for the layer
    Parameter
    ---------
    output_table : string
        Table name for postgresql, returned from function get_tablename
    return_json : json
        Resulted json response from calling the api, returned from function get_data
    primary_key : string
        primary key for this layer, returned from function get_info
    Returns
    --------
    insert_columm : string
        String of column name and types use for creating a new postgresql table
    """
    new_column = '('
    insert_column= '('
    fields = return_json['fields']
    for field in fields:
        if field['type'] == 'esriFieldTypeInteger' or field['type'] == 'esriFieldTypeSingle' or field['type'] == 'esriFieldTypeInteger' or field['type'] =='esriFieldTypeOID' or field['type'] == 'esriFieldTypeSmallInteger' or field['type'] =='esriFieldGlobalID':
                column_type = 'integer'    
        elif field['type'] == 'esriFieldTypeString':
                column_type = 'text'      
        elif field['type'] == 'esriFieldTypeDouble':   
                column_type = 'numeric'        
        elif field['type'] == 'esriFieldTypeDate':   
                column_type = 'timestamp without time zone'
        
        column_name = (field['name'].lower()).replace('.', '_') 
        new_column = new_column + column_name +' '+column_type+', '
        insert_column = insert_column + column_name +','

    new_column = new_column +'geom geometry)' 
    insert_column = insert_column + 'geom)'

    with con:
        with con.cursor() as cur:   
            cur.execute("create table covid_gis._{} {}".format(output_table, new_column))    

    # Add primary key
    with con:

        with con.cursor() as cur:
            
            cur.execute("alter table covid_gis._{} add primary key ({})".format(output_table, primary_key)) 
        
    return insert_column  

def send_tempdata(output_table, insert_column, return_json):
    """Function to send data to our postgresql database.
    Parameters
    ----------
    output_table : string
        Table name for postgresql, returned from function get_tablename
    insert_column : string
        List of column name for SQL data inserting query
    return_json : json
        Resulted json response from calling the api, returned from function get_data
    """   
    rows = []
    features = return_json['features'] 
    fields = return_json['fields']
    trials = [[field['name'], field['type']] for field in fields]
    for feature in features:
        try:
            geom = feature['geometry']
        except KeyError:
            geom = None
        geometry_type = return_json['geometryType']
        geometry = get_geometry(geometry_type, geom)
        row = [feature['attributes'][trial[0]] if trial[1] != 'esriFieldTypeDate' or feature['attributes'][trial[0]] == None else to_time(feature['attributes'][trial[0]]) for trial in trials]
        row.append(geometry)
        rows.append(row)
    
    sql='INSERT INTO covid_gis._{} {} VALUES %s'.format(output_table, insert_column)
    with con:
        with con.cursor() as cur:
               execute_values(cur,sql, rows)    
    print('sent')

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
    if geom is None:
        geometry = None
    else:
        geometry = (func(geom)) 
    return geometry

def to_time(input):
    """
    Convert epoch time to postgresql timestamp without time zone

    Parameters
    -----------
    input : string
        epoch time attribute in return_json

    Returns
    --------
    time : string
        time in the type of postgresql timestamp without time zone
    """      
    time = datetime.datetime.fromtimestamp(abs(input)/1000).strftime('%Y-%m-%d %H:%M:%S')
    return time

def get_data(mapserver, id, max_number = None, record_max = None):
    """
    Function to retreive layer data from GCCView rest api.

    Parameters
    -----------
    mapserver : string
        The name of the mapserver we are accessing, returned from function mapserver_name

    id : numeric
        unique layer id that represent a single layer in the mapserver

    max_number : numeric
        Number for parameter `resultOffset` in the query, indicating the number of rows this query is going to skip

    record_max : numeric
        Number for parameter `resultRecordCount` in the query, indicating the number of rows this query is going to fetch

    Returns
    --------
    return_json : json
        Resulted json response from calling the GCCView rest api

    """     
    if max_number == None: 
        max_number = ''
    else:
        max_number = max_number
    if record_max == None:
        record_max =''
    else:
        record_max = record_max
    base_url = "https://services3.arcgis.com/b9WvedVPoizGfvfD/ArcGIS/rest/services/{}/FeatureServer/{}/query".format(mapserver, id)
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
    """
    Function to check if last query return all rows

    Parameters
    -----------
    return_json : json
        Resulted json response from calling the api, returned from function get_data

    Returns
    --------
    rule : string
        boolean rule "add" or "dont add" indicating if last query returned all rows in the layer
    """ 
    if return_json.get('exceededTransferLimit', False) == True:
        rule = 'add'
    else:
        rule = 'dont add'  
    return rule   


def find_diff(insert_column, primary_key, where_id):
    """Function to find differences between existing table and the newly created table.
    Parameters
    ----------
    output_table : string
        Table name for postgresql, returned from function get_tablename
    insert_column : string
        String of column name and types use for creating a new postgresql table
    primary_key : string
        primary key for this layer, returned from function get_info
    where_id : string
        column name for where the upsert sql is going to look for differences, returned from function get_info
    """    
    now = str(datetime.datetime.now())
    
    insert_column1 = ((insert_column.replace('(', '')).replace(')', '')).split(",")
    except_column = '('
    for i in insert_column1:
            except_value = "EXCLUDED.{}".format(i)
            except_column = except_column + except_value +','        
   
    excluded_column = except_column[:-1]+')'
                

    # Find if old table exists
    with con:
        with con.cursor() as cur:
            cur.execute("select count(1) from information_schema.tables where table_schema = 'covid_gis' and table_name = 'curbto_installations'")
            result = cur.fetchone()
            # If table exists
            if result[0] == 1:
                with con:
                    with con.cursor() as cur:
                        cur.execute(''' WITH data AS (                 
                                        select * from covid_gis._curbto_installations)
                                        , ups AS (
                                        INSERT INTO covid_gis.curbto_installations AS t
                                        TABLE  data  
                                        ON     CONFLICT (objectid) DO UPDATE
                                        SET    objectid = t.objectid
                                        WHERE  t.dateofclosure <> Excluded.dateofclosure
                                        RETURNING t.objectid
                                        )
                                        , dele AS (
                                        DELETE FROM covid_gis.curbto_installations AS t
                                        USING  data  d
                                        RIGHT   JOIN covid_gis.curbto_installations u USING (objectid)
                                        WHERE  d.objectid IS NULL            
                                        RETURNING t.objectid
                                        )
                                        , del AS (
                                        DELETE FROM covid_gis.curbto_installations AS t
                                        USING  data     d
                                        LEFT   JOIN ups u USING (objectid)
                                        WHERE  u.objectid IS NULL            
                                        AND    t.objectid = d.objectid  AND    t <> d  
                                        RETURNING t.objectid
                                        )
                                        , ins AS (
                                        INSERT INTO covid_gis.curbto_installations AS t
                                        SELECT *
                                        FROM   data
                                        JOIN   del USING (objectid)   
                                        RETURNING objectid
                                        )
                                        SELECT ARRAY(TABLE ups) AS inserted 
                                            , ARRAY(TABLE ins) AS updated; 
                                        COMMENT on table covid_gis.curbto_installations is 'last updated: {}' '''.format(now)) 
                            
                # And then drop the temp table
                with con:
                    with con.cursor() as cur:
                         cur.execute("drop table covid_gis._curbto_installations")
                print('Updated table')           
            # if table does not exists -> create a new one
            else: 
                with con:
                    with con.cursor() as cur:
                        cur.execute("alter table covid_gis._curbto_installations rename to curbto_installations; comment on table covid_gis.curbto_installations is 'last updated: {}'".format(now))

def get_data(mapserver, id, max_number = None, record_max = None):
    """
    Function to retreive layer data from GCCView rest api.

    Parameters
    -----------
    mapserver : string
        The name of the mapserver we are accessing, returned from function mapserver_name

    id : numeric
        unique layer id that represent a single layer in the mapserver

    max_number : numeric
        Number for parameter `resultOffset` in the query, indicating the number of rows this query is going to skip

    record_max : numeric
        Number for parameter `resultRecordCount` in the query, indicating the number of rows this query is going to fetch

    Returns
    --------
    return_json : json
        Resulted json response from calling the GCCView rest api

    """        
    if max_number == None: 
        max_number = ''
    else:
        max_number = max_number
    if record_max == None:
        record_max =''
    else:
        record_max = record_max
    base_url = url = "https://services3.arcgis.com/b9WvedVPoizGfvfD/ArcGIS/rest/services/{}/FeatureServer/{}/query".format(mapserver, id)
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
    print(max_number)
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

@click.command()
@click.option('--mapserver', help = 'Mapserver name, e.g. cotgeospatial_2 will be 2', type=str)
@click.option('--id', help = 'layer id', type=int)
def get_layer(mapserver, id):
    
    """
    This function calls to the GCCview rest API and inserts the outputs to the output table in the postgres database.
    Parameters
    ----------
    mapserver : str
        The name of the mapserver that host the desire layer
    id : int
        The id of desire layer
        
    """  
    primary_key = 'objectid'
    where_id = 'dateofclosure'
    rule = "add"
    counter = 0
    output_table = 'curbto_installations'
    while rule == "add":
           
        if counter == 0:
            return_json = get_data(mapserver, id)
            insert_column = create_table(output_table, return_json, primary_key)
            features = return_json['features']
            record_max=(len(features))
            max_number = record_max
            send_tempdata(output_table, insert_column, return_json)
            counter += 1
            rule = find_limit(return_json)
            if rule != 'add':
                print('all rows inserted in ', output_table)
        else:
            return_json = get_data(mapserver, id, max_number = max_number, record_max = record_max)
            send_tempdata(output_table, insert_column, return_json)
            counter += 1
            rule = find_limit(return_json)
            if rule == 'add':
                max_number = max_number + record_max
            else:
                print('all rows inserted in ', output_table)
    
    find_diff(insert_column, primary_key, where_id)

if __name__ == '__main__':
        get_layer() 
