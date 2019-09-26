'''
This script pulls spatial layers from gccview to your own schema. 
'''
import configparser
import requests
import datetime
from psycopg2 import connect
from psycopg2 import sql
from psycopg2.extras import execute_values
from time import sleep
import click
CONFIG = configparser.ConfigParser()
CONFIG.read(r'C:\Users\nchan6\Documents\db.cfg')
dbset = CONFIG['DBSETTINGS']
con = connect(**dbset)



def mapserver_name(mapserver_n):
"""
Function to return cot_geospatial mapserver from integer

Parameters
-----------
mapserver_n 
    the mapserver number

Return
-------
map
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
    func = switcher.get(mapserver_n)
    return(func)

def get_tablename(mapserver, id):
"""
Function to retrieve the name of the layer

Parameters
-----------
mapserver
    The mapserver that host the layer
id
    The id of the layer

Returns
--------
output_name
    The table name of the layer
"""
    url = 'https://insideto-gis.toronto.ca/arcgis/rest/services/'+mapserver+'/MapServer/layers?f=json'
    r = requests.get(url, verify = False)
    ajson = r.json()
    layers = ajson['layers']
    for layer in layers:
        if layer['id'] == id:
            output_name = (layer['name'].lower()).replace(' ', '_')
        else:
            continue
    return output_name       

def create_table(output_table, return_json):
    '''Create a new table in postgresql for the layer'''
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
            cur.execute("create table _{} {}".format(output_table, new_column)) 
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
    '''Convert epoch time to postgresql timestamp without time zone'''    
    time = datetime.datetime.fromtimestamp(abs(input)/1000).strftime('%Y-%m-%d %H:%M:%S')
    return time

def get_data(mapserver, id, max_number = None, record_max = None):
    '''Get data from gcc view rest api'''        
    base_url = "https://insideto-gis.toronto.ca/arcgis/rest/services/{}/MapServer/{}/query".format(mapserver, id)
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
        rule = 'add'
    else:
        rule = 'dont add'  
    return rule   

def send_tempdata(output_table, insert_column, return_json):
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
    sql='INSERT INTO _{} {} VALUES %s'.format(output_table, insert_column)
    with con:
        with con.cursor() as cur:
               execute_values(cur,sql, rows)    
    print('sent')

def get_layer(mapserver_n, id):
    
    """
    This function calls to the GCCview rest API and inserts the outputs to the output table in the postgres database.

    Parameters
    ----------
    mapserver : int
        The name of the mapserver that host the desire layer

    id : int
        The id of desire layer
        
    """  
    mapserver = mapserver_name(mapserver_n)
    output_table = get_tablename(mapserver, id)
    rule = "add"
    counter = 0

    while rule == "add":
           
        if counter == 0:
            return_json = get_data(mapserver, id)
            insert_column = create_table(output_table, return_json)
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
    
  