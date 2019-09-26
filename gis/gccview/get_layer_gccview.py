import configparser
import requests
import datetime
from psycopg2 import connect
from psycopg2 import sql
from psycopg2.extras import execute_values
from time import sleep
import click
CONFIG = configparser.ConfigParser()
CONFIG.read(r'/home/natalie/airflow/db.cfg')
dbset = CONFIG['DBSETTINGS']
con = connect(**dbset)

def mapserver_name(mapserver_n):
    """Function to return the mapserver name from integer.
    
    Parameters
    ------------
    mapserver_n : numeric
        The number of mapserver we will be accessing. 0 for 'cot_geospatial'
    
    Returns
    --------
    name : string
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
    name = switcher.get(mapserver_n)
    return(name)

def get_tablename(mapserver, id):
    """
    Function to return the name of the layer.

    Parameter
    ----------
    mapserver : string
        The name of the mapserver we are accessing, returned from function mapserver_name

    id : numeric
        unique layer id that represent a single layer in the mapserver

    Returns
    --------
    output_name : string
        Table name for postgresql
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
            cur.execute("create table gis._{} {}".format(output_table, new_column))    
    # Add primary key
    with con:

        with con.cursor() as cur:
            
            cur.execute("alter table gis._{} add primary key ({})".format(output_table, primary_key)) 
        
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
        epoch time attribute in return_json

    Returns
    --------
    time : string
        time in the type of postgresql timestamp without time zone
    """  
    time = datetime.datetime.fromtimestamp(input/1000).strftime('%Y-%m-%d %H:%M:%S')
    return time

def get_data(mapserver, id, max_number = None, record_max = None):
    """Function to retreive layer data from GCCView rest api.

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
    """Function to check if last query return all rows

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
        geom = feature['geometry']
        geometry_type = return_json['geometryType']
        geometry = get_geometry(geometry_type, geom)
        row = [feature['attributes'][trial[0]] if trial[1] != 'esriFieldTypeDate' or feature['attributes'][trial[0]] == None else to_time(feature['attributes'][trial[0]]) for trial in trials]
        row.append(geometry)
        rows.append(row)
    
    sql='INSERT INTO gis._{} {} VALUES %s'.format(output_table, insert_column)
    with con:
        with con.cursor() as cur:
               execute_values(cur,sql, rows)    
    print('sent')

# Switcher function to get layer's info, the primary key of a layer and the column where upsert sql is going to look for differences

def ward():
    return['objectid', 'trans_id_create']
def centreline():
    return['geo_id', 'trans_id_create']
def bike():
    return['objectid', 'trans_id_create']
def traffic_camera():
    return['rec_id', 'geom']
def permit_parking():
    return['area_long_code', 'geom']
def tmms_service_request():
    return['object_id', 'geom']
def prai_transit_shelter():
    return['id', 'status']
def bylaw_pt():
    return['objectid', 'last_updated_date']
def bylaw_line():
    return['objectid', 'last_updated_date']
def school():
    return['objectid', 'geom']
def ev_charging_station():
    return['id', 'trans_id_create']
def day_care():
    return['objectid', 'geom']
def middle_child():
    return['objectid', 'geom']
def bia():
    return['objectid', 'geom']
def proposed_bia():
    return['objectid', 'geom']
def film_permit():
    return['objectid', 'lastupdated']
def film_parking():
    return['objectid', 'lastupdated']
def hotel():
    return['objectid', 'geom']
def convenience_store():
    return['objectid', 'geom']
def supermarket():
    return['objectid', 'geom']
def worship():
    return['objectid', 'geom']
def ymca():
    return['objectid', 'geom']
def census_tract():
    return['area_name', 'geom']
def neighbourhood_impro():
    return['area_short_code', 'trans_id_create']
def priority_neigh():
    return['area_short_code', 'geom']
def neigh_demo():
    return['area_short_code', 'total_pop']
def aborginal():
    return['objectid', 'geom']
def attraction():
    return['id', 'name']
def dropin():
    return['objectid', 'geom']
def early_year():
    return['id', 'geom']
def family_resource():
    return['objectid', 'geom']
def food_bank():
    return['objectid', 'geom']
def long_term_care():
    return['id', 'geom']
def parenting_family_lit():
    return['id', 'geom']
def retirement():
    return['id', 'geom']
def senior_housing():
    return['id', 'geom']
def shelter():
    return['objectid', 'geom']
def social_housing():
    return['objectid', 'geom']
def private_road():
    return['objectid', 'trans_id_create']
def library():
    return['id', 'geom']


def get_info(mapserver, id):
    server_info = str(mapserver)+'_'+str(id)
    return server_info

def info(server_info):
    switcher = {
            '0_0': ward,
            '0_2': centreline,
            '2_2': bike,
            '2_3': traffic_camera,
            '2_11': permit_parking,
            '2_35': prai_transit_shelter, 
            '2_37': tmms_service_request,
            '2_38': bylaw_pt,
            '2_39': bylaw_line,
            '20_1': ev_charging_station,
            '22_1': day_care,
            '22_2': middle_child,
            '23_1': bia,
            '23_13': proposed_bia,
            '23_9': film_permit,
            '23_10': film_parking,
            '23_12': hotel,
            '26_1': convenience_store,
            '26_4': supermarket,
            '26_3': worship,
            '26_6': ymca,
            '26_7': census_tract,
            '26_11': neighbourhood_impro,
            '26_13': priority_neigh,
            '26_16': neigh_demo,
            '26_45': aborginal,
            '26_46': attraction,
            '26_47': dropin, 
            '26_48': early_year,
            '26_49': family_resource,
            '26_50': food_bank,
            '26_53': long_term_care,
            '26_54': parenting_family_lit,
            '26_58': retirement,
            '26_59': senior_housing, 
            '26_61': shelter,
            '26_62': social_housing,
            '27_13': private_road,
            '28_17': school,
            '28_28': library
        
    }
    func = switcher.get(server_info)
    info1 = func()
    return info1

def find_diff(output_table,insert_column, primary_key, where_id):
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
    now = datetime.datetime.now()
    date = (str(now.year)+str(now.month)+str(now.day))
    
    insert_column1 = ((insert_column.replace('(', '')).replace(')', '')).split(",")
    except_column = '('
    for i in insert_column1:
            except_value = "EXCLUDED.{}".format(i)
            except_column = except_column + except_value +','        
   
    excluded_column = except_column[:-1]+')'


    # Find if old table exists
    with con:
        with con.cursor() as cur:
            cur.execute("select count(1) from information_schema.tables where table_schema = 'gis' and table_name = '{}'".format(output_table))
            result = cur.fetchone()
            # If table exists
            if result[0] == 1:
                # Delete rows that no longer exists in the new table
                with con:
                    with con.cursor() as cur:
                        cur.execute("delete from gis.{} where {} = (select {} from gis.{} except select {} from gis._{})".format(output_table, primary_key, primary_key, output_table, primary_key, output_table))

                # And then upsert stuff
                # if we are using geometry to compare
                if where_id == 'geom':
                    with con:
                        with con.cursor() as cur:
                            cur.execute("insert into gis.{} as a {} select {} from gis._{} on conflict({}) do update set {} = {} where ST_Equals(a.{},Excluded.{}) = False; comment on table gis.{} is 'last updated: {}'".format(output_table, insert_column, (insert_column[:-1])[1:], output_table, primary_key, insert_column,excluded_column,where_id, where_id, output_table, date)) 
                # if we are not using geometry to compare 
                else:
                    with con:
                        with con.cursor() as cur:
                            cur.execute("insert into gis.{} as a {} select {} from gis._{} on conflict({}) do update set {} = {} where a.{} <> Excluded.{}; comment on table gis.{} is 'last updated: {}'".format(output_table, insert_column, (insert_column[:-1])[1:], output_table, primary_key, insert_column,excluded_column,where_id, where_id, output_table, date)) 
                            
                # And then drop the temp table
                with con:
                    with con.cursor() as cur:
                         cur.execute("drop table gis._{}".format(output_table))
                print('Updated table')           
            # if table does not exists -> create a new one and add to audit list
            else: 
                with con:
                    with con.cursor() as cur:
                        cur.execute("alter table gis._{} rename to {}; comment on table gis.{} is 'last updated: {}'".format(output_table, output_table,output_table, date))
                with con:
                    with con.cursor() as cur:
                        cur.execute("select gis.audit_table('gis.{}')".format(output_table))
                        print('New table created and added to audit table list')

@click.command()
@click.option('--mapserver_n', help = 'Mapserver number, e.g. cotgeospatial_2 will be 2', type=int)
@click.option('--id', help = 'layer id', type=int)
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
    info_detail = info(get_info(mapserver_n,id))
    primary_key = info_detail[0]
    where_id = info_detail[1]
    rule = "add"
    counter = 0

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
    
    find_diff(output_table, insert_column, primary_key, where_id)

if __name__ == '__main__':
        get_layer() 
