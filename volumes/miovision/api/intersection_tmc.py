import json
import csv
from requests import Session
import datetime
import pytz
import dateutil.parser
from psycopg2.extras import execute_values
from psycopg2 import connect

with open('sql_credentials.csv', 'r') as sql_credentials:
    conn_dict=csv.DictReader(sql_credentials)
    for row in conn_dict:
        conn_string="host='"+row['host']+"' dbname='"+row['dbname']+"' user='"+row['user']+"' password='"+row['password']+"'"
        conn=connect(conn_string)
        break
    
cur = conn.cursor()   
conn.autocommit = True
truncate_rawdata_new='''TRUNCATE rliu.raw_data_new;'''
cur.execute(truncate_rawdata_new)
aggregations='''INSERT INTO rliu.raw_data(study_id, study_name, lat, lng, datetime_bin, classification, entry_dir_name, entry_name, exit_dir_name, exit_name, movement, volume)
        SELECT * FROM rliu.raw_data_new;
        INSERT INTO rliu.volumes (intersection_uid, datetime_bin, classification_uid, leg, movement_uid, volume)
        SELECT B.intersection_uid, (A.datetime_bin AT TIME ZONE 'America/Toronto') AS datetime_bin, C.classification_uid, A.entry_dir_name as leg, D.movement_uid, A.volume
        FROM rliu.raw_data A
        INNER JOIN miovision.intersections B ON regexp_replace(A.study_name,'Yong\M','Yonge') = B.intersection_name
        INNER JOIN miovision.movements D USING (movement)
        INNER JOIN rliu.classifications C USING (classification)
        ORDER BY (A.datetime_bin AT TIME ZONE 'America/Toronto'), B.intersection_uid, C.classification_uid, A.entry_name, D.movement_uid;
        SELECT rliu.aggregate_15_min_tmc();
        SELECT rliu.aggregate_15_min();
        TRUNCATE rliu.raw_data_new;'''
        
def get_movement(item):
    if (item['entrance'] == 'N' and item['exit'] =='S'):
        return 'thru'
    elif item['entrance'] == 'S' and item['exit'] =='N':
        return 'thru'
    elif item['entrance'] == 'W' and item['exit'] =='E':
        return 'thru'
    elif item['entrance'] == 'E' and item['exit'] =='W':
        return 'thru'
    elif item['entrance'] == 'S' and item['exit'] =='W':
        return 'left'
    elif item['entrance'] == 'N' and item['exit'] =='E':
        return 'left'
    elif item['entrance'] == 'W' and item['exit'] =='N':
        return 'left'
    elif item['entrance'] == 'E' and item['exit'] =='S':
        return 'left'
    elif item['entrance'] == 'S' and item['exit'] =='E':
        return 'right'
    elif item['entrance'] == 'E' and item['exit'] =='N':
        return 'right'
    elif item['entrance'] == 'N' and item['exit'] =='W':
        return 'right'
    elif item['entrance'] == 'W' and item['exit'] =='S':
        return 'right'
    else:
        return 'u_turn'

def get_intersection_tmc():
    headers={'Content-Type':'application/json','Authorization':api_key}
    params = {'endTime': end_iteration_time, 'startTime' : start_time}
    response=session.get(url+intersection_id1+tmc_endpoint, params=params, 
                         headers=headers, proxies=session.proxies)
    if response.status_code==200:
        tmc=json.loads(response.content)
        for item in tmc:
            
            
            item['study_name']=intersection_name
            item['lat']=lat
            item['lng']=lng
            item['classification']=item.pop('class')
            item['volume']=item.pop('qty')
            item['movement']=get_movement(item)
            
            item['entry_dir_name']=item.pop('entrance')
            item['exit_dir_name']=item.pop('exit')
            table=[(intersection_name, lat, lng, item['timestamp'], item['classification'], item['entry_dir_name'],  item['exit_dir_name'],  item['movement'], item['volume'])]
            with conn:
                with conn.cursor() as cur:
                    execute_values(cur, 'INSERT INTO rliu.raw_data_new (study_name, lat, lng, datetime_bin, classification, entry_dir_name, exit_dir_name,  movement, volume) VALUES %s', table, template=None)
                    
        with conn:
            with conn.cursor() as cur:
                cur.execute(aggregations)
        return 1
    else:
        return None

def get_pedestrian():
    headers={'Content-Type':'application/json','Authorization':api_key}
    params = {'endTime': end_iteration_time, 'startTime' : start_time}
    response=session.get(url+intersection_id1+ped_endpoint, params=params, 
                         headers=headers, proxies=session.proxies)
    if response.status_code==200:
        ped=json.loads(response.content)
        for item in ped:
            
            item['study_name']=intersection_name
            item['lat']=lat
            item['lng']=lng
            item['classification']=item.pop('class')
            item['volume']=item.pop('qty')
            temp=str(item['direction'])
            item.pop('direction', None)
            item['movement']=temp.lower()
            item['entry_name']=0
            item['exit_name']=0
            item['entry_dir_name']=item.pop('crosswalkSide')
            item['exit_dir_name']=0
            table=[(intersection_name, lat, lng, item['timestamp'], item['classification'], item['entry_dir_name'],  item['movement'], item['volume'])]
            with conn:
                with conn.cursor() as cur:
                    execute_values(cur, 'INSERT INTO rliu.raw_data_new (study_name, lat, lng, datetime_bin, classification, entry_dir_name, movement, volume) VALUES %s', table, template=None)
        with conn:
            with conn.cursor() as cur:
                cur.execute(aggregations)
        return 1
    else:
        return None 
    
session = Session()
session.proxies = {'https': 'https://137.15.73.132:8080'}
url='https://api.miovision.com/intersections/'
tmc_endpoint = '/tmc'
ped_endpoint='/crosswalktmc'
time_delta = datetime.timedelta(days=1)

with open('time_range.txt', 'r') as f:
    t_rng=f.readlines()
    start_date=dateutil.parser.parse(t_rng[1])
    end_date=dateutil.parser.parse(t_rng[3])

start_time=start_date.astimezone(pytz.timezone('US/Eastern'))
end_time=end_date.astimezone(pytz.timezone('US/Eastern'))


with open('api_key.txt','r') as api:
    api_key=api.read()
    
while True:
    end_iteration_time= start_time + time_delta
    
    with open('intersection_id.csv', 'r') as int_id_file:
        intersection_id=csv.DictReader(int_id_file)
        for row in intersection_id:
            intersection_id1=str(row['id'])
            intersection_name=str(row['name'])
            lat=str(row['lat'])
            lng=str(row['lng'])
            get_intersection_tmc()
            get_pedestrian()
            print(intersection_name)
    print(start_time)
    start_time+=time_delta
    if start_time==end_time:
        break


with conn:
    with conn.cursor() as cur:
        report_dates='''SELECT rliu.report_dates();'''


        execute(report_dates)


with conn:
    with conn.cursor() as cur:        


        refresh_volumes_class='''REFRESH MATERIALIZED VIEW rliu.volumes_15min_by_class WITH DATA;'''


        execute(refresh_volumes_class)
with conn:
    with conn.cursor() as cur:        

        refresh_volumes='''REFRESH MATERIALIZED VIEW rliu.report_volumes_15min WITH DATA;'''


        execute(refresh_volumes)
with conn:
    with conn.cursor() as cur:        
        refresh_report_daily='''REFRESH MATERIALIZED VIEW rliu.report_daily WITH DATA;'''


        execute(refresh_report_daily)


print('Refreshed Views')
