import json
import csv
from requests import Session
import datetime
import pytz
import dateutil.parser
from psycopg2 import connect


session = Session()
session.proxies = {'https': 'https://137.15.73.132:8080'}
url='https://api.miovision.com/intersections/'
tmc_endpoint = '/tmc'
ped_endpoint='/crosswalktmc'
time_delta = datetime.timedelta(days=1)

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

with open('time_range.txt', 'r') as f:
    t_rng=f.readlines()
    start_date=dateutil.parser.parse(t_rng[1])
    end_date=dateutil.parser.parse(t_rng[3])

start_time=start_date.astimezone(pytz.timezone('US/Eastern'))
end_time=end_date.astimezone(pytz.timezone('US/Eastern'))
with open('study_id_logger.csv', 'r') as study_id_logger:
    for row in reversed(list(csv.reader(study_id_logger))):
        study_id=int(row[1])+1
        break
start_id=study_id
id_counter=1

with open('api_key.txt','r') as api:
    api_key=api.read()
    
def get_intersection_tmc():
    headers={'Content-Type':'application/json','Authorization':api_key}
    params = {'endTime': end_iteration_time, 'startTime' : start_time}
    response=session.get(url+intersection_id1+tmc_endpoint, params=params, 
                         headers=headers, proxies=session.proxies)
    if response.status_code==200:
        tmc=json.loads(response.content)
        for item in tmc:
            item['study_id']=study_id
            item['study_name']=intersection_name
            item['lat']=lat
            item['lng']=lng
            item['classification']=item.pop('class')
            item['volume']=item.pop('qty')
            item['movement']=get_movement(item)
            item['entry_name']=None
            item['exit_name']=None
            item['entry_dir_name']=item.pop('entrance')
            item['exit_dir_name']=item.pop('exit')
        return tmc
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
            item['study_id']=study_id
            item['study_name']=intersection_name
            item['lat']=lat
            item['lng']=lng
            item['classification']=item.pop('class')
            item['volume']=item.pop('qty')
            temp=str(item['direction'])
            item.pop('direction', None)
            item['movement']=temp.lower()
            item['entry_name']=None
            item['exit_name']=None
            item['entry_dir_name']=item.pop('crosswalkSide')
            item['exit_dir_name']=None
        return ped
    else:
        return None 

while True:
    end_iteration_time= start_time + time_delta
    
    with open('intersection_id.csv', 'r') as int_id_file:
        intersection_id=csv.DictReader(int_id_file)
        for row in intersection_id:
            intersection_id1=str(row['id'])
            intersection_name=str(row['name'])
            print(intersection_name)
            lat=str(row['lat'])
            lng=str(row['lng'])
            if id_counter==1:
                intersection_tmc=get_intersection_tmc()
                ped=get_pedestrian()
                for item in ped:
                    intersection_tmc.append(item)
                study_id+=1
            else:
                temp_tmc=get_intersection_tmc()
                for item in temp_tmc:
                    intersection_tmc.append(item)
                ped=get_pedestrian()
                for item in ped:
                    intersection_tmc.append(item)
                study_id+=1
            id_counter+=1
    
    print(start_time)
    start_time+=time_delta
    if start_time==end_time:
        break

with open('intersection_tmc_'+str(datetime.date.today())+'.csv','w', newline='') as csvfile:
    fieldnames=['study_id', 'study_name', 'lat', 'lng', 'timestamp', 'classification', 'entry_dir_name', 'entry_name', 'exit_dir_name', 'exit_name', 'movement', 'volume']
    writer=csv.DictWriter(csvfile,fieldnames=fieldnames)
    writer.writeheader()
    for item in intersection_tmc:
        writer.writerow(item)

logger=[start_id, study_id,  start_date.strftime('%Y-%m-%d'),  end_date.strftime('%Y-%m-%d'), datetime.date.today()]

with open('study_id_logger.csv', 'a', newline='') as logger_csv:
    write=csv.writer(logger_csv)
    write.writerow(logger)
    
with open('sql_credentials.csv', 'r') as sql_credentials:
    conn_dict=csv.DictReader(sql_credentials)
    for row in conn_dict:
        conn_string="host='"+row['host']+"' dbname='"+row['dbname']+"' user='"+row['user']+"' password='"+row['password']+"'"
        conn=connect(conn_string)
        break
    
upload = '''DROP TABLE IF EXISTS rliu.raw_data;

CREATE TABLE rliu.raw_data
(
  study_id bigint,
  study_name text,
  lat numeric,
  lng numeric,
  datetime_bin timestamp with time zone,
  classification text,
  entry_dir_name text,
  entry_name text,
  exit_dir_name text,
  exit_name text,
  movement text,
  volume integer
)
WITH (
  OIDS=FALSE
);
ALTER TABLE rliu.raw_data
  OWNER TO rliu;
GRANT ALL ON TABLE rliu.raw_data TO rds_superuser WITH GRANT OPTION;
GRANT ALL ON TABLE rliu.raw_data TO dbadmin;
GRANT SELECT, REFERENCES, TRIGGER ON TABLE rliu.raw_data TO bdit_humans WITH GRANT OPTION;
GRANT ALL ON TABLE rliu.raw_data TO rliu;'''

cur = conn.cursor()
cur.execute(upload)
conn.commit()

with open('intersection_tmc_'+str(datetime.date.today())+'.csv', 'r') as csv:
    
    next(csv)  
    cur.copy_from(csv, 'rliu.raw_data', sep=',')
    
conn.commit()
