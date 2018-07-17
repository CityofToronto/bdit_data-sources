import sys
import json
import csv
from requests import Session
import datetime
import pytz
import dateutil.parser
from psycopg2.extras import execute_values
from psycopg2 import connect
import logging
import configparser


class Logger:
    def logger(self):
        if (logger.hasHandlers()):
            logger.handlers.clear()
        logger = logging.getLogger(__name__)
        logger.setLevel(logging.DEBUG)
        formatter=logging.Formatter('%(asctime)s     	%(levelname)s    %(message)s', datefmt='%d %b %Y %H:%M:%S')
        file_handler = logging.FileHandler('logging.log')
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)
        return logger
    with open('logging.log', 'w'):
            pass
logger.debug('Start')


CONFIG = configparser.ConfigParser()
CONFIG.read(r'config.cfg')
api_key=CONFIG['API']
key=api_key['key']
dbset = CONFIG['DBSETTINGS']
conn = connect(**dbset)

logger.debug('Connected to DB')

cur = conn.cursor()   
#conn.autocommit = True
with conn:
    with conn.cursor() as cur:
        try:
             truncate_rawdata_new='''TRUNCATE rliu.raw_data_new;'''
             cur.execute(truncate_rawdata_new)
        except:
            logger.exception('SQL query and/or connection error')
            sys.exit()
        
        
insert='''INSERT INTO rliu.raw_data(study_id, study_name, lat, lng, datetime_bin, classification, entry_dir_name, entry_name, exit_dir_name, exit_name, movement, volume)
        SELECT * FROM rliu.raw_data_new;
        INSERT INTO rliu.volumes (intersection_uid, datetime_bin, classification_uid, leg, movement_uid, volume)
        SELECT B.intersection_uid, (A.datetime_bin AT TIME ZONE 'America/Toronto') AS datetime_bin, C.classification_uid, A.entry_dir_name as leg, D.movement_uid, A.volume
        FROM rliu.raw_data_new A
        INNER JOIN miovision.intersections B ON regexp_replace(A.study_name,'Yong\M','Yonge') = B.intersection_name
        INNER JOIN miovision.movements D USING (movement)
        INNER JOIN rliu.classifications C USING (classification)
        ORDER BY (A.datetime_bin AT TIME ZONE 'America/Toronto'), B.intersection_uid, C.classification_uid, A.entry_name, D.movement_uid;'''

        
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

def get_intersection_tmc(table):
    headers={'Content-Type':'application/json','Authorization':key}
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
            temp=[intersection_name, lat, lng, item['timestamp'], item['classification'], item['entry_dir_name'],  item['exit_dir_name'],  item['movement'], item['volume']]
            table.append(temp)

        return table
    else:
        logger.exception('Could not pull intersection from API')
        
        return None

def get_pedestrian(table):
    headers={'Content-Type':'application/json','Authorization':key}
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
            item['entry_dir_name']=item.pop('crosswalkSide')
            item['exit_dir_name']=None
            temp=[intersection_name, lat, lng, item['timestamp'], item['classification'], item['entry_dir_name'],  item['exit_dir_name'],  item['movement'], item['volume']]
            table.append(temp)
            
        return table
    else:
        logger.exception('Could not pull intersection from API')
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
    table=[]
    with open('intersection_id.csv', 'r') as int_id_file:
        intersection_id=csv.DictReader(int_id_file)
        for row in intersection_id:
            intersection_id1=str(row['id'])
            intersection_name=str(row['name'])
            lat=str(row['lat'])
            lng=str(row['lng'])
            logger.info(intersection_name)
            try:
                table=get_intersection_tmc(table)
            except:
                logger.exception('Could not connect to Api')
                sys.exit()
            try:
                table=get_pedestrian(table)
            except:
                logger.exception('Could not connect to Api')
                sys.exit()
      
            
    logger.info('Completed data pulling for {}'.format(start_time))
    try:
        with conn:
            with conn.cursor() as cur:
                execute_values(cur, 'INSERT INTO rliu.raw_data_new (study_name, lat, lng, datetime_bin, classification, entry_dir_name, exit_dir_name,  movement, volume) VALUES %s', table)
                conn.commit
        with conn:
            with conn.cursor() as cur:             
                cur.execute(insert)
                conn.commit
                
        with conn:
            with conn.cursor() as cur:             
                cur.execute('''SELECT rliu.aggregate_15_min_tmc();''')
                conn.commit
        with conn:
            with conn.cursor() as cur:             
                cur.execute('''SELECT rliu.aggregate_15_min(); TRUNCATE rliu.raw_data_new;''')
                conn.commit
        logger.info('Completed data processing for {}'.format(start_time))
    except:
        logger.exception('Data Processing and/or connection error')
        sys.exit()
    start_time+=time_delta
    if start_time==end_time:
        break


try:
    with conn:
        with conn.cursor() as cur:
            report_dates='''SELECT rliu.report_dates();'''
            cur.execute(report_dates)
            refresh_volumes_class='''REFRESH MATERIALIZED VIEW rliu.volumes_15min_by_class WITH DATA;'''
            cur.execute(refresh_volumes_class)
            refresh_volumes='''REFRESH MATERIALIZED VIEW rliu.report_volumes_15min WITH DATA;'''
            cur.execute(refresh_volumes)
            refresh_report_daily='''REFRESH MATERIALIZED VIEW rliu.report_daily WITH DATA;'''
            cur.execute(refresh_report_daily)
    logger.info('Updated Views')
    logger.info('Done')
except:
    logger.exception('Cannot Refresh Views')
    sys.exit()
    
