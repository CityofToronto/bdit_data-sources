# -*- coding: utf-8 -*-
"""
Created on Wed Oct 17 15:26:52 2018

@author: rliu4
"""


import json
import csv
from requests import Session
import datetime
import configparser
from psycopg2 import connect
from psycopg2.extras import execute_values
import dateutil.parser

def roundTime(dt=None, roundTo=60):
   """function to round to nearest 5 minutes
   Author: Thierry Husson 2012 - Use it as you want but don't blame me.
   """
   seconds = (dt.replace(tzinfo=None) - dt.min).seconds
   rounding = (seconds+roundTo/2) // roundTo * roundTo
   return dt + datetime.timedelta(0,rounding-seconds,-dt.microsecond)

def get_signs():
    headers={'Content-Type':'application/json','x-api-key':api_key}
    #params = {'endTime': end_iteration_time, 'startTime' : start_time}
    response=session.get(url+signs_endpoint,
                         headers=headers, proxies=session.proxies)
    if response.status_code==200:
        signs=response.json()
        return signs


def get_statistics(location):
    headers={'Content-Type':'application/json','x-api-key':api_key}
    response=session.get(url+statistics_endpoint+str(location)+end, 
                         headers=headers, proxies=session.proxies)
    if response.status_code==200:
        statistics=response.json()
        return statistics
    else:
        return response.status_code



session = Session()
session.proxies = {'https': 'https://137.15.73.132:8080'}
url='https://api.streetsoncloud.com/sv2'
signs_endpoint = '/signs'
statistics_endpoint='/signs/statistics/location/'
end='/period/1443/speed_units/0'
CONFIG = configparser.ConfigParser()
CONFIG.read('config.cfg')
key=CONFIG['API']
api_key=key['key']
dbset = CONFIG['DBSETTINGS']
conn = connect(**dbset)

table=[]

with conn:
    with conn.cursor() as cur:
        string="SELECT * from wys.locations"
        cur.execute(str(string))
        signs_list=cur.fetchall()
        signs_iterator=signs_list
        conn.commit()
for signs_iterator in signs_iterator:
    location=signs_iterator[1]
    #address=item['address']
    statistics=get_statistics(location)
    raw_data=statistics['LocInfo']
    raw_records=raw_data['raw_records']
    for item in raw_records:
        datetime_bin=item['datetime']
        datetime_bin= dateutil.parser.parse(str(datetime_bin))
        datetime_bin=roundTime(datetime_bin,roundTo=5*60)
        counter=item['counter']
        for item in counter:
            temp=[location, datetime_bin, item['speed'], item['count']]
            table.append(temp)   
    signs_iterator=signs_list
    
    
with conn.cursor() as cur:
    execute_values(cur, 'INSERT INTO wys.raw_data (api_id, datetime_bin, speed, count) VALUES %s', table)
    conn.commit()



#with open('wys_api_'+str(datetime.date.today())+'.csv','w', newline='') as csvfile:
#    fieldnames=['location_id', 'datetime_bin', 'speed', 'count']
#    writer=csv.writer(csvfile, delimiter=',')
#    writer.writerow(fieldnames)
#    for item in table:
#        writer.writerow(item)
#
