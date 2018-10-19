# -*- coding: utf-8 -*-
"""
Created on Wed Oct 17 15:26:52 2018

@author: rliu4
"""


import json
import csv
from requests import Session
import datetime
import pytz
import dateutil.parser
import configparser


def get_signs():
    headers={'Content-Type':'application/json','x-api-key':api_key}
    #params = {'endTime': end_iteration_time, 'startTime' : start_time}
    response=session.get(url+signs_endpoint,
                         headers=headers, proxies=session.proxies)
    if response.status_code==200:
        signs=json.loads(response.content)
        return signs


def get_statistics(location):
    headers={'Content-Type':'application/json','x-api-key':api_key}
    response=session.get(url+statistics_endpoint+str(location)+end, 
                         headers=headers, proxies=session.proxies)
    if response.status_code==200:
        statistics=json.loads(response.content)
        return statistics
    else:
        return response.status_code



session = Session()
session.proxies = {'https': 'https://137.15.73.132:8080'}
url='https://api.streetsoncloud.com/sv2'
signs_endpoint = '/signs'
statistics_endpoint='/signs/statistics/location/'
end='/period/1440/speed_units/0'
settings_endpoint='/signs/settings/location/1845/speed_units/0'
CONFIG = configparser.ConfigParser()
CONFIG.read('config.cfg')
key=CONFIG['API']
api_key=key['key']
signs=get_signs()
table=[]
#for item in signs:
location='7562'
#location=item['location_id']
#address=item['address']
statistics=get_statistics(location)
raw_data=statistics['LocInfo']
raw_records=raw_data['raw_records']
for item in raw_records:
    datetime_bin=item['datetime']
    for item in item['counter']:
        temp=[location, datetime_bin, item['speed'], item['count']]
        table.append(temp)


with open('wys_api_'+str(datetime.date.today())+'.csv','w', newline='') as csvfile:
    fieldnames=['location_id', 'datetime_bin', 'speed', 'count']
    writer=csv.writer(csvfile, delimiter=',')
    writer.writerow(fieldnames)
    for item in table:
        writer.writerow(item)

