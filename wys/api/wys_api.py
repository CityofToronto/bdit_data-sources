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



def get_signs():
    headers={'Content-Type':'application/json','x-api-key':api_key}
    #params = {'endTime': end_iteration_time, 'startTime' : start_time}
    response=session.get(url+signs_endpoint,
                         headers=headers, proxies=session.proxies)
    if response.status_code==200:
        signs=json.loads(response.content)
        return signs


def get_statistics():
    headers={'Content-Type':'application/json','x-api-key':api_key}
    #params = {'datetime': '2018-09-14 13:10:00',}
    response=session.get(url+statistics_endpoint,
                         headers=headers, proxies=session.proxies)
    if response.status_code==200:
        statistics=json.loads(response.content)
        return statistics
    else:
        return response.status_code

def get_settings():
    headers={'Content-Type':'application/json','x-api-key':api_key}
    #params = {'datetime': '2018-09-14 13:10:00',}
    response=session.get(url+settings_endpoint,
                         headers=headers, proxies=session.proxies)
    if response.status_code==200:
        settings=json.loads(response.content)
        return settings
    else:
        return response.status_code

session = Session()
session.proxies = {'https': 'https://137.15.73.132:8080'}
url='https://api.streetsoncloud.com/sv2'
signs_endpoint = '/signs'
statistics_endpoint='/signs/statistics/location/1845/period/5/speed_units/'
settings_endpoint='/signs/settings/location/1845/speed_units/0'
api_key=______
signs=get_signs()
statistics=get_statistics()
settings=get_settings()

