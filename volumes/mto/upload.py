# -*- coding: utf-8 -*-
"""
Created on Tue Jul 11 17:37:51 2017

@author: qwang2
"""

import ast

from io import StringIO

import logging

import pandas as pd

import requests
from requests.auth import HTTPBasicAuth

from utilities import db_connect
from utilities import get_sql_results

logger = logging.getLogger('upload_mto_data')
logger.setLevel(logging.INFO)
    
if not logger.handlers:
    handler = logging.FileHandler('upload_mto_data.log', mode='w')
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    handler.setFormatter(formatter)
    logger.addHandler(handler)

db = db_connect()
sensors = get_sql_results(db, "SELECT detector_id, ABS(lat)>1 FROM mto.sensors", ['detectorid','loc'])
auth = open('auth.txt','r').read()
auth = ast.literal_eval(auth)
baseurl = auth['baseurl']
auth = HTTPBasicAuth(auth['username'],auth['password'])

logger.info('Database connected.')

start_year = 2010
start_month = 11
end_year = 2016
end_month = 12

for year in range(start_year, end_year+1):
    
    if year == start_year and start_year != end_year:
        sm = start_month
        em = 13
    elif year == start_year and start_year == end_year:
        sm = start_month
        em = end_month+1
    elif year == end_year:
        sm = 1
        em = end_month+1
    else:
        sm = 1
        em = 13

    for month in range(sm,em):
        if month < 10:
            m = '0' + str(month)
        else:
            m = str(month)
        table = []
        for s,l in zip(sensors['detectorid'], sensors['loc']):
            params = {'year':year, 'month':month, 'reportType':'min_30', 'sensorName':s} 
            try:
                data = StringIO(requests.get(url=baseurl, params=params, auth=auth).text)
                if l:
                    data = pd.read_csv(data, skiprows=4, nrows=48, index_col=0)
                else:
                    data = pd.read_csv(data, skiprows=2, nrows=48, index_col=0)
            except OSError:
                print(s, 'not found')
                continue
            
            data = data.transpose()
            
            for dt in data.index:
                if dt != ' ':
                    for v,t in zip(data.loc[dt], data.columns):
                        table.append([s,dt + ' ' + t,v])
                      
        sql_trunc = 'TRUNCATE mto.mto_agg_30_placeholder_yearplaceholder_month'
        sql_create = 'CREATE TABLE IF NOT EXISTS mto.mto_agg_30_placeholder_yearplaceholder_month (CHECK (EXTRACT(MONTH FROM count_bin) = placeholder_month AND EXTRACT(YEAR FROM count_bin) = placeholder_year)) INHERITS (mto.mto_agg_30)'
        sql_crrule = 'CREATE OR REPLACE RULE mto_agg_30_placeholder_yearplaceholder_month AS ON INSERT TO mto.mto_agg_30 WHERE date_part(\'month\'::text, new.count_bin) = ' + m + ' AND date_part(\'year\'::text, new.count_bin) = placeholder_year DO INSTEAD INSERT INTO mto.mto_agg_30_placeholder_yearplaceholder_month (detector_id, count_bin, volume) VALUES (new.detector_id, new.count_bin, new.volume);'
        
        sql_trunc = sql_trunc.replace('placeholder_year',str(year))
        sql_trunc = sql_trunc.replace('placeholder_month',m)
        sql_create = sql_create.replace('placeholder_year',str(year))
        sql_create = sql_create.replace('placeholder_month',m)
        sql_crrule = sql_crrule.replace('placeholder_year',str(year))
        sql_crrule = sql_crrule.replace('placeholder_month',m)
        
        db.query(sql_create)
        db.query(sql_trunc)
        db.query(sql_crrule)
        db.inserttable('mto.mto_agg_30_'+str(year)+m, table)

        logger.info(str(year)+m+' Uploaded.')