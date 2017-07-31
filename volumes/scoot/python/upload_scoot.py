# -*- coding: utf-8 -*-
"""
Created on Thu Feb 16 14:20:40 2017

@author: qwang2
"""
import pandas as pd
from pg import DB
import configparser

CONFIG = configparser.ConfigParser()
CONFIG.read('db.cfg')
dbset = CONFIG['DBSETTINGS']

db = DB(dbname=dbset['database'],host=dbset['host'],user=dbset['user'],passwd=dbset['password'])

def makestartdatetime(x):
    if x['Time_Start'] == '24:00':
        x['Time_Start'] = '00:00'
    return str(pd.to_datetime(str(x['Date']) + ' ' + x['Time_Start'], format='%Y%m%d %H:%M'))
def makeenddatetime(x):    
    if x['Time_End'] == '24:00':
        x['Time_End'] = '00:00'
    return str(pd.to_datetime(str(x['Date']) + ' ' + x['Time_End'],format='%Y%m%d %H:%M'))

start_year = 2017
end_year = 2017
start_month = 3
end_month = 5

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
        print('Start Uploading...', year, month)
        if month < 10:
            m = '0' + str(month)
        else:
            m = str(month)
        sdata = pd.read_table('DETS_'+str(year)+m+'.txt', delim_whitespace=True, skiprows=9, header = None, 
                              names=['detector','dow','Date','Time_Start','Time_End','flow_mean','occ_mean','vehicle_occ_mean','lpu_factor_mean'])

        sdata['start_time'] = sdata.apply(makestartdatetime,axis=1)
        sdata['end_time'] = sdata.apply(makeenddatetime,axis=1)
        del(sdata['Time_Start'])
        del(sdata['Time_End'])
        del(sdata['dow'])
        del(sdata['Date'])
        sdata = sdata[['detector','start_time','end_time','flow_mean','occ_mean','vehicle_occ_mean','lpu_factor_mean']]
        sdata = sdata.values.tolist()
        print('Data Ready')
        r = 'CREATE OR REPLACE RULE scoot_'+str(year)+m+'_insert AS\
            ON INSERT TO scoot.scoot_agg_15_all\
            WHERE date_part(\'month\'::text, new.start_time) = 1::double precision AND date_part(\'year\'::text, new.start_time) = 2017::double precision DO INSTEAD  INSERT INTO scoot.agg_15_'+str(year)+m+' (detector, start_time, end_time, flow_mean, occ_mean, vehicle_occ_mean, lpu_factor_mean)\
            VALUES (new.detector, new.start_time, new.end_time, new.flow_mean, new.occ_mean, new.vehicle_occ_mean, new.lpu_factor_mean)'
        q = 'CREATE TABLE scoot.agg_15_'+str(year)+m+'(detector text, \
        start_time timestamp without time zone, end_time timestamp without time zone, \
        flow_mean int, occ_mean double precision, vehicle_occ_mean int, \
        LPU_factor_mean double precision, \
        CONSTRAINT c'+str(year)+m+' CHECK (date_part(\'month\'::text, start_time) = ' + m + '::double precision AND date_part(\'year\'::text, start_time) = ' + str(year) + '::double precision))'
        db.query(q)
        db.query(r)
        print('Table Created')
        db.inserttable('scoot.scoot_agg_15_all', sdata)
        print('Table Inserted')