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
start_month = 4
end_month = 4
sdata_prev = pd.DataFrame()

for year in range(end_year, start_year-1, -1):
    
    if year == start_year and start_year != end_year:
        sm = start_month - 1
        em = 12
    elif year == start_year and start_year == end_year:
        sm = start_month - 1
        em = end_month
    elif year == end_year:
        sm = 0
        em = end_month
    else:
        sm = 0
        em = 12

    for month in range(em, sm, -1):
        print('Start Uploading...', year, month)
        
        if month < 10:
            m = '0' + str(month)
        else:
            m = str(month)
            
        sdata = pd.read_table('DETS_'+str(year)+m+'.txt', delim_whitespace=True, skiprows=9, header = None, 
                              names=['detector','dow','Date','Time_Start','Time_End','flow_mean','occ_mean','vehicle_occ_mean','lpu_factor_mean'])

        sdata['start_time'] = sdata.apply(makestartdatetime,axis=1)
        sdata['end_time'] = sdata.apply(makeenddatetime,axis=1)
        sdata['month'] = sdata['Date']//100-sdata['Date']//10000*100
        del(sdata['Time_Start'])
        del(sdata['Time_End'])
        del(sdata['dow'])
        del(sdata['Date'])

        sdata = pd.concat([sdata, sdata_prev])
        
        sdata_prev = sdata[sdata['month'] != month]
        
        sdata = sdata[sdata['month'] == month]
        sdata = sdata[['detector','start_time','end_time','flow_mean','occ_mean','vehicle_occ_mean','lpu_factor_mean']]
        
        sdata = sdata.values.tolist()
        
        print('Data Ready')
        r = 'CREATE OR REPLACE RULE scoot_'+str(year)+m+'_insert AS\
            ON INSERT TO scoot.scoot_agg_15_all\
            WHERE date_part(\'month\'::text, new.start_time) = ' + m + '::double precision AND date_part(\'year\'::text, new.start_time) = ' + str(2017) + '::double precision DO INSTEAD  INSERT INTO scoot.agg_15_'+str(year)+m+' (detector, start_time, end_time, flow_mean, occ_mean, vehicle_occ_mean, lpu_factor_mean)\
            VALUES (new.detector, new.start_time, new.end_time, new.flow_mean, new.occ_mean, new.vehicle_occ_mean, new.lpu_factor_mean)'
        q = 'CREATE TABLE scoot.agg_15_'+str(year)+m+'(detector text, \
        start_time timestamp without time zone, end_time timestamp without time zone, \
        flow_mean int, occ_mean double precision, vehicle_occ_mean int, \
        LPU_factor_mean double precision, \
        CONSTRAINT c'+str(year)+m+' CHECK (date_part(\'month\'::text, start_time) = ' + m + '::double precision AND date_part(\'year\'::text, start_time) = ' + str(year) + '::double precision))'
        db.query(q)
        db.query(r)
        print('Table Created')
        db.inserttable('scoot.agg_15_'+str(year)+m, sdata)
        print('Table Inserted')
     
if !sdata_prev.empty:
    if month == 1:
        year = year - 1
        m = '12'
    else:
        if (month-1) < 10:
            m = '0' + str(month-1)
        else:
            m = str(month-1)
    sdata_prev = sdata_prev[['detector','start_time','end_time','flow_mean','occ_mean','vehicle_occ_mean','lpu_factor_mean']]
    db.inserttable('scoot.agg_15_'+str(year)+m, sdata_prev.values.tolist())