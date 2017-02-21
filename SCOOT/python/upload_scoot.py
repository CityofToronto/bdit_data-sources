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

for year in range(2015, 2017):
    if year == 2015:
        sm = 5
        em = 13
    elif year == 2017:
        sm = 1
        em = 2
    else:
        sm = 1
        em = 13

    for month in range(sm,em):
        print('Start Uploading...', year, month)
        if year==2012 and (month == 1 or month==2):
            continue
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
        q = 'CREATE TABLE scoot.raw_'+str(year)+m+'(detector text, \
        start_time timestamp without time zone, end_time timestamp without time zone, \
        flow_mean int, occ_mean double precision, vehicle_occ_mean int, \
        LPU_factor_mean double precision)'
        db.query(q)
        print('Table Created')
        db.inserttable('scoot.raw_'+str(year)+m, sdata)
        print('Table Inserted')