# -*- coding: utf-8 -*-
"""
Created on Fri Jun  9 14:55:38 2017

@author: qwang2
"""
import pandas as pd
from pg import DB
import configparser
from datetime import timedelta, date
import datetime
def daterange(start_date, end_date):
    for n in range(int ((end_date - start_date).days)+1):
        yield start_date + timedelta(n)
        

CONFIG = configparser.ConfigParser()
CONFIG.read('db.cfg')
dbset = CONFIG['DBSETTINGS']

db = DB(dbname=dbset['database'],host=dbset['host'],user=dbset['user'],passwd=dbset['password'])

def subtract_ct(x): # Subtract Cycle Time 
    return (datetime.datetime.combine(x['Date'], x['Time']) - timedelta(seconds = int(x['CycleTime']))).time()
    
# ---------------TO BE MODIFIED----------------
start_date = date(2017, 4, 21)
end_date = date(2017, 4, 22)
datapath = "L:/BIG DATA/SCOOT Data/RAW/"
# --------------------END----------------------

ruled = []
for load_date in daterange(start_date, end_date):
    print('Loading ' + load_date.strftime("%Y-%m-%d"))
    sdate = load_date.strftime("%Y%m%d")
    
    # Read in Scoot Data
    scoot = pd.DataFrame()
    for hour in range(0, 24):
        if hour<10:
            h = '0'+str(hour)
        else:
            h = str(hour)
        s1 = pd.read_table(datapath+sdate+"RAW/M29_"+sdate+"H"+h+"R01.txt",delim_whitespace=True,error_bad_lines=False,skiprows=1,header=None, 
                          names=['Date','Time','M29','detector','CycleTime','Volume','Occupancy'])
        s2 = pd.read_table(datapath+sdate+"RAW/M29_"+sdate+"H"+h+"R02.txt",delim_whitespace=True,error_bad_lines=False,skiprows=1,header=None, 
                          names=['Date','Time','M29','detector','CycleTime','Volume','Occupancy'])
        s3 = pd.read_table(datapath+sdate+"RAW/M29_"+sdate+"H"+h+"R03.txt",delim_whitespace=True,error_bad_lines=False,skiprows=1,header=None, 
                          names=['Date','Time','M29','detector','CycleTime','Volume','Occupancy'])
        scoot = pd.concat([scoot,s1,s2,s3])                     
    
    scoot = scoot[scoot['Volume'] != 'Cell']
    scoot['Volume'] = scoot['Volume'].astype(int)
    scoot['Occupancy'] = scoot['Occupancy'].astype(int)
    scoot['seconds'] = (pd.to_datetime(scoot['Date']+' '+scoot['Time'])-pd.to_datetime(sdate)).dt.total_seconds().astype(int)
    scoot['Date'] = pd.to_datetime(scoot['Date']).dt.date
    scoot['Time'] = pd.to_datetime(scoot['Time']).dt.time
    
    # Shift all count time by a cycle time to represent the start of the signal(count) cycle
    scoot['seconds'] = scoot['seconds'] - scoot['CycleTime']
    scoot = scoot[scoot['seconds'] >= 0]
    scoot['Time'] = scoot.apply(subtract_ct, axis=1) 
    
    # Sort based on Time (Data is sorted most of the time, occasional mess)
    scoot = scoot.sort_values(['seconds'])
        
    del scoot['M29']

    scoot['Date'] = scoot['Date'].apply(lambda x: x.strftime("%Y-%m-%d"))
    scoot['Time'] = scoot['Time'].apply(lambda x: x.strftime("%H:%M:%S"))
    scoot = scoot.values.tolist()
    
    # Create partition rules if not inplace (Will replace if this month was loaded before)
    if (load_date.year, load_date.month) not in ruled:
        ruled.append((load_date.year, load_date.month))
        r = 'CREATE OR REPLACE RULE cycle_level_' + load_date.strftime("%Y%m") + '_insert AS \
            ON INSERT TO scoot.cyclelevel_' + load_date.strftime("%Y%m") + \
            ' WHERE date_part(\'month\'::text, new.count_time) = ' + str(load_date.month) + '::double precision  \
            AND date_part(\'year\'::text, new.count_time) = ' + str(load_date.year) + '::double precision \
            DO INSTEAD INSERT INTO scoot.cyclelevel_' + load_date.strftime("%Y%m") + '(count_date, count_time, detector, cycle_time, flow, occupancy, seconds) \
            VALUES (new.count_date, new.count_time, new.detector, new.cycle_time, new.flow, new.occupancy, new.seconds)'
        db.query(r)
    db.inserttable('scoot.cyclelevel_' + load_date.strftime("%Y%m"), scoot)
    
    print('Finished ' + load_date.strftime("%Y-%m-%d"))
    