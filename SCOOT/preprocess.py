# -*- coding: utf-8 -*-
"""
Created on Thu Feb 23 14:17:20 2017

@author: qwang2
"""
import pandas as pd

# (re)format Dataframe columns
def makestartdatetime(x):
    if x['Time_Start'] == '24:00':
        x['Time_Start'] = '00:00'
    return pd.to_datetime(str(x['Date']) + ' ' + x['Time_Start'])
def makeenddatetime(x):
    if x['Time_End'] == '24:00':
        x['Time_End'] = '00:00'
    return pd.to_datetime(str(x['Date']) + ' ' + x['Time_End'])
def makestopindex(x):
    corridor = [1147466,1147283,14255078,30020765,12347485,1147201,8491741,13973647,30082914,1147026]
    return corridor.index(x['centreline_id'])+0.5
    
    
def preprocess_scoot(sdata,sdetectors):
    # Merge for detector mapping
    sdata = sdata.merge(sdetectors,left_on='Site',right_on='det')
    # Convert to 15min actual volume detection
    sdata['flow_mean_veh/h'] = sdata['flow_mean_veh/h']//4
    # Take out weekends
    sdata = sdata[(sdata['DOW']!='SA') & (sdata['DOW']!='SU')]
    # Format datetime info
    sdata['time_15'] = sdata['Time_Start'].str[:2].astype(int)*4 + (sdata['Time_Start'].str[-2:].astype(int)//15)
    sdata['month'] = sdata['Date']//100-sdata['Date']//10000*100
    # Formulate stop index along the corridor
    sdata['stopindex'] = sdata.apply(makestopindex,axis=1)
    sdata['Date'] = pd.to_datetime(sdata['Date'].astype(str)).dt.date
    
    return sdata[['month','time_15','Date','centreline_id','stopindex','flow_mean_veh/h','direction']]
    
def preprocess_flow(fdata):   
    ''' This function formats data for plotting.
        Input dataframe requires columns:
        - count_bin: type datetime
        - centreline_id
        - volume: actual volume
        - dir_bin: direction
        Output dataframe has columns:
        - month, time_15, date
        - centreline_id, stopindex, dir_bin
        - volume'''
    
    fdata['DOW'] = fdata['count_bin'].dt.dayofweek   
    fdata['date'] = fdata['count_bin'].dt.date
    fdata['month'] = fdata['count_bin'].dt.month
    fdata['time_15'] = fdata['count_bin'].dt.hour*4+fdata['count_bin'].dt.minute//15
    fdata['stopindex'] = fdata.apply(makestopindex,axis=1)
    dirmap = {1:'EB', -1:'WB'}
    fdata['direction'] = fdata['dir_bin'].map(dirmap)
    # Filter out weekends
    fdata = fdata[(fdata['DOW']!=0) & (fdata['DOW']!=6)]
    fdata = fdata.loc[:,('month','time_15','date','centreline_id','stopindex','volume','dir_bin', 'direction')]
    return fdata
    
def get_start_end_month(start_year, end_year, start_month, end_month, year):
    ''' This function returns the start and end month given a year '''
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
    return (sm,em)
    
def fill_missing_values(index,s,start,end):
    ''' This function expands and fills in list s to a specified size (fill missing values with 0)
        index: index of each element in s
        s: list to be filled
        start, end: index of the returned list''' 
    ss = []
    i = start
    indexi = 0
    while i <= end:
        if indexi >= len(index):
            ss.append(0)
            i += 1
        elif index[indexi]>i:
            while i<index[indexi]:
                ss.append(0)
                i += 1
        else:
            ss.append(s[indexi])
            indexi += 1
            i += 1
    
    
    # fill in gaps
    i = start
    while i <= end:
        if (ss[i] is None or ss[i] == 0):
            j = i
            while (ss[j] is None or ss[j] == 0) and j < end:
                j += 1
            if i == 0:
                while i < j:
                    ss[i] = ss[j]
                    i += 1
            elif j == end:
                v = i-1
                while i <= j:
                    ss[i] = ss[v]
                    i += 1
            else:
                step = (ss[j]-ss[i-1])/(j-i+1)
                while i < j:
                    ss[i] = ss[i-1]+step
                    i += 1
        else:
            i += 1
    return ss
    
