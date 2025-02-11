# -*- coding: utf-8 -*-
"""
Created on Sun Nov  6 17:43:55 2016

@author: qwang2
"""

def caststartdate(x):
    return str(x['start_date'])
def castenddate(x):
    if x['end_date'] is None:
        return None
    else:
        return str(x['end_date'])
def caststarttime(x):
    if x['start_time'] is None:
        return None
    else:
        return str(x['start_time'])
def castendtime(x):
    if x['end_time'] is None:
        return None
    else:
        return str(x['end_time'])
    
import pandas as pd
from pg import DB
import configparser
from fuzzywuzzy import fuzz

CONFIG = configparser.ConfigParser()
CONFIG.read('db.cfg')
dbset = CONFIG['DBSETTINGS']

db = DB(dbname=dbset['database'],host=dbset['host'],user=dbset['user'],passwd=dbset['password'])

tm_events = pd.DataFrame(db.query('SELECT tm_event_id,classification,date,name,start_time,id FROM city.tm_events LEFT JOIN city.tm_venues USING tm_venue_id').getresult(), 
                         columns=['event_id','classification','start_date','event_name','start_time','venue_id'])
city_events = pd.DataFrame(db.query('SELECT id,event_name,start_date,start_time,end_date,end_time,classification,venue_id FROM city.opendata_events').getresult(),
                           columns = ['event_id','event_name','start_date','start_time','end_date','end_time','classification','venue_id' ])
tm_events.dropna(subset=['start_date'],inplace=True)
city_events.dropna(subset=['start_date'],inplace=True)
tm_events = tm_events.reset_index(drop=True)
city_events = city_events.reset_index(drop=True)

# process headings
tm_events['source'] = 0
city_events['source'] = 1
tm_events['end_date'] = None
tm_events['end_time'] = None

# check for duplicates and drop 
i = 0
j = 0
tm_drop = []
city_drop = []
'''
#too slow
for i in range(len(tm_events)):
    for j in range(len(city_events)):
        if tm_events.ix[i,'start_time'] is not None and city_events.ix[j,'start_time'] is not None:        
            if (tm_events.ix[i,'start_date'] == city_events.ix[j,'start_date'] and tm_events.ix[i,'start_time']==city_events.ix[j,'start_time'] and 
                tm_events.ix[i,'venue_id']==city_events.ix[j,'venue_id'] and fuzz.partial_ratio(tm_events.ix[i,'event_name'],city_events.ix[j,'event_name'])>90):
                if city_events.ix[j,'end_time'] is not None:
                    tm_drop.append(i)
                else:
                    city_drop.append(j)
        else:
            if (tm_events.ix[i,'start_date'] == city_events.ix[j,'start_date'] and tm_events.ix[i,'venue_id']==city_events.ix[j,'venue_id'] and 
                fuzz.partial_ratio(tm_events.ix[i,'event_name'],city_events.ix[j,'event_name'])>90):
                if city_events.ix[j,'end_time'] is not None:
                    tm_drop.append(i)
                else:
                    city_drop.append(j)
'''
print('Duplicate Events:')
for timeS1,timeE1,date1,venue1,name1 in zip(tm_events['start_time'], tm_events['end_time'], tm_events['start_date'], tm_events['venue_id'], tm_events['event_name']):    
    j = 0
    for timeS2,timeE2,dateS2,dateE2,venue2,name2 in zip(city_events['start_time'], city_events['end_time'], city_events['start_date'], city_events['end_date'], city_events['venue_id'], city_events['event_name']):       
        if timeS1 is not None and timeS2 is not None:
            if dateE2 is not None:
                if date1<=dateE2 and date1>=dateS2 and timeS1==timeS2 and venue1==venue2 and fuzz.partial_ratio(name1,name2)>90:
                    if timeE2 is not None:
                        print(name1,name2)
                        tm_drop.append(i)
                    else:
                        if j not in city_drop:
                            print(name1, name2)
                            city_drop.append(j)
            else:
                if date1==dateS2 and timeS1==timeS2 and venue1==venue2 and fuzz.partial_ratio(name1,name2)>90:
                    if timeE2 is not None:
                        print(name1,name2)
                        tm_drop.append(i)
                    else:
                        if j not in city_drop:
                            print(name1, name2)
                            city_drop.append(j)
        else:
            if dateE2 is not None:
                if date1<=dateE2 and date1>=dateS2 and venue1==venue2 and fuzz.partial_ratio(name1,name2)>90:
                    if timeE2 is not None:
                        print(name1,name2)
                        tm_drop.append(i)
                    else:
                        if j not in city_drop:
                            print(name1, name2)
                            city_drop.append(j)
            else:
                if date1==dateS2 and venue1==venue2 and fuzz.partial_ratio(name1,name2)>90:
                    if timeE2 is not None:
                        print(name1,name2)
                        tm_drop.append(i)
                    else:
                        if j not in city_drop:
                            print(name1, name2)
                            city_drop.append(j)
        j = j + 1
    i = i + 1
  
city_events = city_events.drop(city_drop)         
tm_events = tm_events.drop(tm_drop) 
events = pd.concat([city_events,tm_events])  
events.reset_index(inplace = True)
del events['index']

# restructure recurring events (construct a new table to store them)
num_group = 0
grouping = []
group_venue = []
group_id = []
for (vid,name) in zip(events['venue_id'],events['event_name']):
    i = 0
    match = 0
    maxmatch = 0
    group = 0
    for (vid1,name1) in zip(group_venue,grouping):
        match = fuzz.ratio(name,name1)
        if fuzz.partial_ratio(name,name1) > match and fuzz.ratio(name,name1) > 50:
            match = fuzz.partial_ratio(name,name1)
        if (match > maxmatch and vid == vid1):
            maxmatch = match
            group = i
        i = i + 1
    if maxmatch < 80:
        group_id.append(num_group)
        grouping.append(name)
        group_venue.append(vid)
        num_group = num_group + 1
    else:
        group_id.append(group)
        
events['group_id'] = group_id
grouped = events.groupby(['group_id'])

event_table = []
for (id),group in grouped:
    row = []
    row.append(id)
    row.append(group['event_name'].iloc[0])
    row.append(group['venue_id'].iloc[0])
    row.append(group['classification'].iloc[0])
    row.append(len(group))
    event_table.append(row)
    
del events['venue_id']
del events['classification']
del events['event_name']
events = events[['group_id', 'event_id', 'start_date','start_time','end_date','end_time','source']]
events['start_date'] = events.apply(caststartdate,axis = 1)
events['end_date'] = events.apply(castenddate,axis = 1)
events['start_time'] = events.apply(caststarttime,axis = 1)
events['end_time'] = events.apply(castendtime,axis = 1)
events = events.values.tolist()
db.truncate('city.event_groups')
db.truncate('city.event_details')
db.inserttable('city.event_groups', event_table)
db.inserttable('city.event_details', events)

db.close()