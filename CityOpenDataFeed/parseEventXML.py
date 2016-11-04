# -*- coding: utf-8 -*-
"""
Created on Thu Oct 27 10:12:48 2016

@author: qwang2
"""
import pandas as pd
import requests
import xml.etree.ElementTree as ET
from pg import DB
import datetime
import configparser

CONFIG = configparser.ConfigParser()
CONFIG.read('db.cfg')
dbset = CONFIG['DBSETTINGS']

db = DB(dbname=dbset['database'],host=dbset['host'],user=dbset['user'],passwd=dbset['password'])
proxies = {'http':'http://137.15.73.132:8080'}

r = requests.get('http://wx.toronto.ca/festevents.nsf/tpaview?readviewentries', proxies = proxies)

tree = ET.fromstring(r.content)
      
for entry in tree.findall('viewentry'):
    row = {}
    row["id"] = entry.attrib['noteid']
    row["event_name"] = entry.find("./entrydata[@name='EventName']/text").text
    
    if entry.find("./entrydata[@name='Location']/text") is not None:
        row["venue_name"] = entry.find("./entrydata[@name='Location']").find("./text").text
    else:
        loc=''
        for l in entry.findall("./entrydata[@name='CategoryList']/textlist/text"):
            loc = loc+l.text+' '
        row["venue_name"] = loc
        
    row["venue_address"] = entry.find("./entrydata[@name='Address']/text").text
    
    try:
        t = entry.find("./entrydata[@name='TimeBegin']/text").text
        if t.find('PM')>0:
            start_time = 12
        else:
            start_time = 0
        start_time = start_time + int(t[:t.find(':')])
        row["start_time"] = datetime.time(start_time,0,0)
    except:
        row["start_time"] = None
    
    try:
        t = entry.find("./entrydata[@name='TimeEnd']/text").text
        if t.find('PM')>0:
            end_time = 12
        else:
            end_time = 0
        end_time = end_time + int(t[:t.find(':')])
        row["end_time"] = datetime.time(end_time,0,0)
    except:
        row["end_time"] = None
        
    row["start_date"] = entry.find("./entrydata[@name='DateBeginShow']/text").text
    row["end_date"] = entry.find("./entrydata[@name='DateEndShow']/text").text
    
    if entry.find("./entrydata[@name='CategoryList']/text") is not None:
        row["classification"] = entry.find("./entrydata[@name='CategoryList']/text").text
    else:
        cat=''
        for c in entry.findall("./entrydata[@name='CategoryList']/textlist/text"):
            cat = cat+c.text+','
        row["classification"] = cat[:len(cat)-1]
        
    db.upsert('city.opendata_events',row)
    
db.close()
'''            
Events = pd.DataFrame({'Event':event, 'Venue':venue, 'Venue Address':venueAdd, 'Category':category, 'DateBegin':dateBegin, 'TimeBegin':timeBegin, 'DateEnd':dateEnd, 'TimeEnd':timeEnd})
Events.to_csv('city_open_data_events.csv')
'''