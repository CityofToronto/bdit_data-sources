# -*- coding: utf-8 -*-
"""
Created on Thu Oct 27 10:12:48 2016

@author: qwang2
"""
import pandas as pd
import requests
import xml.etree.ElementTree as ET

proxies = {'http':'http://137.15.73.132:8080'}

r = requests.get('http://wx.toronto.ca/festevents.nsf/tpaview?readviewentries', proxies = proxies)

event = []
venue = []
venueAdd = []
dateBegin = []
timeBegin = []
dateEnd = []
timeEnd = []
category = []
tree = ET.fromstring(r.content)
      
for entry in tree.findall('viewentry'):
    if entry.find("./entrydata[@name='Location']/text") is not None:
        venue.append(entry.find("./entrydata[@name='Location']").find("./text").text)
    else:
        loc=''
        for l in entry.findall("./entrydata[@name='CategoryList']/textlist/text"):
            loc = loc+l.text+' '
        venue.append(loc)
        
    event.append(entry.find("./entrydata[@name='EventName']").find("./text").text)
    venueAdd.append(entry.find("./entrydata[@name='Address']").find("./text").text)
    dateBegin.append(entry.find("./entrydata[@name='DateBeginShow']").find("./text").text)
    timeBegin.append(entry.find("./entrydata[@name='TimeBegin']").find("./text").text)
    dateEnd.append(entry.find("./entrydata[@name='DateEndShow']").find("./text").text)
    timeEnd.append(entry.find("./entrydata[@name='TimeEnd']").find("./text").text)
    
    if entry.find("./entrydata[@name='CategoryList']/text") is not None:
        category.append(entry.find("./entrydata[@name='CategoryList']/text").text)
    else:
        cat=''
        for c in entry.findall("./entrydata[@name='CategoryList']/textlist/text"):
            cat = cat+c.text+','
        category.append(cat[:len(cat)-1])
            
Events = pd.DataFrame({'Event':event, 'Venue':venue, 'Venue Address':venueAdd, 'Category':category, 'DateBegin':dateBegin, 'TimeBegin':timeBegin, 'DateEnd':dateEnd, 'TimeEnd':timeEnd})
Events.to_csv('city_open_data_events.csv')
