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
from AddressFunctions import geocode
from AddressFunctions import FormatAddress
from fuzzywuzzy import fuzz

CONFIG = configparser.ConfigParser()
CONFIG.read('db.cfg')
dbset = CONFIG['DBSETTINGS']

db = DB(dbname=dbset['database'],host=dbset['host'],user=dbset['user'],passwd=dbset['password'])
proxies = {'http':'http://137.15.73.132:8080'}

r = requests.get('http://app.toronto.ca/cc_sr_v1_app/data/edc_eventcal_APR', proxies = proxies)

tree = r.json()

curId = db.query('SELECT max(id) FROM city.venues').getresult()[0][0]
ODID = db.query('SELECT max(od_id) FROM city.od_venues').getresult()[0][0]

for entry0 in tree:
    entry = entry0['calEvent']
    # Extract Information
    row = {}
    row["id"] = entry['recId']
    row["event_name"] = entry['eventName']
    
    #if entry['locations'][0]['locationName'] is not None:
    row["venue_name"] = entry['locations'][0]['locationName'].replace("\'", "")
    
    row["venue_address"] = row["venue_add_comp"] = entry['locations'][0]['address']
    row["start_date"] = datetime.datetime.strptime(entry['startDate'],'%Y-%m-%dT%H:%M:%S.%fZ').replace(tzinfo=datetime.timezone.utc).astimezone(tz=None).date()
    row["end_date"] = datetime.datetime.strptime(entry['endDate'],'%Y-%m-%dT%H:%M:%S.%fZ').replace(tzinfo=datetime.timezone.utc).astimezone(tz=None).date()
    row["start_time"] = datetime.datetime.strptime(entry['startDate'],'%Y-%m-%dT%H:%M:%S.%fZ').replace(tzinfo=datetime.timezone.utc).astimezone(tz=None).time()
    row["end_time"] = datetime.datetime.strptime(entry['endDate'],'%Y-%m-%dT%H:%M:%S.%fZ').replace(tzinfo=datetime.timezone.utc).astimezone(tz=None).time()
    
    cat=''
    for c in entry['category']:
        cat = cat+c['name']+','
    row["classification"] = cat[:len(cat)-1]
    
    # Update Venues Table    
    exist = db.query("SELECT * FROM city.venues where venue_add_comp = \'"+row["venue_add_comp"]+"\'").getresult()
    if exist == []:
        names = db.query("SELECT venue_name FROM city.venues").getresult()
        for name in names :
            if fuzz.ratio(name[0], row["venue_name"]) > 80 or fuzz.ratio(name[0], row["venue_name"]) > 60 and fuzz.partial_ratio(name[0], row["venue_name"]) > 90:
                exist = db.query("SELECT * FROM city.venues where venue_name = \'"+name[0]+"\'").getresult()

    venue = {}
    if exist == []:
        #insert
        curId = curId + 1
        venue["id"] = curId
        venue["venue_name"] = row["venue_name"]
        venue["venue_add_comp"] = row["venue_add_comp"]
        
        if row["venue_add_comp"] is not None and not entry['locations'][0]['geoCoded']:
            (add,lat,lon) = geocode(FormatAddress(row["venue_add_comp"]))
        elif entry['locations'][0]['geoCoded']:
            add = venue["venue_add_comp"]
            lat = entry['locations'][0]['coords']['lat']
            lon = entry['locations'][0]['coords']['lng']
        else:
            add = None
            lat = None
            lon = None
        
        venue["venue_address"] = add
        venue["lat"] = lat
        venue["lon"] = lon
        db.insert('city.venues', venue)
        print('INSERT VENUE', row["venue_name"])
        row["venue_id"] = curId
    elif None in exist[0][0:4]:
        #update
        venue["id"] = exist[0][2]
        row["venue_id"] = exist[0][2]
        venue["venue_name"] = row["venue_name"]
        venue["venue_add_comp"] = row["venue_add_comp"]
        if row["venue_add_comp"] is not None and not entry['locations'][0]['geoCoded']:
            (add,lat,lon) = geocode(FormatAddress(row["venue_add_comp"]))
        elif entry['locations'][0]['geoCoded']:
            add = venue["venue_add_comp"]
            lat = entry['locations'][0]['coords']['lat']
            lon = entry['locations'][0]['coords']['lng']
        else:
            add = None
            lat = None
            lon = None
        venue["venue_address"] = add
        venue["lat"] = lat
        venue["lon"] = lon
        db.upsert('city.venues', venue)
        print('UPSERT VENUE', row["venue_name"])

    else:
        # do nothing
        row["venue_id"] = exist[0][2]
        
   
   # Update ODVenues Table
    venue = {}
    exist = db.query("SELECT * FROM city.od_venues where venue_address = \'"+row["venue_add_comp"]+"\'").getresult()
    if exist == []:
        exist = db.query("SELECT * FROM city.od_venues where venue_name = \'"+row["venue_name"]+"\'").getresult()
    if exist == []:
        ODID = ODID + 1
        venue["venue_address"] = row["venue_add_comp"]
        venue["venue_name"] = row["venue_name"]
        venue["od_id"] = ODID
        venue["id"] = row["venue_id"]
        db.insert('city.od_venues',venue)
        row["od_venue_id"] = ODID
    else:
        row["od_venue_id"] = exist[0][0]

    db.upsert('city.od_events',row)
     

db.close()

'''            
Events = pd.DataFrame({'Event':event, 'Venue':venue, 'Venue Address':venueAdd, 'Category':category, 'DateBegin':dateBegin, 'TimeBegin':timeBegin, 'DateEnd':dateEnd, 'TimeEnd':timeEnd})
Events.to_csv('city_open_data_events.csv')
'''