# -*- coding: utf-8 -*-
"""
Created on Tue Oct 25 10:51:26 2016

@author: qwang2
"""

import requests
import pandas as pd
from pg import DB
import time
import datetime
import re
import configparser

CONFIG = configparser.ConfigParser()
CONFIG.read('db.cfg')
dbset = CONFIG['DBSETTINGS']

db = DB(dbname=dbset['database'],host=dbset['host'],user=dbset['user'],passwd=dbset['password'])
proxies = {'https':'https://137.15.73.132:8080'}

def FormatAddress(add):
    address = re.compile('[1-9][0-9]*\s.*\s(Ave|Rd|St|Cres|Pkwy|Place|Blvd|Dr|Lane|Way|Cir|Towers|Trail)(\.)?(\s[EWNS])?')    
    if type(add) is str:
        add = add.replace('Avenue', 'Ave')
        add = add.replace(' Av ', ' Ave ')
        add = add.replace('Road', 'Rd')
        add = add.replace('Street', 'St')
        add = add.replace('Crescent', 'Cres')
        add = add.replace('Parkway', 'Pkwy')
        add = add.replace('Drive', 'Dr')
        add = add.replace('Terrace', 'Terr')
        add = add.replace('Boulevard', 'Blvd')
        add = add.replace('Circle', 'Cir')
        add = add.replace('West', 'W')
        add = add.replace('East', 'E')
        add = add.replace('South', 'S')
        add = add.replace('North', 'N')
        add = add.replace(',', '')
        m = address.search(add)
        if m is not None:
            add = add[m.start():m.end()]
        add = add.replace('.', '')
        
# Update Venue List
venues = []
curId = db.query('SELECT max(id) FROM city.Venues') + 1
url = 'https://app.ticketmaster.com/discovery/v2/venues.json?size=499&stateCode=ON&countryCode=CA&includeTest=no&markets=102&city=Toronto&apikey=A3sEV24x7118ADXEEDhenqtDxmH3ijxg'
while (True): 
    r = requests.get(url,proxies = proxies).json()
    for l in r["_embedded"]["venues"]:
        if l["city"]["name"] == 'Toronto':
            exists = db.query('SELECT exists(SELECT true FROM city.Venues where tm_venue_id = '+l["id"]+')')
            venue = {}
            if "address" in l.keys():
                venue["venue_address"] = FormatAddress(l["address"]["line1"])
            else:
                venue["venue_address"] = None
            venue["tm_venue_id"] = l["id"]
            venue["venue_name"] = l["name"]
            venue["id"] = curId
            if not exists:
                curId = curId + 1
                db.insert('city.Venues', venue)
            venues.append(venue)
    if "next" in r["_links"].keys():
        url = 'https://app.ticketmaster.com'+r["_links"]["next"]["href"][:len(r["_links"]["next"]["href"])-7]+'&apikey=A3sEV24x7118ADXEEDhenqtDxmH3ijxg'
    else:
        break;
        
'''
# Get Events from List of Venues
events = {}
noEventVenue = {}
eventVenue = {}
data = []
i = 0
for venue in venues:
    i = i + 1
    key = venue["tm_venue_id"]
    r = requests.get('https://app.ticketmaster.com/discovery/v2/events.json?venueId='+key+'&apikey=A3sEV24x7118ADXEEDhenqtDxmH3ijxg',proxies = proxies).json();
    if "_embedded" in r.keys():
        eventVenue[key] = venues[key]
        for l in r["_embedded"]["events"]:
            event = {}
            event['tm_event_id'] = l["id"]
            try:
                for c in l["classifications"]:
                    if c["primary"]:
                        try:
                            event["classification"] = c["segment"]["name"]
                        except KeyError:
                            event["classification"] = None
            except KeyError:
                event["classification"] = None
            try:
                event["date"] = l["dates"]["start"]["localDate"]
            except KeyError:
                event["date"] = None
            try:
                event["name"] = l["name"]
            except KeyError:
                event["name"] = None
            try:
                t = time.strptime(l["dates"]["start"]["localTime"], "%H:%M:%S")
                event["start_time"] = datetime.time(t[3],t[4],t[5])
            except KeyError:
                event["start_time"] = None
            event["tm_venue_id"] = key
            events[l["id"]] = event
            db.upsert('city.TM_events',event)
    else:
        noEventVenue[key] = venues[key]
        
db.close()
'''
'''
#Exporting Events to csv
def getkey(x):
    for key in x["venue"].keys():
        return key
def getvalue(x):
    for value in x["venue"].values():
        return value

a = pd.DataFrame(events).transpose()
a["venue name"] = a.apply(getkey, axis = 1)
a["venue address"] = a.apply(getvalue, axis = 1)
del a["venue"]
a.to_csv('events.csv')
'''