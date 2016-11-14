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
import configparser
from GenVenueList import geocode
from GenVenueList import FormatAddress

CONFIG = configparser.ConfigParser()
CONFIG.read('db.cfg')
dbset = CONFIG['DBSETTINGS']

db = DB(dbname=dbset['database'],host=dbset['host'],user=dbset['user'],passwd=dbset['password'])
proxies = {'https':'https://137.15.73.132:8080'}

# Update Venue List
venues = []
curId = db.query('SELECT max(id) FROM city.venues').getresult()[0][0]
url = 'https://app.ticketmaster.com/discovery/v2/venues.json?size=499&stateCode=ON&countryCode=CA&includeTest=no&markets=102&city=Toronto&apikey=A3sEV24x7118ADXEEDhenqtDxmH3ijxg'
while (True): 
    r = requests.get(url,proxies = proxies).json()
    for l in r["_embedded"]["venues"]:
        if l["city"]["name"] == 'Toronto':
            exists = db.query('SELECT exists(SELECT true FROM city.tm_venues where tm_venue_id = \''+l["id"]+'\')').getresult()[0][0]
            venue = {}
            if "address" in l.keys():
                venue["venue_address"] = FormatAddress(l["address"]["line1"])
            else:
                venue["venue_address"] = None
            venue["tm_venue_id"] = l["id"]
            venue["venue_name"] = l["name"]
            if exists == False:                
                curId = curId + 1
                venue["id"] = curId
                venue["venue_add_comp"] = venue["venue_address"]
                (add,lat,lon) = geocode(venue["venue_address"])
                venue["venue_address"] = add
                venue["lat"] = lat
                venue["lon"] = lon
                venue["capacity"] = None
                db.upsert('city.venues', venue)
                db.upsert('city.tm_venues', {"id":curId, "tm_venue_id":venue["tm_venue_id"]})
            venues.append(venue)
    if "next" in r["_links"].keys():
        url = 'https://app.ticketmaster.com'+r["_links"]["next"]["href"][:len(r["_links"]["next"]["href"])-7]+'&apikey=A3sEV24x7118ADXEEDhenqtDxmH3ijxg'
    else:
        break;
        
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