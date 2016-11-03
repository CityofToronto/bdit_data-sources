# -*- coding: utf-8 -*-
"""
Created on Tue Oct 25 10:51:26 2016

@author: qwang2
"""

import requests
import pandas as pd

proxies = {'https':'https://137.15.73.132:8080'}


# Get Venues List in Toronto
venues = {}
url = 'https://app.ticketmaster.com/discovery/v2/venues.json?size=499&stateCode=ON&countryCode=CA&includeTest=no&markets=102&city=Toronto&apikey=A3sEV24x7118ADXEEDhenqtDxmH3ijxg'
while (True): 
    r = requests.get(url,proxies = proxies).json()
    for list in r["_embedded"]["venues"]:
        if list["city"]["name"] == 'Toronto':
            venue = {}
            if "address" in list.keys():
                venue[list["name"]] = list["address"]["line1"]
            else:
                venue[list["name"]] = ""
            venues[list["id"]] = venue
    if "next" in r["_links"].keys():
        url = 'https://app.ticketmaster.com'+r["_links"]["next"]["href"][:len(r["_links"]["next"]["href"])-7]+'&apikey=A3sEV24x7118ADXEEDhenqtDxmH3ijxg'
    else:
        break;

# Get Events from List of Venues
events = {}
noEventVenue = {}
eventVenue = {}

i = 0
for key in venues:
    i = i + 1
    r = requests.get('https://app.ticketmaster.com/discovery/v2/events.json?venueId='+key+'&apikey=A3sEV24x7118ADXEEDhenqtDxmH3ijxg',proxies = proxies).json();
    if "_embedded" in r.keys():
        eventVenue[key] = venues[key]
        for l in r["_embedded"]["events"]:
            event = {}
            try:
                event["name"] = l["name"]
            except KeyError:
                event["name"] = ""
            try:
                event["date"] = l["dates"]["start"]["localDate"]
            except KeyError:
                event["date"] = ""
            try:
                event["time"] = l["dates"]["start"]["localTime"]
            except KeyError:
                event["time"] = ""
            try:
                for c in l["classifications"]:
                    if c["primary"]:
                        try:
                            event["classification"] = c["segment"]["name"]
                        except KeyError:
                            event["classification"] = ""
                        try:
                            event["genre"] = c["genre"]["name"]
                        except KeyError:
                            event["genre"] = ""
            except KeyError:
                event["classification"] = ""
                event["genre"] = ""
            event["venue"] = venues[key]
            event["tm_venue_id"] = key
            events[l["id"]] = event
    else:
        noEventVenue[key] = venues[key]

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
