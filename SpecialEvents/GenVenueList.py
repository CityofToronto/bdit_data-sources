# -*- coding: utf-8 -*-
"""
Created on Fri Oct 28 15:30:21 2016

@author: qwang2
"""

import pandas as pd
import re
import requests

if __name__ == "__main__":
    TMvenues = pd.read_csv('tm_venues.csv',encoding = 'latin-1')
    Cityevents = pd.read_csv('city_open_data_events.csv',encoding = 'latin-1')
    proxies = {'https':'https://137.15.73.132:8080'}
    
    # Clean up address format
    newadd = []
    for add in TMvenues['venue_address']:
        if type(add) is str:
            add = FormatAddress(add)      
            newadd.append(add)
        else:
            newadd.append(None)
    TMvenues['venue_address'] = newadd
    
    newadd = []
    for add in Cityevents['venue_address']:
        if type(add) is str:
            add = FormatAddress(add)
            newadd.append(add)
        else:
            newadd.append(None)
    Cityevents['venue_address'] = newadd
    
    # Check TM Venue Address Duplicates
    addMap = dict(zip(TMvenues['venue_address'].tolist(), TMvenues['id'].tolist()))
    newID = []
    for (Id,add) in zip(TMvenues['id'], TMvenues['venue_address']):
        newID.append(addMap[add])
    TMvenues['id'] = newID
    
    # Check TM Venue Names Duplicates
    nameMap = dict(zip(TMvenues['venue_name'].tolist(), TMvenues['id'].tolist()))
    newID = []
    for (Id,name) in zip(TMvenues['id'], TMvenues['venue_name']):
        newID.append(nameMap[name])
    TMvenues['id'] = newID
    
    # Add id to venues in the city list
    newID = []
    curId = max(TMvenues['id'])
    for (venueAdd,venueName) in zip(Cityevents['venue_address'],Cityevents['venue_name']):
        if venueAdd in addMap.keys():
            newID.append(addMap[venueAdd])
        elif venueName in nameMap.keys():
            newID.append(nameMap[venueName])
        else:
            curId = curId + 1
            newID.append(curId)
            addMap[venueAdd] = curId  
            nameMap[venueName] = curId
    Cityevents['id'] = newID                                                                                        
    
    # Combine the two venue lists
    Cityevents['tm_venue_id'] = None
    completeVenueList = pd.concat([TMvenues[['venue_name', 'venue_address', 'id', 'tm_venue_id']], 
                                   Cityevents[['venue_name', 'venue_address', 'id', 'tm_venue_id']]]).drop_duplicates(['id'])    
    tm_venues = pd.read_csv('tm_venues.csv', encoding = 'latin-1')
    completeVenueList = completeVenueList.merge(tm_venues[['tm_venue_id', 'capacity']], on='tm_venue_id', how='left')
    
    # Geocode information
    newadd = []
    latitude = []
    longitude = []
    
    for add in completeVenueList['venue_address']:
        if type(add) is str:
            #(add, lat, lon) = geocode(add)
            add = add.replace(' ', '+')
            url = 'https://maps.googleapis.com/maps/api/geocode/json?address='+add+',+Toronto,+ON,+Canada&key=AIzaSyBkp0W5IHAXgcb28MN_8wnUMxO1BGOlM3E'
            r = requests.get(url,proxies = proxies).json()
            (add,lat,lon) = geocode(add)
            newadd.append(add)
            latitude.append(lat)
            longitude.append(lon)
    
        else:
            newadd.append(None)
            latitude.append(None)
            longitude.append(None)
            
    completeVenueList['venue_address_formatted'] = newadd
    completeVenueList['lat'] = latitude
    completeVenueList['lon'] = longitude
    