# -*- coding: utf-8 -*-
"""
Created on Fri Oct 28 15:30:21 2016

@author: qwang2
"""

import pandas as pd
import re

def cleanupaddress():    
    address = re.compile('[1-9][0-9]*\s.*\s(Ave|Rd|St|Cres|Pkwy|Place|Blvd|Dr|Lane|Way|Cir|Towers|Trail)(\.)?(\s[EWNS])?')
    
    # Clean up address format
    newadd = []
    for add in TMvenues['venue_address']:
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
            add = add.replace(',', '')
            m = address.search(add)
            if m is not None:
                add = add[m.start():m.end()]
            add = add.replace('.', '')
            newadd.append(add)
        else:
            newadd.append('')
    TMvenues['venue_address'] = newadd
    TMvenues['postcode'] = None
    newadd = []
    postcodes = []
    postcode = re.compile('[A-Z][0-9][A-Z]\s?[0-9][A-Z][0-9]')
    for add in Cityevents['venue_address']:
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
            add = add.replace(',', '')
            m = address.search(add)
            if m is not None:
                add1 = add[m.start():m.end()]
                add1 = add1.replace('.', '')
            else:
                m = postcode.search(add)
                if m is not None:
                    add1 = add[:m.start()]
                else:
                    add1 = add
            newadd.append(add1)
            m = postcode.search(add)
            if m is not None:
                add1 = add[m.start():m.end()]
                postcodes.append(add1)
            else:
                postcodes.append(None)
        else:
            newadd.append(None)
            postcodes.append(None)
            
    Cityevents['venue_address'] = newadd
    Cityevents['postcode'] = postcodes
    
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
    ID = []
    curId = max(TMvenues['id'])
    for (venueAdd,venueName) in zip(Cityevents['venue_address'],Cityevents['venue_name']):
        if venueAdd in addMap.keys():
            ID.append(addMap[venueAdd])
        elif venueName in nameMap.keys():
            ID.append(nameMap[venueName])
        else:
            curId = curId + 1
            ID.append(curId)
            addMap[venueAdd] = curId  
            nameMap[venueName] = curId
    Cityevents['id'] = ID
    
    # Combine the two venue lists
    completeVenueList = pd.concat([Cityevents[['venue_name', 'venue_address', 'id']], TMvenues[['venue_name', 'venue_address', 'id']]]).drop_duplicates(['id'])
    pcMap = dict(zip(Cityevents['id'], Cityevents['postcode']))
    postcodes = []
    i = 0
    for Id in completeVenueList['id']:
        if Id in pcMap.keys():
            postcodes.append(pcMap[Id])
            i = i + 1
        else:
            postcodes.append(None)
    completeVenueList['postcode'] = postcodes
    
    
    # Prepare file to be geocoded
    # Extract addresses with postcodes specified
    GC = completeVenueList.dropna()
    GC.to_csv('Geocode_venues.csv', index = False)

def cleanupdatetimeinfo():
    StartDate = []
    EndDate = []
    monthDict = {'Jan':1, 'Feb':2, 'Mar':3, 'Apr':4, 'May':5, 'June':6, 'July':7, 'Aug':8, 'Sept':9, 
                 'Oct':10, 'Nov':11, 'Dec':12, 'January':1, 'February':2, 'March':3, 'April':4, 'August':8,
                 'September':9, 'October':10, 'November':11, 'December':12}
    year = re.compile('201[0-9]')
    month = re.compile('[A-Z][a-z]+')
    date = re.compile('[1-9][0-9]?')
    for d in Cityevents['date']:
        if any(c.isalpha() for c in d):
            yr = year.search(d)
            while yr is not None:
                d = d[:yr.start()-1] + d[yr.end()+1:]
                yr = year.search(d)
            mo = month.findall(d)
            da = date.findall(d)
            if not da or not mo or len(da)==1:
                print(d)
                EndDate.append(None)
                StartDate.append(None)
            elif any([m1 not in monthDict.keys() for m1 in mo]):
                print(d)
                EndDate.append(None)
                StartDate.append(None)
            else:
                m1 = monthDict[mo[0]]
                d1 = da[0]
                d2 = da[1]
                if m1<10:
                    m1 = '0'+str(m1)
                else:
                    m1 = str(m1)
                if int(d1) < 10:
                    d1 = '0' + d1
                if int(d2) < 10:
                    d2 = '0' + d2
                if len(mo) == 2:
                    m2 = monthDict[mo[1]]
                    if m2<10: 
                        m2 = '0'+str(m2)
                    else:
                        m2 = str(m2)
                    EndDate.append(pd.to_datetime('2015'+m2+d2, format = '%Y%m%d'))
                else:
        
                    EndDate.append(pd.to_datetime('2015'+m1+d2, format = '%Y%m%d'))
                StartDate.append(pd.to_datetime('2015'+m1+d1, format = '%Y%m%d'))            
        else:
            StartDate.append(pd.to_datetime(d))
            EndDate.append(pd.to_datetime(d))
    Cityevents['start_date'] = StartDate
    Cityevents['end_date'] = EndDate
    Cityevents.to_csv('2015Events.csv')
             
TMvenues = pd.read_csv('tm_venues.csv',encoding = 'latin-1')
Cityevents = pd.read_csv('2015EventsFull.csv',encoding = 'latin-1')

#cleanupaddress()
#cleanupdatetimeinfo()
