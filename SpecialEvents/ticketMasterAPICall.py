# -*- coding: utf-8 -*-
"""
Created on Tue Oct 25 10:51:26 2016

@author: qwang2
"""
import time
import datetime
import configparser
import logging

import requests
import pandas as pd
from pg import DB
from AddressFunctions import geocode
from AddressFunctions import rev_geocode
from AddressFunctions import format_address
from fuzzywuzzy import fuzz


API_KEY = 'A3sEV24x7118ADXEEDhenqtDxmH3ijxg'


logger = logging.getLogger(__name__)
handler = logging.StreamHandler()
formatter = logging.Formatter('%(asctime)s %(name)-12s %(levelname)-8s %(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)

def parse_args(args):
    """Parse command line arguments
    
    Args:
        sys.argv[1]: command line arguments
        
    Returns:
        dictionary of parsed arguments
    """
    parser = argparse.ArgumentParser(description='Scrapes Toronto OpenData Events')
    
    parser.add_argument('--proxy', help='Proxy IP, like http://137.15.73.132:8080')
    
    return parser.parse_args(args)

def update_venues(db, proxies):
    r = None
    venues = []
    inserted_venues += 1
    
    while (True): 
        
        if r is None:
            logger.info('Requesting initial ticketmaster venues page')
            url = 'https://app.ticketmaster.com/discovery/v2/venues.json'
            params = {'size':499,
                      'stateCode':'ON',
                      'countryCode':'CA',
                      'includeTest':'no',
                      'markets':102,
                      'city':'Toronto'.
                      'apikey':API_KEY}
        elif "next" in r["_links"].keys():
            logger.info('Requesting next ticketmaster venues page')
            url = 'https://app.ticketmaster.com'+r["_links"]["next"]["href"][:len(r["_links"]["next"]["href"])-7]
            params = {'apikey':API_KEY}
        else:
            logger.info('Ticketmaster venues pages exhausted')
            break;
        
        r = requests.get(url, proxies=proxies, params=params).json()
        
        logger.info('Processing venues')
        
        for i, l in enumerate(r["_embedded"]["venues"]):
            if l["city"]["name"] == 'Toronto':

                
                # Get Venue Information
                venue = {}
                venue["tm_venue_id"] = l["id"]
                venue["venue_name"] = l["name"].replace("\'","")

                if i % 50 == 0:
                    logger.info('Processing venue #%s', i+1)
                    logger.info('Venue: %s, id: %s', venue["venue_name"], venue["tm_venue_id"])

                if "address" in l.keys():
                    if bool(l["address"]):
                        (dummy, venue["venue_address"]) = l['address'].popitem()
                        venue["venue_address"] = format_address(venue["venue_address"])
                    else:
                        venue["venue_address"] = None
                    if venue["venue_address"] is not None:
                        exist = db.query("SELECT * FROM city.venues where venue_add_comp = $1", venue["venue_address"]).getresult()
                else:
                    exist = []
                    venue["venue_address"] = None
                venue["venue_add_comp"] = venue["venue_address"]

                if exist == [] and venue["venue_name"] is not None:
                    names = db.query("SELECT venue_name FROM city.venues").getresult()
                    for name in names :
                        n = name[0].replace("\'","")
                        if fuzz.ratio(n, venue["venue_name"]) > 80 or fuzz.ratio(n, venue["venue_name"]) > 60 and fuzz.partial_ratio(n, venue["venue_name"]) > 90:
                            exist = db.query("SELECT * FROM city.venues where venue_name = $1", n).getresult()

                if exist == []:       
                    if venue['venue_name'].find('TBA') > 0 or venue['venue_name'].find('Vary By') > 0:
                        venue["id"] = 2
                    else:
                        logger.info('INSERT VENUE: %s', venue['venue_name'])
                        curId = curId + 1
                        venue["id"] = curId
                        if "location" in l.keys() and "postalCode" in l.keys():
                            if "latitude" in l["location"].keys() and "longitude" in l["location"].keys():
                                lat = l["location"]["latitude"]
                                lon = l["location"]["longitude"]
                            if venue["venue_address"] is not None and lat != 0 and lon != 0:
                                add = venue["venue_address"]+", Toronto, ON " + l["postalCode"] +", Canada"
                            elif lat!=0 and lon!=0:
                                coord = str(lat) + ',' + str(lon)
                                r = requests.get(url,proxies = proxies).json()
                                (venue["venue_add_comp"],add) = rev_geocode(coord)
                            elif venue["venue_address"] is not None:
                                (add,lat,lon) = geocode(venue["venue_address"])
                            else:
                                add = None
                        else:
                            (add,lat,lon) = geocode(venue["venue_address"])
                        venue["venue_address"] = add
                        venue["lat"] = lat
                        venue["lon"] = lon
                        venue["capacity"] = None
                        db.insert('city.venues', venue)
                        inserted_venues += 1
                else:
                    for venue["id"] in exist[0]:
                        if type(venue["id"]) == int:
                            break 
                venues.append(venue)

                # Update TM venues table
                exist = db.query("SELECT * FROM city.tm_venues where tm_venue_id = $1", venue["tm_venue_id"]).getresult()
                if exist == []:
                    db.insert('city.tm_venues', venue)
                    inserted_venues += 1
    return venues, inserted_venues

def update_events(db, venues):
    inserted_count = 0
    for i, venue in enumerate(venues):

        params = {'apikey': API_KEY,
                  'venueId': venue["tm_venue_id"]}
        r = requests.get('https://app.ticketmaster.com/discovery/v2/events.json',
                         proxies=proxies,
                         params=params).json();
        if "_embedded" in r.keys():
            for l in r["_embedded"]["events"]:
                event = {}
                event['tm_event_id'] = l["id"]
                try:
                    for c in l["classifications"]:
                        if c["primary"]:
                            try:
                                event["classification"] = c["segment"]["name"]
                                '''
                                if event["classification"] not in cla:
                                    cla.append(event["classification"])'''
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
                    event["start_time"] = datetime.time(t[3], t[4], t[5])
                except KeyError:
                    event["start_time"] = None
                event["tm_venue_id"] = venue["tm_venue_id"]
                exist = db.query("SELECT * FROM city.TM_events where tm_event_id = $1", event["tm_event_id"]).getresult()
                if exist == []:
                    db.insert('city.TM_events', event)
                    inserted_count += 1
    return inserted_count

def main(**kwargs):
    CONFIG = configparser.ConfigParser()
    CONFIG.read('db.cfg')
    dbset = CONFIG['DBSETTINGS']

    logger.info('Connecting to Database')
    db = DB(dbname=dbset['database'],host=dbset['host'],user=dbset['user'],passwd=dbset['password'])
    proxies = {'https':kwargs.get('proxy', None)}

    # Update Venue List
    venues = []
    curId = db.query('SELECT max(id) FROM city.venues').getresult()[0][0]
    
    logger.info('Updating venues table')
    venues, inserted_venues = update_venues(db, proxies)
    

    # Get Events from List of Venues
    #cla = []
    logger.info('Finished updating venues tables, %s new venues inserted', inserted_venues)
    
    inserted_count = update_events(db, venues)
    logger.info('Finished processing events, %s events inserted', inserted_count)
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

if __name__ == '__main__':
    logger.setLevel(logging.INFO)
    try:
        main(**vars(parse_args(sys.argv[1:])))
    except Exception as exc:
        logger.critical(exc)