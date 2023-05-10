# -*- coding: utf-8 -*-
"""
Created on Tue Oct 25 10:51:26 2016

@author: qwang2
"""
import sys
import time
import datetime
import configparser
import logging
import traceback
import argparse


import requests
from pg import DB
from AddressFunctions import geocode, rev_geocode, format_address, AddressParserException

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

    parser.add_argument('--proxy',
                        help='Proxy IP, like http://137.15.73.132:8080')

    return parser.parse_args(args)

def process_venue(i, l, db, curId):
    inserted_venue = 0
    # Get Venue Information
    venue = {}
    venue["tm_venue_id"] = l["id"]
    venue["venue_name"] = l["name"].replace("\'", "")

    if i % 50 == 0:

        logger.info('Processing venue #%s', i+1)
        logger.info('Venue: %s, id: %s',
                    venue["venue_name"],
                    venue["tm_venue_id"])

    exist = []
    if "address" in l.keys():
        if bool(l["address"]):
            (dummy, venue["venue_address"]) = l['address'].popitem()
            venue["venue_address"] = format_address(venue["venue_address"])
        else:
            venue["venue_address"] = None
        if venue["venue_address"] is not None:
            exist = db.query("SELECT * FROM city.venues where venue_add_comp = $1",
                             venue["venue_address"]).getresult()
    else:
        exist = []
        venue["venue_address"] = None
    venue["venue_add_comp"] = venue["venue_address"]

    if exist == [] and venue["venue_name"] is not None:
        names = db.query("SELECT venue_name FROM city.venues").getresult()
        for name in names:
            n = name[0].replace("\'", "")
            if (fuzz.ratio(n, venue["venue_name"]) > 80 or
                    fuzz.ratio(n, venue["venue_name"]) > 60 and
                    fuzz.partial_ratio(n, venue["venue_name"]) > 90):

                exist = db.query("SELECT * FROM city.venues where venue_name = $1",
                                 n).getresult()

    if exist == []:
        if (venue['venue_name'].find('TBA') > 0 or
            venue['venue_name'].find('Vary By') > 0):
            venue["id"] = 2
        else:
            logger.info('INSERT VENUE: %s', venue['venue_name'])
            logger.debug('Address: %s', venue["venue_address"])
            curId = curId + 1
            venue["id"] = curId
            if "location" in l.keys() and "postalCode" in l.keys():
                logger.debug('Postal Code: %s', l["postalCode"])
                lat = l["location"].get("latitude", 0)
                lon = l["location"].get("longitude", 0)
                logger.debug('Coords: (%s, %s)', lat, lon)
                if venue["venue_address"] is not None and lat != 0 and lon != 0:
                    add = venue["venue_address"] + ", Toronto, ON "
                    add += l["postalCode"] + ", Canada"
                elif int(lat) != 0 and int(lon) != 0:
                    coord = str(lat) + ',' + str(lon)
                    try:
                        (venue["venue_add_comp"], add) = rev_geocode(coord)
                    except AddressParserException as ape:
                        logger.error(ape)
                elif venue["venue_address"] is not None:
                    (add, lat, lon) = geocode(venue["venue_address"])
                else:
                    add = None
            else:
                (add, lat, lon) = geocode(venue["venue_address"])
            venue["venue_address"] = add
            venue["lat"] = lat
            venue["lon"] = lon
            venue["capacity"] = None
            db.insert('city.venues', venue)
            inserted_venue += 1
    else:
        for venue["id"] in exist[0]:
            if type(venue["id"]) == int:
                break
    return venue, inserted_venue


def update_venues(db, proxies, curId):
    r = {}
    venues = []
    inserted_venues = 0
    venues_processed = 0
    while True: 
        
        if not r:
            logger.info('Requesting initial ticketmaster venues page')
            url = 'https://app.ticketmaster.com/discovery/v2/venues.json'
            params = {'size':200,
                      'stateCode':'ON',
                      'countryCode':'CA',
                      'includeTest':'no',
                      'keyword':'Toronto',
                      'apikey':API_KEY}
        elif "next" in r["_links"].keys():
            logger.info('Requesting next ticketmaster venues page')
            url = 'https://app.ticketmaster.com'
            url += r["_links"]["next"]["href"]
            params = {'apikey':API_KEY}
        else:
            logger.info('Ticketmaster venues pages exhausted')
            break

        r = requests.get(url, proxies=proxies, params=params)

        r.raise_for_status()
        r = r.json()

        logger.info('Processing venues')
        logger.debug(r.keys())
        
        for l in r["_embedded"]["venues"]:
            if l["city"]["name"] == 'Toronto':
                venues_processed += 1
                venue, inserted_venue = process_venue(venues_processed,
                                                      l,
                                                      db,
                                                      curId)
                venues.append(venue)

                # Update TM venues table
                exist = db.query("SELECT * FROM city.tm_venues where tm_venue_id = $1",
                                 venue["tm_venue_id"]).getresult()
                if exist == []:
                    db.insert('city.tm_venues', venue)
                    inserted_venue += 1
                    logger.error('Venue %s, inserted twice', venue['name'])
                inserted_venues += inserted_venue
        
    return venues, inserted_venues

def update_events(db, proxies, venues):
    inserted_count = 0
    for i, venue in enumeratate(venues):

        if i % 50 == 0:
            logger.info('Getting events for venue #%s', i+1)
            logger.info('Venue: %s, id: %s',
                        venue["venue_name"])
        params = {'apikey': API_KEY,
                  'venueId': venue["tm_venue_id"]}
        r = requests.get('https://app.ticketmaster.com/discovery/v2/events.json',
                         proxies=proxies,
                         params=params).json()
        if "_embedded" in r.keys():
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
                    event["start_time"] = datetime.time(t[3], t[4], t[5])
                except KeyError:
                    event["start_time"] = None
                event["tm_venue_id"] = venue["tm_venue_id"]
                exist = db.query("SELECT * FROM city.TM_events where tm_event_id = $1",
                                 event["tm_event_id"]).getresult()
                if exist == []:
                    db.insert('city.TM_events', event)
                    inserted_count += 1
    return inserted_count


def main(**kwargs):
    CONFIG = configparser.ConfigParser()
    CONFIG.read('db.cfg')
    dbset = CONFIG['DBSETTINGS']

    logger.info('Connecting to Database')
    db = DB(dbname=dbset['database'],
            host=dbset['host'],
            user=dbset['user'],
            passwd=dbset['password'])
    proxies = {'https': kwargs.get('proxy', None)}

    # Update Venue List
    venues = []
    curId = db.query('SELECT max(id) FROM city.venues').getresult()[0][0]

    logger.info('Updating venues table')
    venues, inserted_venues = update_venues(db, proxies, curId)


    # Get Events from List of Venues
    #cla = []
    logger.info('Finished updating venues tables, %s new venues inserted',
                inserted_venues)

    inserted_count = update_events(db, proxies, venues)
    logger.info('Finished processing events, %s events inserted', inserted_count)
    db.close()

if __name__ == '__main__':
    logger.setLevel(logging.DEBUG)
    try:
        main(**vars(parse_args(sys.argv[1:])))
    except Exception as exc:
        logger.critical(traceback.format_exc())
