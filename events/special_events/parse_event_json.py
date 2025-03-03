# -*- coding: utf-8 -*-
"""
Created on Thu Oct 27 10:12:48 2016

@author: qwang2
"""

import sys
import configparser
import argparse
import logging
import traceback
import datetime

import requests
from pg import DB
from fuzzywuzzy import fuzz

from AddressFunctions import geocode
from AddressFunctions import format_address

logger = logging.getLogger(__name__)
handler = logging.StreamHandler()
formatter = logging.Formatter('%(asctime)s %(name)-12s %(levelname)-8s %(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)

CURID, ODID = 0, 0

def convert_date(date):
    date = datetime.datetime.strptime(date, '%Y-%m-%dT%H:%M:%S.%fZ')
    return date.replace(tzinfo=datetime.timezone.utc).astimezone(tz=None).date()

def convert_time(date):
    date = datetime.datetime.strptime(date, '%Y-%m-%dT%H:%M:%S.%fZ')
    return date.replace(tzinfo=datetime.timezone.utc).astimezone(tz=None).time()

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

def process_event(i, entry, db):
    inserted_venue = 0
    updated_venue = 0
    # Extract Information
    row = {}
    row["id"] = entry['recId']
    row["event_name"] = entry['eventName']

    if i % 50 == 0:
        logger.info('Processing event #%s', i)
        logger.info('Event: %s, event_id: %s', row["event_name"], row["id"])
    #if entry['locations'][0]['locationName'] is not None:
    row["venue_name"] = entry['locations'][0]['locationName'].replace("\'", "")

    row["venue_address"] = row["venue_add_comp"] = entry['locations'][0]['address']
    row["start_date"] = convert_date(entry['startDate'])
    row["end_date"] = convert_date(entry['endDate'])
    row["start_time"] = convert_time(entry['startDate'])
    row["end_time"] = convert_time(entry['endDate'])

    cat = ''
    for c in entry['category']:
        cat = cat + c['name'] + ','
    row["classification"] = cat[:len(cat)-1]

    # Update Venues Table    
    exist = db.query("SELECT * FROM city.venues where venue_add_comp = $1",
                     row["venue_add_comp"]).getresult()
    if exist == []:
        names = db.query("SELECT venue_name FROM city.venues").getresult()
        for name in names:
            if (fuzz.ratio(name[0], row["venue_name"]) > 80 or
                    fuzz.ratio(name[0], row["venue_name"]) > 60 and
                    fuzz.partial_ratio(name[0], row["venue_name"]) > 90):
                
                exist = db.query("SELECT * FROM city.venues where venue_name = $1",
                                 name[0]).getresult()

    venue = {}
    if exist == []:
        #insert
        global CURID
        CURID += 1
        venue["id"] = CURID
        venue["venue_name"] = row["venue_name"]
        venue["venue_add_comp"] = row["venue_add_comp"]

        if row["venue_add_comp"] is not None and not entry['locations'][0]['geoCoded']:
            (add, lat, lon) = geocode(format_address(row["venue_add_comp"]))
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
        logger.info('INSERT VENUE: %s', row["venue_name"])
        inserted_venue += 1
        row["venue_id"] = CURID
    elif None in exist[0][0:4]:
        #update
        venue["id"] = exist[0][2]
        row["venue_id"] = exist[0][2]
        venue["venue_name"] = row["venue_name"]
        venue["venue_add_comp"] = row["venue_add_comp"]
        if row["venue_add_comp"] is not None and not entry['locations'][0]['geoCoded']:
            (add, lat, lon) = geocode(format_address(row["venue_add_comp"]))
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
        updated_venue += 1
        logger.info('UPSERT VENUE %s', row["venue_name"])

    else:
        # do nothing
        row["venue_id"] = exist[0][2]


   # Update ODVenues Table
    venue = {}
    exist = db.query("SELECT * FROM city.od_venues where venue_address = $1",
                     row["venue_add_comp"]).getresult()
    if exist == []:
        exist = db.query("SELECT * FROM city.od_venues where venue_name = $1",
                         row["venue_name"]).getresult()
    if exist == []:
        global ODID
        ODID += 1
        venue["venue_address"] = row["venue_add_comp"]
        venue["venue_name"] = row["venue_name"]
        venue["od_id"] = ODID
        venue["id"] = row["venue_id"]
        db.insert('city.od_venues', venue)
        inserted_venue += 1
        row["od_venue_id"] = ODID
    else:
        row["od_venue_id"] = exist[0][0]

    db.upsert('city.od_events', row)
    if inserted_venue > 1 | updated_venue > 1:
        logger.error('Venue id %s updated multiple times', venue["id"])
    return inserted_venue, updated_venue

def main(**kwargs):

    CONFIG = configparser.ConfigParser()
    CONFIG.read('db.cfg')
    dbset = CONFIG['DBSETTINGS']

    logger.info('Connecting to Database')
    db = DB(dbname=dbset['database'],
            host=dbset['host'],
            user=dbset['user'],
            passwd=dbset['password'])
    
    proxies = {'http':kwargs.get('proxy', None)}

    logger.info('Requesting data')
    r = requests.get('http://app.toronto.ca/cc_sr_v1_app/data/edc_eventcal_APR', proxies=proxies)

    events = r.json()

    global CURID, ODID
    CURID = db.query('SELECT max(id) FROM city.venues').getresult()[0][0]
    ODID = db.query('SELECT max(od_id) FROM city.od_venues').getresult()[0][0]

    logger.info('Processing events')
    inserted_events, inserted_venues, updated_venues = 0, 0, 0
    
    for i, entry0 in enumerate(events):
        try:
            inserted_venue, updated_venue = process_event(i, entry0['calEvent'], db)
            inserted_events += 1
            inserted_venues += inserted_venue
            updated_venues += updated_venue
        except KeyError as key_error:
            logger.error('Key error with event: %s, key %s, skipping', 
                         entry0['calEvent'].get('eventName', ''),
                         key_error.args[0])

    logger.info('%s events processed, %s venues inserted, %s venues updated',
                inserted_events,
                inserted_venues,
                updated_venues)
    logger.info('closing connection to DB')
    db.close()

    
if __name__ == '__main__':
    logger.setLevel(logging.INFO)
    try:
        main(**vars(parse_args(sys.argv[1:])))
    except Exception as exc:
        logger.critical(traceback.format_exc())
            
#Events = pd.DataFrame({'Event':event, 'Venue':venue, 'Venue Address':venueAdd, 'Category':category, 'DateBegin':dateBegin, 'TimeBegin':timeBegin, 'DateEnd':dateEnd, 'TimeEnd':timeEnd})
#Events.to_csv('city_open_data_events.csv')
