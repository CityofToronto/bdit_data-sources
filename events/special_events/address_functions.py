# -*- coding: utf-8 -*-
"""
Created on Mon Nov 14 10:55:15 2016

@author: qwang2
"""
import re
import logging
import requests

LOGGER = logging.getLogger(__name__)

class AddressParserException(Exception):
    pass

def format_address(add):
    global LOGGER
    '''
        INPUT: address text
        OUTPUT: formatted address text (or original text if function fails to recognize an address)
    '''
    add = add.lower()
    add = add.replace('avenue', 'ave')
    add = add.replace(' av ', ' ave ')
    add = add.replace('road', 'rd')
    add = add.replace('street', 'st')
    add = add.replace('crescent', 'cres')
    add = add.replace('parkway', 'pkwy')
    add = add.replace('drive', 'dr')
    add = add.replace('terrace', 'terr')
    add = add.replace('boulevard', 'blvd')
    add = add.replace('circle', 'cir')
    add = add.replace(',', '')
    add = add.replace("\'", "")
    add = add.replace('.', '')
    add1 = add.split()
    add = ''
    LOGGER.debug(add1)
    for word in add1:
        if word == 'west': 
            word = 'w'
        if word == 'east': 
            word = 'e'
        if word == 'north': 
            word = 'n'
        if word == 'south': 
            word = 's'
        add = add + word[0].upper() + word[1:] + ' '
    
    address = re.compile('[1-9][0-9]*\s.*(Ave|Rd|St|Cres|Pkwy|Place|Blvd|Dr|Lane|Way|Cir|Towers|Trail|Quay|Terr|Square|Grove|Pl|Park|Ct)(\.)?(\s[EWNS])?')    
    m = address.search(add)
    if m is not None:
        add = add[m.start():m.end()]
        return add
    else:
        return add[:-1]
        
def geocode(add):
    ''' 
        INPUT: address text
        OUTPUT: (formatted long address, latitude, longitude)
    '''
    global LOGGER
    proxies = {'https':'https://137.15.73.132:8080'}
    add = add.replace(' ', '+')
    url = 'https://maps.googleapis.com/maps/api/geocode/json?address='+add+',+Toronto,+ON,+Canada&key=AIzaSyBkp0W5IHAXgcb28MN_8wnUMxO1BGOlM3E'
    r = requests.get(url, proxies = proxies).json()
    if r["status"] == 'ZERO_RESULTS':
        return (add.replace('+', ' '),None,None)
    try:
        lat = r["results"][0]["geometry"]["location"]["lat"]
        lon = r["results"][0]["geometry"]["location"]["lng"]
        add = r["results"][0]["formatted_address"]
    except:
        LOGGER.error('Geocoding failed for %s', add)
        LOGGER.error('Request status: %s', r["status"])
        return(add,None,None)
    return (add,lat,lon)

def rev_geocode(coord):    
    '''
        INPUT: latlong coordinates in string form
        OUTPUT: fortmatted short address and long address
    '''
    global LOGGER
    proxies = {'https':'https://137.15.73.132:8080'}
    url = 'https://maps.googleapis.com/maps/api/geocode/json?latlng='+coord+'&key=AIzaSyBkp0W5IHAXgcb28MN_8wnUMxO1BGOlM3E'
    r = requests.get(url,proxies = proxies)
    if r.status_code != requests.codes.ok:
        errmsg = 'Reverse geocoding failed with {}'.format(coord)
        raise AddressParserException(errmsg)
    r = r.json()
    
    if len(r["results"]==0):
        errmsg = 'No reverse geocoding results for {}'.format(coord)
        raise AddressParserException(errmsg)
    for d in r["results"]:
        if any((t in ['street_address', 'intersection', 'point_of_interest'] for t in d["types"])):
            add_sh = d['address_components'][0]['short_name']+ ' ' + d['address_components'][1]['short_name']
            return (add_sh, d['formatted_address'])
