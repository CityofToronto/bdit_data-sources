# -*- coding: utf-8 -*-
"""
Created on Mon Nov 14 10:55:15 2016

@author: qwang2
"""
import re
import requests

def FormatAddress(add):
    '''
        INPUT: address text
        OUTPUT: formatted address text (or original text if function fails to recognize an address)
    '''
    address = re.compile('[1-9][0-9]*\s.*(Ave|Rd|St|Cres|Pkwy|Place|Blvd|Dr|Lane|Way|Cir|Towers|Trail|Quay|Terr|Square|Grove|Pl|Park|Ct)(\.)?(\s[EWNS])?')    
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
    add = add.replace("\'", "")
    m = address.search(add)
    if m is not None:
        add = add[m.start():m.end()]
    add = add.replace('.', '')
    return add
    
def geocode(add):
    ''' 
        INPUT: address text
        OUTPUT: (formatted long address, latitude, longitude)
    '''
    proxies = {'https':'https://137.15.73.132:8080'}
    add = add.replace(' ', '+')
    url = 'https://maps.googleapis.com/maps/api/geocode/json?address='+add+',+Toronto,+ON,+Canada&key=AIzaSyBkp0W5IHAXgcb28MN_8wnUMxO1BGOlM3E'
    r = requests.get(url,proxies = proxies).json()
    if r["status"] == 'ZERO_RESULTS':
        return (add.replace('+', ' '),None,None)
    try:
        lat = r["results"][0]["geometry"]["location"]["lat"]
        lon = r["results"][0]["geometry"]["location"]["lng"]
        add = r["results"][0]["formatted_address"]
    except:
        print(r["status"])
        return(add,None,None)
    return (add,lat,lon)

def rev_geocode(coord):    
    '''
        INPUT: latlong coordinates in string form
        OUTPUT: fortmatted short address and long address
    '''
    proxies = {'https':'https://137.15.73.132:8080'}
    url = 'https://maps.googleapis.com/maps/api/geocode/json?latlng='+coord+'&key=AIzaSyBkp0W5IHAXgcb28MN_8wnUMxO1BGOlM3E'
    r = requests.get(url,proxies = proxies).json()
    for d in r["results"]:
        if any((t in ['street_address', 'intersection', 'point_of_interest'] for t in d["types"])):
            add_sh = d['address_components'][0]['short_name']+ ' ' + d['address_components'][1]['short_name']
            return (add_sh, d['formatted_address'])
