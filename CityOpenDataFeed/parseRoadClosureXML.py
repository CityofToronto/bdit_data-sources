# -*- coding: utf-8 -*-
"""
Created on Thu Oct 27 10:12:48 2016

@author: qwang2
"""

import requests
import xml.etree.ElementTree as ET
from pg import DB
import time


proxies = {'http':'http://137.15.73.132:8080'}
db = DB(dbname='bigdata',host='137.15.155.38',port=5432,user='qwang2',passwd='xe3cer4')


r = requests.get('http://www1.toronto.ca/transportation/roadrestrictions/RoadRestrictions.xml', proxies = proxies)

tree = ET.fromstring(r.content)

# if writing to csv
'''
f = open('RoadClosures_open_data.csv', 'w')
f.write("id,road,name,district,latitude,longitude,roadclass,planned,severityoverride,"\
        "source,lastupdated,starttime,endtime,workperiod,expired,signing,notification,workeventtype,"\
        "contractor,permittype,description\n")
''' 

# upload directly to database
data = []
for a in zip(tree.findall(".//Id"), tree.findall(".//Road"), tree.findall(".//Name"),tree.findall(".//District"),
             tree.findall(".//Latitude"), tree.findall(".//Longitude"), tree.findall(".//RoadClass"),
             tree.findall(".//Planned"), tree.findall(".//SeverityOverride"), tree.findall(".//Source"),
             tree.findall(".//LastUpdated"), tree.findall(".//StartTime"), tree.findall(".//EndTime"), 
             tree.findall(".//WorkPeriod"), tree.findall(".//Expired"), tree.findall(".//Signing"), 
             tree.findall(".//Notification"), tree.findall(".//WorkEventType"), tree.findall(".//Contractor"),
             tree.findall(".//PermitType"), tree.findall(".//Description")):
    rowsql = []
    for x in a:
        if x.tag in ('LastUpdated', 'StartTime', 'EndTime') and x.text is not None:
            y = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(float(x.text)/1000))
        else:
            y = x.text
        if x.tag == 'Name':
            rowsql.append(None)
            rowsql.append(None)
            rowsql.append(None)
        rowsql.append(y)
    rowsql.append(None)
    data.append(rowsql)
    
db.truncate('city.restrictions_import')
db.inserttable('city.restrictions_import', data)    

sql = db.query("DELETE FROM city.restrictions USING city.restrictions_import WHERE city.restrictions.id = city.restrictions_import.id")
sql= db.query("INSERT INTO city.restrictions SELECT * FROM city.restrictions_import")
db.close()
