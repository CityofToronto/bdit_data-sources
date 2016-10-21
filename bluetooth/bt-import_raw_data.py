from __future__ import division
from suds.client import Client 
import pandas as pd
import datetime
import calendar
import time
import urllib.request
import numpy as np
from sqlalchemy import create_engine

# function to convert binary objects to dataframe
#   inList is a list of lists of dataObjects
#   each element of inList is one day of data
#   each day has however many cars triggered the sensor
#def toDataFrame(inList):
#    colNames = ['measured_timestamp','measured_time','analysis_id','outlier_level']
#    df = pd.DataFrame(columns=colNames)
#    
#    for item in inList:
#        for data in item:
#            df = df.append({'measured_timestamp':pd.to_datetime(data.measuredTimeTimestamp, infer_datetime_format=True, format='%m/%d/%Y  %H:%M:%S %p'),'measured_time':data.measuredTime,'analysis_id':data.analysisId,'outlier_level':data.outlierLevel},ignore_index=True)
#    
#    return df

def selectDate():
    dateStr = str(input('Input a date [YYYY-MM-DD]: '))
    return(datetime.datetime.strptime(dateStr, '%Y-%m-%d'))

def accessAPI(inList,un,pw,config):
    try:
        inList.append(blip.service.exportPerUserData(un,pw,config)) 
    except urllib.error.URLError: 
        print('In the error handler (URL error)...')            
        time.sleep(30)
        inList = accessAPI(inList,un,pw,config)
    except Exception: 
        print('In the error handler (SUDS error)...')            
        time.sleep(30)
        inList = accessAPI(inList,un,pw,config)
    return inList
    
############### CODE BEGINS HERE ###############

# credentials go here!

# Access the API using the suds package
print('Going in!')
try:
    blip = Client(WSDLfile, proxy=proxy, faults=True)
except urllib.error.URLError:
    print('hi')    
    time.sleep(10)
    blip = Client(WSDLfile, proxy=proxy, faults=True)
    
# Create a config object
config = blip.factory.create('perUserDataExportConfiguration')
config.live = False
config.includeOutliers = True

#list of all route segments
allAnalyses = blip.service.getExportableAnalyses(un,pw)
for i in range(len(allAnalyses)):
    print(str(i) + ',' + allAnalyses[i].reportName)
    
# Pull data from all relevant indices
# allInd = [0,1,4,6,7,8,9,10,11,12,13,15,16,58,59,60,61,74,75,76,77,78,79,84]
allInd = [0,1,4,6,7,8,9,10,11,12,13,14,15,16,85,59,60,61,70,71,74,75,76,77,78,79,80,81,82,83,84,85,86,87,95,96,97,98,99]
allInd.extend(list(range(102,131)))
objectList = []

# Variables to edit
start_year = 2014
end_year = 2016
start_month = 1
end_month = 12


for j in allInd:
    for year in range(start_year, end_year+1):
        for month in range(start_month, end_month+1):
            ndays = calendar.monthrange(year, month)[1]
            print('Reading from: ' + str(allAnalyses[j].reportName))
            print(datetime.datetime.now().time())
            config.analysisId = allAnalyses[j].id
            config.startTime = datetime.datetime(year, month,1,0,0,0)
   
            for i in range(ndays):
                for k in range(2):                
                    config.endTime = config.startTime + datetime.timedelta(hours = 12)
    
                    print('Accessing API...')
                    print(datetime.datetime.now().time())
                    objectList = accessAPI(objectList,un,pw,config)
        
                    config.startTime = config.startTime + datetime.timedelta(hours = 12)
                    print('Sleeping...')
                    print(datetime.datetime.now().time())
                    time.sleep(2)
    
            x = pd.DataFrame([Client.dict(item) for sublist in objectList for item in sublist])
            x = x.rename(columns={"userId" : "user_id", "analysisId" : "analysis_id", "measuredTime" : "measured_time", "measuredTimeNoFilter" : "measured_time_no_filter", \
            "startPointNumber" : "startpoint_number", "startPointName" : "startpoint_name", "endPointNumber" : "endpoint_number", "endPointName" : "endpoint_name", \
            "measuredTimeTimestamp" : "measured_timestamp", "outlierLevel" : "outlier_level", "deviceClass" : "device_class"})
            x.measured_timestamp = pd.to_datetime(x.measured_timestamp, infer_datetime_format=True, format='%m/%d/%Y  %H:%M:%S %p')
            x.to_sql('raw_data',engine, schema = 'bluetooth', if_exists = 'append', index = False)
            objectList = []
    
    
    
