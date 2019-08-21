# -*- coding: utf-8 -*-
"""
Created on Thu Feb 28 10:16:47 2019

Script to convert text from an xml to geometries 
and put the output into a postgres DB table 

@author: crosic
"""

import pandas as pd
import xml.etree.cElementTree as et
import psycopg2
import configparser
import pandas.io.sql as psql


# Part 1
# read xml into pandas dataframe


def getvalueofnode(node):
    """ return node text or None """
    return node.text if node is not None else None


parsedXML = et.parse(r"C:\Users\crosic\Downloads\traffic-and-parking-by-law-schedules-data-march-2019\Ch_950_Sch_35_SpeedLimitOnPublicHighways.xml")
dfcols = ['ID', 'Chapter', 'Schedule', 'ScheduleName', 'Highway', 'Between', 'Speed_Limit_km_per_hr']
df_xml = pd.DataFrame(columns=dfcols)
for node in parsedXML.getroot():
    ID = node.find('ID')
    Chapter = node.find('Chapter')
    Schedule = node.find('Schedule')
    ScheduleName = node.find('ScheduleName')
    Highway = node.find('Highway')
    Between = node.find('Between')
    Speed_Limit_km_per_hr = node.find('Speed_Limit_km_per_hr')
    
    df_xml = df_xml.append(
            pd.Series([getvalueofnode(ID), getvalueofnode(Chapter), getvalueofnode(Schedule),
                       getvalueofnode(ScheduleName), getvalueofnode(Highway),  
                       getvalueofnode(Between),  getvalueofnode(Speed_Limit_km_per_hr)], 
                       index=dfcols),
            ignore_index=True)

# 6554 rows     
print(df_xml.shape[0])


repealed = df_xml[df_xml.Between.isnull()]
# 1521
print(repealed.shape[0])


bylaws = df_xml[df_xml.Between.notnull()]
# 5033
print(bylaws.shape[0])


# Part 2
# match bylaws to geometry 



CONFIG = configparser.ConfigParser()
CONFIG.read('db.cfg')
dbset = CONFIG['DBSETTINGS']
con = psycopg2.connect(**dbset)

from sqlalchemy import create_engine

engine = create_engine('postgresql://' + dbset['user'] + ':' + dbset['password'] + '@' + dbset['host'] + '/' + dbset['database'])


# get one row at a time 
def text_to_centreline(highway, between):
   try:
      df = psql.read_sql("SELECT centreline_segments AS geom,  con AS confidence, notice, line_geom, oid1_geom, oid2_geom, street_name_arr, ratio FROM crosic.text_to_centreline('{}', '{}', {})".format(highway.replace("'", ""), between.replace("'", ""), 'NULL'), con)
   except Exception as e:
        print("------------------------")
        print("street: " + highway) 
        print(" from: " + between)
        print("ERROR " + str(e))
        print("------------------------")
        raise Exception("Error in text_to_centreline: " + str(e))
        return  [highway, between, "Error", df['geom'].item(), df['notice'].item(), df['line_geom'].item(), df['oid1_geom'].item(), df['oid2_geom'].item(), df['street_name_arr'].item(), df['ratio'].item()]

   return [highway, between, df['confidence'].item(),  df['geom'].item(), df['notice'].item(), df['line_geom'].item(), df['oid1_geom'].item(), df['oid2_geom'].item(), df['street_name_arr'].item(), df['ratio'].item()]

def call_text_to_centreline_on_every_row(bylaws):
    rows = []
    total_rows = bylaws.shape[0]
    for index, row in bylaws.iterrows():
        
        # print messages to tell user where you are in process
        if index%100 == 0:
          print("Calling SQL function on index " + str(index))	
        if index == total_rows-1:
          print("Processing last row with index value of " + str(index))
        
        # call text_to_centreline function
        try: 
          row_with_wkt = text_to_centreline(row[4], row[5])
          rows.append([row[0]] +  row_with_wkt)
        except Exception as e:
          rows.append([row[0], row[1], row[2], "No Match - ERROR", None, str(e), None, None, None])
          continue 
        
    return pd.DataFrame(data=rows, columns=['id','street_name', 'extents',  'confidence', 'geom', 'notice', 'line_geom', 'oid1_geom', 'oid2_geom', 'street_name_arr', 'ratio'])  #gpd.GeoDataFrame(data=rows)

    
    
# convert list of rows to sql table 
geom_data = call_text_to_centreline_on_every_row(bylaws)
geom_data.to_sql('posted_speed_limit_xml_open_data_withQC', engine)


con.close()

