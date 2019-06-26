# -*- coding: utf-8 -*-
"""
Created on Thu Feb 28 10:16:47 2019

Script to convert text from a DB to geometries 
and put the output into another postgres DB table 

@author: crosic
"""

import psycopg2
import configparser

#import numpy as np
import pandas as pd
import pandas.io.sql as psql

from shapely.wkt import loads

CONFIG = configparser.ConfigParser()
CONFIG.read('db.cfg')
dbset = CONFIG['DBSETTINGS']
con = psycopg2.connect(**dbset)

from sqlalchemy import create_engine

engine = create_engine('postgresql://' + dbset['user'] + ':' + dbset['password'] + '@' + dbset['host'] + '/' + dbset['database'])

def text_to_centreline(highway, fr, to): 
    if to != None:
        try:
            df = psql.read_sql(
                    "SELECT centreline_segments AS geom,  con AS confidence, notice, line_geom, oid1_geom, oid2_geom FROM crosic.text_to_centreline('{}', '{}', '{}')".format(
                            highway.replace("'", ""), fr.replace("'", ""), to.replace("'", "")), con)
        except Exception as e:

            print("------------------------")
            print("street: " + highway) 
            print(" from: " + fr)
            print("ERROR")
            print("------------------------")
            raise Exception("Error making DB call: " + str(e))
            return [highway, fr, to, "Error", df['geom'].item(), df['notice'].item(), df['line_geom'].item(), df['oid1_geom'].item(), df['oid2_geom'].item()]

    else:
    	try:
         	  df = psql.read_sql("SELECT centreline_segments AS geom,  con AS confidence, notice, line_geom, oid1_geom, oid2_geom FROM crosic.text_to_centreline('{}', '{}', {})".format(highway.replace("'", ""), fr.replace("'", ""), 'NULL'), con)
    	except Exception as e:

            print("------------------------")
            print("street: " + highway) 
            print(" from: " + fr)
            print("ERROR " + str(e))
            print("------------------------")
            raise Exception("Error in text_to_centreline: " + str(e))
            return  [highway, fr, "Error", df['geom'].item(), df['notice'].item(), df['line_geom'].item(), df['oid1_geom'].item(), df['oid2_geom'].item()]


            
    #if df['geom'].item() == None:
     #   print("------------------------")
      #  print("street: " + highway) 
      #  print(" from: " + fr)
       # print(" confidence: " + str(df['confidence'].item()))
       # print("------------------------")
        
    return [highway, fr, df['confidence'].item(),  df['geom'].item(), df['notice'].item(), df['line_geom'].item(), df['oid1_geom'].item(), df['oid2_geom'].item()]



def load_geoms(row_with_wkt, i):
    row_with_geom = row_with_wkt[:i]
    if row_with_wkt[i] != None:
        row_with_geom.append(loads(row_with_wkt[i]))
    else:
        row_with_geom.append(None)
    return row_with_geom



def get_rows(df):
    rows = []
    total_rows = df.shape[0]
    for index, row in df.iterrows():
        if index%100 == 0:
            print("Calling SQL function on index " + str(index))	
        if index == total_rows-1:
            print("Processing last row with index value of " + str(index))
        try:
            if df.shape[1] == 4:
                row_with_wkt = text_to_centreline(row[1], row[2], row[3])
        
            elif df.shape[1] == 3:
                row_with_wkt = text_to_centreline(row[1], row[2], None)
                #row_with_wkt = row_with_null[:2] + row_with_null[3:]

            rows.append([row[0]] +  row_with_wkt)
        except Exception as e:
            print("ERROR CALLING TEXT TO CENTRELINE:" + str(e))
            if df.shape[1] == 4:
                rows.append([row[0], row[1], row[2], row[3], "No Match - ERROR", None, str(e), None, None, None])
            elif df.shape[1] == 3:
                rows.append([row[0], row[1], row[2], "No Match - ERROR", None, str(e), None, None, None])
            continue
        #print(row_with_wkt)
    #print(rows)
    return pd.DataFrame(data=rows, columns=['id','street_name', 'extents',  'confidence', 'geom', 'notice', 'line_geom', 'oid1_geom', 'oid2_geom'])  #gpd.GeoDataFrame(data=rows)



def main(input_table, output_table):
    sql = "SELECT id, street_name, extents FROM {} WHERE geom is NULL".format(input_table) # + " WHERE geom_str IS NULL;"
    df = psql.read_sql(sql, con)
    # filter out unncessary columns
    df2 = df[['id', 'street_name', 'extents']].copy()
    
    # in case if there is a random row with a value missing or a row with no values
    df2 = df2.dropna(axis=0)
    
    #subset = pd.DataFrame(df2.head(200))

    try:
        geom_data = get_rows(df2)
        #geom_data.columns = ['id','street_name', 'extents',  'confidence', 'geom_str']
        
    except Exception as e:
        print("Exception: " + str(e))
        con.close()
        print(e)
        return pd.DataFrame()
    #print(geom_data)
    

    #geom_data = geom_data.loc[lambda geom_data: geom_data['geom_str'].notnull()]

    geom_data.to_sql(output_table, engine)
    
    return geom_data
    
        



#geom_data = main("crosic.posted_speed_geoms_2019_all", "posted_speed_geoms_2019_all_originally_non_matched")


geom_data = main( "posted_speed_geoms_2019_all_2", "posted_speed_geoms_2019_all_originally_unmatched_2_test_multi")


#for row in geom_data.loc[lambda geom_data: geom_data[3].isnull()].iterrows():
#    print(row)
    
    
    
con.close()
    
    

# Alcorn and oaklands do not intersect



# Albert Franck Place works if btwn1 and btwn2 are swtiched

# SELECT * FROM crosic.text_to_centreline('Amsterdam Boulevard', 'OConnor Drive and Victoria Park Avenue', NULL)  not sure 

    
# crosic.text_to_centreline('Aspenwood Drive', 'A point 110 metres north of Paynter Drive (east intersection) and a point 265 metres east of Paynter Drive (east intersection)', NULL)
# Paynter drive intersects Aspenwood twice, choosing wrong intersection  
    
    

# SELECT * FROM text_to_centreline('Park Lane', 'Park Lawn Road and ((((the west end of)))) Park Lane', NULL)
# doesnt match correctly because Lane gets corrected to Ln when the intersection file doesnt contain that abbreviation




# Chapman -> too far 








