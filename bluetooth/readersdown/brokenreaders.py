# import relevant modules

import matplotlib.pyplot as plt
import pandas as pd
import configparser
from psycopg2 import connect
import psycopg2.sql as pg
import pandas.io.sql as pandasql
import io
from os.path import expanduser
import os

# connect to database 

def con_to_db(cfg_path): 
    CONFIG = configparser.ConfigParser()
    CONFIG.read(r'%s' % cfg_path)
    dbset = CONFIG['DBSETTINGS']
    con = connect(**dbset)

###################################################################################################    
    
con_to_db('your cfg file path here')

###################################################################################################

def find_badroutes():
    
    # finds instances of potentially 'bad routes', i.e. routes who may have readers that are down. 
    # returns dataframe with these routes
    
    string = ''' SELECT segments.analysis_id
               FROM bluetooth.segments
            EXCEPT
             SELECT DISTINCT f.analysis_id
               FROM (SELECT observations.analysis_id,
                        observations.measured_timestamp
                       FROM bluetooth.observations
                      WHERE observations.measured_timestamp <= (( SELECT max(observations_1.measured_timestamp) AS max
                               FROM bluetooth.observations observations_1)) AND observations.measured_timestamp >= (( SELECT max(observations_1.measured_timestamp) - '02:00:00'::interval
                               FROM bluetooth.observations observations_1)) AND (observations.analysis_id IN ( SELECT segments.analysis_id
                               FROM bluetooth.segments))
                      ORDER BY observations.measured_timestamp DESC) f'''

    df = pandasql.read_sql(pg.SQL(string), con)
    badroutes = []
    
    for i in range(len(df['analysis_id'])):
        string = '''SELECT analysis_id, startpoint_name, endpoint_name,  measured_timestamp as last_active FROM bluetooth.observations
                    WHERE analysis_id = %d 
                    ORDER BY measured_timestamp desc 
                    LIMIT 1''' % list(df['analysis_id'])[i]
        row = pandasql.read_sql(pg.SQL(string), con)
        badroutes.append(list(row.loc[0]))
    badroutes = pd.DataFrame(badroutes, columns = list(row.columns.values)).sort_values(by='last_active').reset_index(drop=True)
    return badroutes
    
badroutes = find_badroutes()

def find_goodroutes():
    
    # finds all 'good routes', i.e. routes with seemingly working readers
    
    string2 = '''SELECT analysis_id, startpoint_name, endpoint_name
                FROM bluetooth.observations
                    WHERE observations.measured_timestamp <= (( SELECT max(observations_1.measured_timestamp) AS max
                    FROM bluetooth.observations observations_1)) AND observations.measured_timestamp >= (( SELECT max(observations_1.measured_timestamp) - '02:00:00'::interval
                    FROM bluetooth.observations observations_1)) AND (observations.analysis_id IN ( SELECT segments.analysis_id
                    FROM bluetooth.segments))
                    GROUP BY analysis_id, startpoint_name, endpoint_name;'''

    df2 = pandasql.read_sql(pg.SQL(string2), con)
    return df2

goodroutes = find_goodroutes()
    
def find_brokenreaders(badroutes, goodroutes):
    
    ### INPUTS
    # badroutes: a dataframe returned by `badroutes()` 
    # goodroutes: a dataframe returned by `goodroutes()`
    
    # returns a final dataframe contianing all individual broken readers, the last time they were 
    # active, and also the routes affected by those broken readers. 
    # Moreover, it puts this dataframe into a csv for the user, into their documents folder
    
    final = []
    for i in range(len(badroutes)): 
        if (badroutes['startpoint_name'].values[i] not in goodroutes['startpoint_name'].values \
        and badroutes['startpoint_name'].values[i] not in goodroutes['endpoint_name'].values):
            final.append(badroutes['startpoint_name'].values[i])
    for i in range(len(badroutes)):
        if badroutes['endpoint_name'].values[i] not in goodroutes['startpoint_name'].values \
        and badroutes['endpoint_name'].values[i] not in goodroutes ['endpoint_name'].values:
            final.append(badroutes['endpoint_name'].values[i])
    final = list(set(final))
    for i in final: 
        j = (len(badroutes))-1
        while j < len(badroutes):
            if i == badroutes['startpoint_name'].values[j]:
                final[final.index(i)] = ([i, badroutes['last_active'].values[j]])
                break
            elif i == badroutes['endpoint_name'].values[j]:
                final[final.index(i)] = ([i, badroutes['last_active'].values[j]])
                break
            else:
                j = j-1
    for i in final:
        final[final.index(i)].append([])
        starts = list(badroutes['startpoint_name'].values)
        ends = list(badroutes['endpoint_name'].values)
        j = 0 
        while j < len(starts):
            if i[0] == starts[j]: 
                final[final.index(i)][2].append('' + starts[j] + ' to ' + ends[j] + '')
                j += 1
            else:
                j += 1
        j = 0
        while j < len(ends):
            if i[0] == ends[j]:
                final[final.index(i)][2].append('' + starts[j] + ' to ' + ends[j] + '')
                j += 1
            else:
                j += 1
        final[final.index(i)][2] = (", ".join(final[final.index(i)][2]))
    for i in range(len(badroutes)): 
        if (badroutes['startpoint_name'].values[i] in goodroutes['startpoint_name'].values or \
            badroutes['startpoint_name'].values[i] in goodroutes['endpoint_name'].values)\
        and (badroutes['endpoint_name'].values[i] in goodroutes['startpoint_name'].values or \
             badroutes['endpoint_name'].values[i] in goodroutes['endpoint_name'].values):
            final.append(['-', '-', badroutes['startpoint_name'].values[i]])
    
    broken_readers = pd.DataFrame(final, columns = ['Reader', 'Last Active', 'Routes Affected'])
    home = expanduser("~")
    path=  home + '\\Documents'
    broken_readers.to_csv(os.path.join(path,r'broken_readers.csv'), index = False)
    return broken_readers

broken_readers = find_brokenreaders(badroutes, goodroutes)