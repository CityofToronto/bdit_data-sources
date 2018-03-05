import pandas as pd
import configparser
from psycopg2 import connect
import psycopg2.sql as pg
import pandas.io.sql as pandasql


CONFIG = configparser.ConfigParser()
CONFIG.read(r'C:\Users\alouis2\Documents\Python Scripts\db.cfg')
dbset = CONFIG['DBSETTINGS']
con = connect(**dbset)

query = ''' SELECT segments.analysis_id
               FROM bluetooth.segments
            EXCEPT
             SELECT DISTINCT f.analysis_id
               FROM ( SELECT observations.analysis_id,
                        observations.measured_timestamp
                       FROM bluetooth.observations
                      WHERE observations.measured_timestamp <= (( SELECT max(observations_1.measured_timestamp) AS max
                               FROM bluetooth.observations observations_1)) AND observations.measured_timestamp >= (( SELECT max(observations_1.measured_timestamp) - '02:00:00'::interval
                               FROM bluetooth.observations observations_1)) AND (observations.analysis_id IN ( SELECT segments.analysis_id
                               FROM bluetooth.segments))
                      ORDER BY observations.measured_timestamp DESC) f'''

df = pandasql.read_sql(pg.SQL(query), con)

broken_readers = []
for i in range(len(df['analysis_id'])):
    string = '''SELECT * FROM bluetooth.observations
                WHERE analysis_id = %d 
                ORDER BY measured_timestamp desc 
                LIMIT 1''' % list(df['analysis_id'])[i]
    row = pandasql.read_sql(pg.SQL(string), con)
    broken_readers.append(list(row.loc[0]))

broken_readers = pd.DataFrame(broken_readers, columns = list(row.columns.values))