from pathlib import Path
import configparser
from psycopg2 import connect
import struct
import pandas as pd
from datetime import datetime

# Parse lane data
def parse_lane_data(laneData):
    result = []

    with memoryview(laneData) as mv:
        index = 0
        while index < len(mv):
            # Get lane
            lane = mv[index][0]

            # Get speed
            #Stored in km/h * 100. 65535 for null value. Convert 0 to null to maintain backward compatibility
            speed = struct.unpack('<H', mv[index + 1] + mv[index + 2])[0] #>H denotes stored in big-endian format (PostGreSQL uses big-endian)
            speedKmh = None if speed == 65535 or speed == 0 else speed / 100.0

            # Get volume
            #Stored in vehicles per hour. 65535 for null value.
            volume = struct.unpack('<H', mv[index + 3] + mv[index + 4])[0]
            volumeVehiclesPerHour = None if volume == 65535 else volume

            # Get occupancy
            #Stored in percent * 100. 65535 for null value.
            occupancy = struct.unpack('<H', mv[index + 5] + mv[index + 6])[0]
            occupancyPercent = None if occupancy == 65535 else occupancy / 100.0

            # Increment index by 15 to move to the next lane
            index += 15

            result.append([lane, speedKmh, volumeVehiclesPerHour, occupancyPercent]) 

    return result


CONFIG = configparser.ConfigParser()
CONFIG.read(str(Path.home().joinpath('db.cfg'))) #Creates a path to your db.cfg file
dbset = CONFIG['DBSETTINGS']
con = connect(**dbset)

date_start = datetime.timestamp(datetime.fromisoformat('2023-05-17'))
date_end = datetime.timestamp(datetime.fromisoformat('2023-05-17'))

sql = f'''SELECT 
    upper(c.sourceid) AS detector_id,
    d.timestamputc,
    d.vdsid,
    d.lanedata
FROM vdsdata AS d
JOIN vdsconfig AS c ON
    d.vdsid = c.vdsid
    AND d.divisionid = c.divisionid
    AND to_timestamp(d.timestamputc) >= c.starttimestamputc
    AND (
        to_timestamp(d.timestamputc) <= c.endtimestamputc
        OR c.endtimestamputc IS NULL) --no end date
JOIN EntityLocationLatest AS e ON
    c.vdsid = e.entityid
    AND c.divisionid = e.divisionid
WHERE 
    timestamputc >= {date_start}
    AND timestamputc < {date_end} + 86400
    AND e.entityid IS NOT NULL --we only have locations for these ids
    AND d.divisionid = 2 --other is 8001 which are signals
    AND substring(sourceid, 1, 3) <> 'BCT' --bluecity.ai sensors; '''
# AND sourceid = 'de0020dwg';''' #for testing

detector_sql = '''SELECT 
    c.divisionid,
    c.vdsid,
    upper(c.sourceid) AS detector_id,
    c.starttimestamputc,
    c.endtimestamputc,
    c.lanes,
    c.hasgpsunit,
    c.managementurl,
    c.description,
    c.fssdivisionid,
    c.fssid,
    c.rtmsfromzone,
    c.rtmstozone,
    c.detectortype,
    c.createdby,
    c.createdbystaffid,
    c.signalid,
    c.signaldivisionid,
    c.movement,
    e.entitytype,                  
    e.locationtimestamputc,        
    e.latitude,                    
    e.longitude,                   
    e.altitudemetersasl,           
    e.headingdegrees,              
    e.speedkmh,                    
    e.numsatellites,               
    e.dilutionofprecision,         
    e.mainroadid,                  
    e.crossroadid,                 
    e.secondcrossroadid,           
    e.mainroadname,                
    e.crossroadname,               
    e.secondcrossroadname,         
    e.streetnumber,                
    e.offsetdistancemeters,        
    e.offsetdirectiondegrees,      
    e.locationsource,              
    e.locationdescriptionoverwrite
FROM vdsconfig AS c 
JOIN EntityLocationLatest AS e ON 
    c.vdsid = e.entityid 
    AND c.divisionid = e.divisionid 
ORDER BY c.vdsid'''

with con: 
    df = pd.read_sql(sql, con)
    detector_inventory = pd.read_sql(detector_sql, con)

print(f"Number of rows: {df.shape[0]}")


#get number of lanes in the binary data stream for each row
row_lengths = df['lanedata'].map(len)
empty_rows = df[row_lengths == 0]

if empty_rows.empty is False:
    print(f'Rows with empty lanedata: {empty_rows.shape[0]}')
else: 
    print(f'No empty rows discarded.')

#drop empty rows
df.drop(empty_rows.index, inplace = True) #remove empty rows

df['datetime'] = df['timestamputc'].map(datetime.fromtimestamp)
df['datetime_15min'] = df['timestamputc'].map(lambda a: 60 * 15 * (a // (60 * 15))).map(datetime.fromtimestamp) #very fast 15min binning

#parse each `lanedata` column entry 
lane_data = df['lanedata'].map(parse_lane_data)
n_rows = lane_data.map(len)

#flatten the nested list structure
lane_data = [item for sublist in lane_data for item in sublist]

#convert list structure to df
cols = ['lane', 'speedKmh', 'volumeVehiclesPerHour', 'occupancyPercent'] #removed columns:, #, 'volumePassengerVehiclesPerHour', 'volumeSingleUnitTrucksPerHour', 'volumeComboTrucksPerHour', 'volumeMultiTrailerTrucksPerHour']
lane_data_df = pd.DataFrame(lane_data, columns = cols) 

#repeat original index based on number of rows in result as a join column
lane_data_df.set_index(df.index.repeat(n_rows), inplace=True)

#join with other columns on index 
raw_20sec = df[['detector_id', 'datetime', 'datetime_15min']].join(lane_data_df)

print(raw_20sec.head(20))

#aggregate from 20s hourly rate to 15 minute volume
#this method of aggregation assumes missing bins are zeros. 
def rate_to_volume(x): 
    return x.sum() / 4 / 45 #/4 is for hourly to 15 minute volume. /45 is for 45 20sec bins per 15 minutes (assumes missing bins are zeros).   

volumes_15min = raw_20sec.groupby(['detector_id', 'datetime_15min']).agg(
        volume_15min = ('volumeVehiclesPerHour', rate_to_volume),
        bin_count = ('datetime_15min', 'count')).reset_index()