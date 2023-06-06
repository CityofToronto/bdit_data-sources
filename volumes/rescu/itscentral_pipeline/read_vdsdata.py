from pathlib import Path
import configparser
from psycopg2 import connect
import struct
import pandas as pd
from datetime import datetime

CONFIG = configparser.ConfigParser()
CONFIG.read(str(Path.home().joinpath('db.cfg'))) #Creates a path to your db.cfg file
dbset = CONFIG['DBSETTINGS']
con = connect(**dbset)

date_start = datetime.timestamp(datetime.fromisoformat('2023-05-10'))
sql = f'''SELECT c.sourceid AS detector_id, d.divisionid, d.timestamputc, d.vdsid, d.lanedata 
FROM public.vdsdata AS d
JOIN public.vdsconfig AS c ON 
    d.vdsid = c.vdsid 
    AND d.divisionid = c.divisionid --necessary?
WHERE timestamputc >= {date_start} 
  AND timestamputc < {date_start} + 86400;'''
# AND sourceid = 'de0020dwg';''' #for testing
 
with con: 
    df = pd.read_sql(sql, con)

print(f"Number of rows: {df.shape[0]}")

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

            # Get volume by vehicle lengths - these columns are empty 
            #Each class stored in vehicles per hour. 65535 for null value.
            #passengerVolume = struct.unpack('<H', mv[index + 7] + mv[index + 8])[0]
            #volumePassengerVehiclesPerHour = None if passengerVolume == 65535 else passengerVolume
            #singleUnitTrucksVolume = struct.unpack('<H', mv[index + 9] + mv[index + 10])[0]
            #volumeSingleUnitTrucksPerHour = None if singleUnitTrucksVolume == 65535 else singleUnitTrucksVolume
            #comboTrucksVolume = struct.unpack('<H', mv[index + 11] + mv[index + 12])[0]
            #volumeComboTrucksPerHour = None if comboTrucksVolume == 65535 else comboTrucksVolume
            #multiTrailerTrucksVolume = struct.unpack('<H', mv[index + 13] + mv[index + 14])[0]
            #volumeMultiTrailerTrucksPerHour = None if multiTrailerTrucksVolume == 65535 else multiTrailerTrucksVolume

            # Increment index by 15 to move to the next lane
            index += 15

            result.append([lane, speedKmh, volumeVehiclesPerHour, occupancyPercent]) #, volumePassengerVehiclesPerHour, volumeSingleUnitTrucksPerHour, volumeComboTrucksPerHour, volumeMultiTrailerTrucksPerHour])
            
    return result

#get number of lanes in the binary data stream for each row
row_lengths = df['lanedata'].map(len)
empty_rows = df[row_lengths == 0]

if empty_rows.empty is False:
    print(f'Rows with empty lanedata discarded: {empty_rows.shape[0]}')
else: 
    print(f'No empty rows discarded.')

df.drop(empty_rows.index, inplace = True) #remove empty rows

df['datetime'] = df['timestamputc'].map(datetime.fromtimestamp)

#parse each `lanedata` column entry 
lane_data = df['lanedata'].map(parse_lane_data)

#flatten the nested list structure
lane_data_flat = []
for sublist in lane_data:
    for item in sublist:
        lane_data_flat.append(item)

#convert list structure to df
lane_data_df = pd.DataFrame(lane_data_flat, 
                         columns = ['lane', 'speedKmh', 'volumeVehiclesPerHour', 'occupancyPercent']) 
#removed columns:, 'volumePassengerVehiclesPerHour', 'volumeSingleUnitTrucksPerHour', 'volumeComboTrucksPerHour', 'volumeMultiTrailerTrucksPerHour'])

#repeat original index based on number of rows in result as a join column
n_rows = lane_data.map(len)
lane_data_df['join_col'] = df.index.repeat(n_rows) 

#join with other columns
final = df[['detector_id', 'divisionid','datetime','vdsid']].merge(
    right = lane_data_df, how = 'left', left_index = True, right_on = 'join_col')
final.drop('join_col', inplace = True, axis = 1)

print(final.head(20))

final['datetime_bin'] = [x.floor(freq='15T') for x in final['datetime']] #pandas timestamp.floor 15T = 15 minutes

#aggregate from 20s hourly rate to 15 minute volume
#this method of aggregation assumes missing bins are zeros. 
def rate_to_volume(x): 
    return x.sum() / 4 / 45 #/4 is for hourly to 15 minute volume. /45 is for 45 20sec bins per 15 minutes (assumes missing bins are zeros).   

summary = final.groupby('datetime_bin').agg(
        volume_15min = ('volumeVehiclesPerHour', rate_to_volume),
        bin_count = ('datetime_bin', 'count'))

with pd.option_context("display.max_rows", 1000): 
    print(summary)

print(f"Total daily volume: {summary['volume_15min'].sum():.0f}")

volumes_15min = final.groupby(['detector_id', 'datetime_bin']).agg(
        volume_15min = ('volumeVehiclesPerHour', rate_to_volume),
        bin_count = ('datetime_bin', 'count'))


""" #only volumeVehiclesPerHour and occupancyPercent have any data. 
final.agg({
    'volumeVehiclesPerHour':'sum', 
    'occupancyPercent':'sum', 
    'volumePassengerVehiclesPerHour':'sum', 
    'volumeSingleUnitTrucksPerHour':'sum', 
    'volumeComboTrucksPerHour':'sum', 
    'volumeMultiTrailerTrucksPerHour':'sum'}) """