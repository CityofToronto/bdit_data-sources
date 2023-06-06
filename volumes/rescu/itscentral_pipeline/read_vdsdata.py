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

date_start = datetime.timestamp(datetime.fromisoformat('2023-05-16'))
sql = f"SELECT * FROM public.vdsdata WHERE timestamputc >= {date_start} AND timestamputc < {date_start} + 86400"

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

            # Get volume by vehicle lengths
            #Each class stored in vehicles per hour. 65535 for null value.
            passengerVolume = struct.unpack('<H', mv[index + 7] + mv[index + 8])[0]
            volumePassengerVehiclesPerHour = None if passengerVolume == 65535 else passengerVolume
            singleUnitTrucksVolume = struct.unpack('<H', mv[index + 9] + mv[index + 10])[0]
            volumeSingleUnitTrucksPerHour = None if singleUnitTrucksVolume == 65535 else singleUnitTrucksVolume
            comboTrucksVolume = struct.unpack('<H', mv[index + 11] + mv[index + 12])[0]
            volumeComboTrucksPerHour = None if comboTrucksVolume == 65535 else comboTrucksVolume
            multiTrailerTrucksVolume = struct.unpack('<H', mv[index + 13] + mv[index + 14])[0]
            volumeMultiTrailerTrucksPerHour = None if multiTrailerTrucksVolume == 65535 else multiTrailerTrucksVolume

            # Increment index by 15 to move to the next lane
            index += 15

            result.append([lane, speedKmh, volumeVehiclesPerHour, occupancyPercent, volumePassengerVehiclesPerHour, volumeSingleUnitTrucksPerHour, volumeComboTrucksPerHour, volumeMultiTrailerTrucksPerHour])
            
    return result

#for vdslatestdata
#try #is none

#get number of lanes in the binary data stream for each row
row_lengths = list(map(lambda x: len(x) / 15, df.lanedata.values)) 

empty_rows = df[[x == 0 for x in row_lengths]]
print(f'Empty rows discarded: {empty_rows.shape[0]}')

df.drop(empty_rows.index, inplace = True) #remove empty rows

df['datetime'] = df['timestamputc'].map(lambda x: datetime.fromtimestamp(x))

#parse each column entry 
lane_data = df['lanedata'].map(parse_lane_data)


lane_data_flat = []
for sublist in lane_data:
    for item in sublist:
        lane_data_flat.append(item)

lane_data_df = pd.DataFrame(lane_data_flat, 
                         columns = ['lane', 'speedKmh', 'volumeVehiclesPerHour', 'occupancyPercent', 'volumePassengerVehiclesPerHour', 'volumeSingleUnitTrucksPerHour', 'volumeComboTrucksPerHour', 'volumeMultiTrailerTrucksPerHour'])

#expand rows 
n_rows = lane_data.map(len)
lane_data_df['join_col'] = df.index.repeat(n_rows) #use original index as a join column

final = df[['divisionid','datetime','vdsid']].merge(
    lane_data_df, how = 'left', left_index = True, right_on = 'join_col')

print(final.head(20))

#todo: remove empty rows prior to processing
#time to improve speed further
#what is total volume?