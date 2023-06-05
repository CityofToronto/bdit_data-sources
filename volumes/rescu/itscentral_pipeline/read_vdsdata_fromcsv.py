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

date_start = datetime.timestamp(datetime.fromisoformat('2023-05-24'))
#sql = f"SELECT COUNT(*) FROM public.vdsdata WHERE timestamputc >= {date_start} AND timestamputc < {date_start} + 86400"
sql = f"SELECT * FROM public.vdsdatalatest"

with con: 
    df = pd.read_sql(sql, con)

# Parse lane data
def parse_lane_data(laneData):
    result = []

    with memoryview(laneData) as mv:
        index = 0
        while index < len(mv):
            # Get lane
            lane = mv[index]

            # Get speed
            #Stored in km/h * 100. 65535 for null value. Convert 0 to null to maintain backward compatibility
            speed = struct.unpack_from('<H', mv, index + 1)[0] #<H denotes stored in little-endian format 
            speedKmh = None if speed == 65535 or speed == 0 else speed / 100.0

            # Get volume
            #Stored in vehicles per hour. 65535 for null value.
            volume = struct.unpack_from('<H', mv, index + 3)[0]  
            volumeVehiclesPerHour = None if volume == 65535 else volume

            # Get occupancy
            #Stored in percent * 100. 65535 for null value.
            occupancy = struct.unpack_from('<H', mv, index + 5)[0]  
            occupancyPercent = None if occupancy == 65535 else occupancy / 100.0

            # Get volume by vehicle lengths
            #Each class stored in vehicles per hour. 65535 for null value.
            passengerVolume = struct.unpack_from('<H', mv, index + 7)[0]  
            volumePassengerVehiclesPerHour = None if passengerVolume == 65535 else passengerVolume
            singleUnitTrucksVolume = struct.unpack_from('<H', mv, index + 9)[0]  
            volumeSingleUnitTrucksPerHour = None if singleUnitTrucksVolume == 65535 else singleUnitTrucksVolume
            comboTrucksVolume = struct.unpack_from('<H', mv, index + 11)[0]  
            volumeComboTrucksPerHour = None if comboTrucksVolume == 65535 else comboTrucksVolume
            multiTrailerTrucksVolume = struct.unpack_from('<H', mv, index + 13)[0]  
            volumeMultiTrailerTrucksPerHour = None if multiTrailerTrucksVolume == 65535 else multiTrailerTrucksVolume

            # Increment index by 15 to move to the next lane
            index += 15

            result.append([lane, speedKmh, volumeVehiclesPerHour, occupancyPercent, volumePassengerVehiclesPerHour, volumeSingleUnitTrucksPerHour, volumeComboTrucksPerHour, volumeMultiTrailerTrucksPerHour])
            
    return result

# Get the column data from your source (e.g., a list or a NumPy array)
#df = pd.read_csv('/home/gwolofs/rescu_itscentral/vdsdata_sample.csv')

#for vdslatestdata
empty_rows = df[df['lanedata'] == '\\x']
print(f'Empty rows discarded: {empty_rows.shape[0]}')

df.drop(empty_rows.index, inplace = True) #remove empty rows

df['datetime'] = df['timestamputc'].map(lambda x: datetime.fromtimestamp(x))


df['lanedata'] = df['lanedata'].map(lambda a: bytes.fromhex(a[2:]))

#parse each column entry 
df['lanedata'] = df['lanedata'].map(parse_lane_data)

#convert each entry to a dataframe 
cols = ['lane', 'speedKmh', 'volumeVehiclesPerHour', 'occupancyPercent', 'volumePassengerVehiclesPerHour', 'volumeSingleUnitTrucksPerHour', 'volumeComboTrucksPerHour', 'volumeMultiTrailerTrucksPerHour']
df['lanedata'] = df['lanedata'].map(lambda a: pd.DataFrame(a, columns = cols))

#expand rows 
lane_data = pd.concat(df.lanedata.values)
lane_data['join_col'] = df.index.repeat(df.lanedata.str.len()) #use original index as a join column

final = df[['divisionid','datetime','vdsid']].merge(
    lane_data, how = 'left', left_index = True, right_on = 'join_col')


