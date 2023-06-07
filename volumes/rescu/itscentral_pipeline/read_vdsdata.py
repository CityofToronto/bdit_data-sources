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
FROM public.vdsdata AS d
JOIN public.vdsconfig AS c ON
    d.vdsid = c.vdsid
    AND d.divisionid = c.divisionid
    AND to_timestamp(d.timestamputc) >= c.starttimestamputc
    AND (
        to_timestamp(d.timestamputc) <= c.endtimestamputc
        OR c.endtimestamputc IS NULL) --no end date
WHERE 
    timestamputc >= {date_start}
    AND timestamputc < {date_end} + 86400;'''
# AND sourceid = 'de0020dwg';''' #for testing
 
with con: 
    df = pd.read_sql(sql, con)

print(f"Number of rows: {df.shape[0]}")


#get number of lanes in the binary data stream for each row
row_lengths = df['lanedata'].map(len)
empty_rows = df[row_lengths == 0]

if empty_rows.empty is False:
    print(f'Rows with empty lanedata discarded: {empty_rows.shape[0]}')
else: 
    print(f'No empty rows discarded.')

df.drop(empty_rows.index, inplace = True) #remove empty rows

df['datetime'] = df['timestamputc'].map(datetime.fromtimestamp)
df['datetime_15min'] = df['timestamputc'].map(lambda a: 60 * 15 * (a // (60 * 15))).map(datetime.fromtimestamp) #very fast 15min binning

#parse each `lanedata` column entry 
lane_data = df['lanedata'].map(parse_lane_data)

#flatten the nested list structure
lane_data_flat = [item for sublist in lane_data for item in sublist]

#convert list structure to df
cols = ['lane', 'speedKmh', 'volumeVehiclesPerHour', 'occupancyPercent']
#removed columns:, 'volumePassengerVehiclesPerHour', 'volumeSingleUnitTrucksPerHour', 'volumeComboTrucksPerHour', 'volumeMultiTrailerTrucksPerHour'])
lane_data_df = pd.DataFrame(lane_data_flat, columns = cols) 

#repeat original index based on number of rows in result as a join column
n_rows = lane_data.map(len)
lane_data_df.set_index(df.index.repeat(n_rows), inplace=True)

#join with other columns on index 
final = df[['detector_id', 'datetime', 'datetime_15min']].join(lane_data_df)

print(final.head(20))

#aggregate from 20s hourly rate to 15 minute volume
#this method of aggregation assumes missing bins are zeros. 
def rate_to_volume(x): 
    return x.sum() / 4 / 45 #/4 is for hourly to 15 minute volume. /45 is for 45 20sec bins per 15 minutes (assumes missing bins are zeros).   

summary = final.groupby('datetime_15min').agg(
        volume_15min = ('volumeVehiclesPerHour', rate_to_volume),
        bin_count = ('datetime_15min', 'count'))

with pd.option_context("display.max_rows", 1000): 
    print(summary)

print(f"Total daily volume: {summary['volume_15min'].sum():.0f}")

volumes_15min = final.groupby(['detector_id', 'datetime_15min']).agg(
        volume_15min = ('volumeVehiclesPerHour', rate_to_volume),
        bin_count = ('datetime_15min', 'count')).reset_index()

print(f"Total daily volume: {volumes_15min['volume_15min'].sum():.2f}")


#look at sensor daily totals: 
with pd.option_context("display.max_rows", 1000): 
    print(volumes_15min.groupby(['detector_id']).agg(
            volume_15min = ('volume_15min', sum)).head(1000))

volumes_15min.groupby(['detector_id']).agg(
            volume_15min = ('volume_15min', sum),
            bins = ('datetime_15min', 'count')).to_csv('/home/gwolofs/rescu_itscentral/sample_data.csv')

#look at a single sensor (one which doesn't match total!)
#rawest data:
with pd.option_context("display.max_rows", 1000): 
    print(final[final['detector_id'] == 'de0041deg'])

#15 minute data: 
#overall difference for the day is 14. 
with pd.option_context("display.max_rows", 1000): 
    print(volumes_15min[volumes_15min['detector_id'] == 'de0041deg'].reset_index())


#are there both zeros and nulls? should we treat them differently?
total_raw_rows = empty_rows.shape[0] + lane_data.shape[0]
print(f"{100 * empty_rows.shape[0] / total_raw_rows:.3f}% rows have null lanedata.")

#distinct bins expected: 
bins_expected = final[['detector_id', 'lane']].drop_duplicates().shape[0] * 24 * 60 * 3
print(f"{100 * final.shape[0] / bins_expected:.3f}% of expected bins present based on # detectors and lanes.")

#bins expected but only highways & not ramps
only_highways = final[[x[0] == 'D' and x[-1] != 'R' for x in final['detector_id']]]
bins_expected = only_highways[['detector_id', 'lane']].drop_duplicates().shape[0] * 24 * 60 * 3
print(f"{100 * only_highways.shape[0] / bins_expected:.3f}% of expected bins present based on # detectors and lanes for only highways (not including ramps).")

