import os
import logging
import pandas as pd
from numpy import nan
from psycopg import sql, Error
import struct
from datetime import datetime, timedelta
import pytz

SQL_DIR = os.path.join(os.path.dirname(os.path.abspath(os.path.dirname(__file__))), 'sql')

LOGGER = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

def check_dst(start_date):
    start = datetime.strptime(start_date, '%Y-%m-%d')
    tz_1 = start.astimezone().tzname()
    tz_2 = (start + timedelta(days = 1)).astimezone().tzname()
    if tz_1 == 'EDT' and tz_2 == 'EST':
        LOGGER.info(f"EDT -> EST time change occured today.")
    else:
        LOGGER.info(f"Normal day.")
    return tz_1 == 'EDT' and tz_2 == 'EST'

def fetch_and_insert_data(select_conn, insert_conn, select_query, insert_query, table_name, batch_size = 100000):
    #generic function to pull and insert data using different connections and queries.
    batch=1
    running_total=0
    try:
        with select_conn.get_conn() as con, con.cursor() as cur:
            con.autocommit = True
            LOGGER.info(f"Fetching {table_name}")
            cur.execute(select_query)
            data = cur.fetchmany(batch_size)
            while not len(data) == 0:
                running_total += len(data)
                LOGGER.info(f"Batch {batch} -- {len(data)} rows fetched from {table_name}.")
                insert_data(insert_conn, insert_query, table_name, data)
                data = cur.fetchmany(batch_size)
                batch += 1
            LOGGER.info(f"Total rows fetched: {running_total}.")
    except Error as exc:
        LOGGER.critical(f"Error fetching from {table_name}.")
        LOGGER.critical(exc)
        raise Exception()

def insert_data(conn, query, table_name, data):
    #generic function to insert data
    try:
        with conn.get_conn() as con, con.cursor() as cur:
            # Insert cleaned data into the database
            LOGGER.info(f"Inserting into {table_name}.")
            cur.executemany(query, data)
            LOGGER.info(f"Inserted {len(data)} rows into {table_name}.")
    except Error as exc:
        LOGGER.critical(f"Error inserting into {table_name}.")
        LOGGER.critical(exc)
        raise Exception()

def pull_raw_vdsdata(rds_conn, itsc_conn, start_date):
#pulls data from ITSC table `vdsdata` and inserts into RDS table `vds.raw_vdsdata`

    # Pull raw data from Postgres database
    if not check_dst(start_date):
        fpath = os.path.join(SQL_DIR, 'select/select-itsc_vdsdata.sql')
    else:
        fpath = os.path.join(SQL_DIR, 'select/select-itsc_vdsdata_dst_safe.sql')
    with open(fpath, 'r', encoding="utf-8") as file:
        raw_sql = sql.SQL(file.read()).format(
            start = sql.Literal(start_date + " 00:00:00 EST5EDT")
        )

    fpath = os.path.join(SQL_DIR, 'insert/insert_raw_vdsdata.sql')
    with open(fpath, 'r', encoding="utf-8") as file:
        insert_query = sql.SQL(file.read())
    
    batch_size = 100000
    
    #pull data in batches and transform + insert.
    try:
        with itsc_conn.get_conn() as con, con.cursor() as cur:
            con.autocommit = True
            LOGGER.info("Fetching %s", 'vdsvehicledata')
            cur.execute(raw_sql)
            data = cur.fetchmany(batch_size)
            batch = 1
            running_total_1=0
            running_total_2=0
            while not len(data) == 0:
                running_total_1 += len(data)
                LOGGER.info(f"Batch {batch} -- {len(data)} rows fetched from vdsdata.")
                data = pd.DataFrame(data)
                data.columns=[x.name for x in cur.description]

                # Transform raw data
                transformed_data = transform_raw_data(data)
                transformed_data = transformed_data.replace(nan, None)

                #check for duplicates. Keep first occurance and print/discard others. 
                dups = transformed_data.duplicated(subset=['divisionid', 'vdsid', 'datetime', 'lane'], keep = 'first') 
                if dups.sum() > 0:
                    print("Duplicate values found in vdsdata discarded:")
                    print(transformed_data[dups])
                    transformed_data = transformed_data[~dups] 
                running_total_2 += transformed_data.shape[0]

                #insert data 
                data_tuples = [tuple(x) for x in transformed_data.to_numpy()] #convert df back to tuples for inserting
                insert_data(rds_conn, insert_query, 'raw_vdsdata', data_tuples)

                #fetch next batch
                data = cur.fetchmany(batch_size)
                batch += 1
            LOGGER.info(f"Total rows fetched: {running_total_1}. Total rows inserted (lanedata expanded): {running_total_2}.")
    except Error as exc:
            LOGGER.critical("Error fetching from %s.", 'vdsdata')
            LOGGER.critical(exc)
            raise Exception()
    
def pull_raw_vdsvehicledata(rds_conn, itsc_conn, start_date): 
#pulls data from ITSC table `vdsvehicledata` and inserts into RDS table `vds.raw_vdsvehicledata`
#contains individual vehicle activations from highway sensors (speed, length)

    if not check_dst(start_date):
        fpath = os.path.join(SQL_DIR, 'select/select-itsc_vdsvehicledata.sql')
    else:
        fpath = os.path.join(SQL_DIR, 'select/select-itsc_vdsvehicledata_dst_safe.sql')

    with open(fpath, 'r', encoding='utf-8') as file:
        raw_sql = sql.SQL(file.read())
        raw_sql = raw_sql.format(
            start = sql.Literal(start_date + " 00:00:00 EST5EDT")
        )

    insert_query = sql.SQL("""INSERT INTO vds.raw_vdsvehicledata (
                                    division_id, vds_id, dt, lane, sensor_occupancy_ds, speed_kmh, length_meter
                                    ) VALUES (%s, %s, %s, %s, %s, %s, %s);""")

    fetch_and_insert_data(select_conn=itsc_conn, 
                          insert_conn=rds_conn,
                          select_query=raw_sql,
                          insert_query=insert_query,
                          table_name='vdsvehicledata',
                          batch_size=1000000
                          )


def pull_detector_inventory(rds_conn, itsc_conn):
#pull the detector inventory table (`vdsconfig`) from ITS Central and insert into RDS `vds.vdsconfig` as is. 
#very small table so OK to pull entire table daily. 

    # Pull data from the detector_inventory table
    fpath = os.path.join(SQL_DIR, 'select/select-itsc_vdsconfig.sql')
    with open(fpath, 'r', encoding='utf-8') as file:
        detector_sql = sql.SQL(file.read())

    # upsert data
    fpath = os.path.join(SQL_DIR, 'insert/insert_vdsconfig.sql')
    with open(fpath, 'r', encoding='utf-8') as file:
        insert_query = sql.SQL(file.read())

    fetch_and_insert_data(select_conn=itsc_conn,
                            insert_conn=rds_conn,
                            select_query=detector_sql,
                            insert_query=insert_query,
                            table_name='vdsconfig'
                            )

def pull_entity_locations(rds_conn, itsc_conn):
#pull the detector locations table (`entitylocations`) from ITS Central and insert new rows into RDS `vds.entity_locations`.
#very small table so OK to pull entire table daily. 

    # Pull data from the detector_inventory table
    fpath = os.path.join(SQL_DIR, 'select/select-itsc_entitylocations.sql')
    with open(fpath, 'r', encoding='utf-8') as file:
        entitylocation_sql = sql.SQL(file.read())

    # upsert data
    fpath = os.path.join(SQL_DIR, 'insert/insert_entity_locations.sql')
    with open(fpath, 'r', encoding='utf-8') as file:
        insert_query = sql.SQL(file.read())

    fetch_and_insert_data(select_conn=itsc_conn,
                          insert_conn=rds_conn,
                          select_query=entitylocation_sql,
                          insert_query=insert_query,
                          table_name='entitylocations'
                          )

def pull_commsdeviceconfig(rds_conn, itsc_conn):
#pull the detector locations table (`commdeviceconfig`) from ITS Central and insert new rows into RDS `vds.config_comms_device`.
#very small table so OK to pull entire table daily. 

    # Pull data from the detector_inventory table
    fpath = os.path.join(SQL_DIR, 'select/select-itsc_commdeviceconfig.sql')
    with open(fpath, 'r', encoding='utf-8') as file:
        commdevice_sql = sql.SQL(file.read())

    fpath = os.path.join(SQL_DIR, 'insert/insert_commdeviceconfig.sql')
    with open(fpath, 'r', encoding='utf-8') as file:
        # upsert data
        insert_query = sql.SQL(file.read())

    fetch_and_insert_data(
        select_conn=itsc_conn,
        insert_conn=rds_conn,
        select_query=commdevice_sql,
        insert_query=insert_query,
        table_name='config_comms_device'
    )

def parse_lane_data(laneData):
# Parse binary vdsdata.lanedata column
# Function adapted from SF's C# code stored at: "K:\tra\GM Office\Big Data Group\Data Sources\VDS\RE Back-end Connection to ITS Central data.msg"

    result = []
    
    i = 0 #index within laneData
    while i < len(laneData):
        # Get lane
        lane = laneData[i] #single byte
        # Get speed
        #Stored in km/h * 100. Null value represented by 65535. Convert 0 to null to maintain backward compatibility
        speed = struct.unpack('<H', laneData[i+1:i+3])[0] #two bytes. '<H' denotes stored in little-endian format
        speedKmh = None if speed == 65535 or speed == 0 else speed / 100.0
        
        # Get volume
        #Stored in vehicles per hour. Null value represented by 65535.
        volume = struct.unpack('<H', laneData[i+3:i+5])[0] #two bytes
        volumeVehiclesPerHour = None if volume == 65535 else volume
        
        # Get occupancy
        #Stored in percent * 100. Null value represented by 65535.
        occupancy = struct.unpack('<H', laneData[i+5:i+7])[0] #two bytes
        occupancyPercent = None if occupancy == 65535 else occupancy / 100.0
        
        #these columns were included in the example code but are empty in our data: 
        #Each class stored in vehicles per hour. 65535 for null value.
        #passengerVolume = struct.unpack('<H', laneData[i+7:i+9])
        #volumePassengerVehiclesPerHour = None if passengerVolume == 65535 else passengerVolume
        #singleUnitTrucksVolume = struct.unpack('<H', laneData[i+9:i+11])
        #volumeSingleUnitTrucksPerHour = None if singleUnitTrucksVolume == 65535 else singleUnitTrucksVolume
        #comboTrucksVolume = struct.unpack('<H', laneData[i+11:i+13])
        #volumeComboTrucksPerHour = None if comboTrucksVolume == 65535 else comboTrucksVolume
        #multiTrailerTrucksVolume = struct.unpack('<H', laneData[i+13:i+15])
        #volumeMultiTrailerTrucksPerHour = None if multiTrailerTrucksVolume == 65535 else multiTrailerTrucksVolume
        
        # Increment i by 15 to move to the next lane
        i += 15
        
        result.append([lane, speedKmh, volumeVehiclesPerHour, occupancyPercent])
                    #Extra columns, not used:, volumePassengerVehiclesPerHour, volumeSingleUnitTrucksPerHour, volumeComboTrucksPerHour, volumeMultiTrailerTrucksPerHour])
            
    return result

def transform_raw_data(df):
#transform vdsdata for inserting into RDS.

    #function to import int (UTC) timestamp in correct EST5EDT timezone. 
    UTC_to_EDTEST = lambda a: datetime.fromtimestamp(a, tz = pytz.timezone("EST5EDT"))

    df['datetime'] = df['timestamputc'].map(UTC_to_EDTEST) #convert from integer to timestamp
    df['datetime'] = df['datetime'].dt.tz_localize(None) #remove timezone before inserting into no tz column

    floor_15 = lambda a: 60 * 15 * (a // (60 * 15)) #very fast 15min binning using integer dtype
    df['datetime_15min'] = df['timestamputc'].map(floor_15).map(UTC_to_EDTEST) 
    df['datetime_15min'] = df['datetime_15min'].dt.tz_localize(None) #remove timezone before inserting into no tz column

    #parse each `lanedata` column entry 
    lane_data = df['lanedata'].map(parse_lane_data)
    n_rows = lane_data.map(len) #length of each nested list (# lanes), used to flatten data

    #flatten the nested list structure
    lane_data = [item for sublist in lane_data for item in sublist]

    #convert list structure to df
    cols = ['lane', 'speedKmh', 'volumeVehiclesPerHour', 'occupancyPercent']
    lane_data_df = pd.DataFrame(lane_data, columns = cols) 

    #repeat original index based on number of lanes represented in each row as a join column
    lane_data_df.set_index(df.index.repeat(n_rows), inplace=True)

    #join with other columns on index 
    raw_20sec = df[['divisionid', 'vdsid', 'datetime', 'datetime_15min']].join(lane_data_df)
    
    return raw_20sec