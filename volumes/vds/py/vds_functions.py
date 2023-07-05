import logging
import pandas as pd
from numpy import nan
from psycopg2 import sql, Error
from psycopg2.extras import execute_values
import struct
from datetime import datetime
import pytz

LOGGER = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

def pull_raw_vdsdata(rds_conn, itsc_conn, start_date):
#pulls data from ITSC table `vdsdata` and inserts into RDS table `vds.raw_vdsdata`

    # Pull raw data from Postgres database
    raw_sql = sql.SQL("""SELECT 
        d.divisionid,
        d.vdsid,
        d.timestamputc, --timestamp in INTEGER (UTC)
        d.lanedata
    FROM public.vdsdata AS d
    WHERE
        timestamputc >= extract(epoch from timestamp with time zone {start})
        AND timestamputc < extract(epoch from timestamp with time zone {start} + INTERVAL '1 DAY')
        AND d.divisionid = 2; --other is 8001 which are traffic signal detectors and are mostly empty.
    """).format(
        start = sql.Literal(start_date + " 00:00:00 EST5EDT")
    )

    try: 
        with itsc_conn.get_conn() as con:
            LOGGER.info('Fetching vdsdata')
            raw_data = pd.read_sql(raw_sql, con)
    except Error as exc:
        LOGGER.critical('Error fetching vdsdata.')
        LOGGER.critical(exc)
        con.close()
    
    # Transform raw data
    transformed_data = transform_raw_data(raw_data)
    transformed_data = transformed_data.replace(nan, None)
    transformed_data = transformed_data[[x is not None for x in transformed_data['lane']]] #remove lane is None
    data_tuples = [tuple(x) for x in transformed_data.to_numpy()]

    try:
        with rds_conn.get_conn() as con:
            with con.cursor() as cur:                
                # Insert cleaned data into the database
                insert_query = sql.SQL("""INSERT INTO vds.raw_vdsdata (
                                        division_id, vds_id, datetime_20sec, datetime_15min, lane, speed_kmh, volume_veh_per_hr, occupancy_percent
                                        ) VALUES %s;""")
                LOGGER.info('Inserting vdsdata into RDS.')
                execute_values(cur, insert_query, data_tuples)
    except Error as exc:
        LOGGER.critical('Error inserting vdsdata into RDS.')
        LOGGER.critical(exc)
        con.close()

def pull_raw_vdsvehicledata(rds_conn, itsc_conn, start_date): 
#pulls data from ITSC table `vdsvehicledata` and inserts into RDS table `vds.raw_vdsvehicledata`
#contains individual vehicle activations from highway sensors (speed, length)
      
    raw_sql = sql.SQL("""
    SELECT
        d.divisionid,
        d.vdsid,
        TIMEZONE('UTC', d.timestamputc) AT TIME ZONE 'EST5EDT' AS dt, --convert timestamp (without timezone) at UTC to EDT/EST
        d.lane,
        d.sensoroccupancyds,
        round(d.speedkmhdiv100 / 100, 1) AS speed_kmh,
        round(d.lengthmeterdiv100 / 100, 1) AS length_meter
    FROM public.vdsvehicledata AS d
    LEFT JOIN public.vdsconfig AS c ON
        d.vdsid = c.vdsid
        AND d.divisionid = c.divisionid
        AND d.timestamputc >= c.starttimestamputc
        AND (
            d.timestamputc <= c.endtimestamputc
            OR c.endtimestamputc IS NULL) --no end date
    WHERE
        d.divisionid = 2 --8001 and 8046 have only null values for speed/length/occupancy
        AND TIMEZONE('UTC', d.timestamputc) >= {start}::timestamptz
        AND TIMEZONE('UTC', d.timestamputc) < {start}::timestamptz + INTERVAL '1 DAY'
        AND substring(c.sourceid, 1, 3) <> 'BCT'; --bluecity.ai sensors have no data
    """).format(
        start = sql.Literal(start_date + " 00:00:00 EST5EDT")
    )
    
    try:
        with itsc_conn.get_conn() as con:
            with con.cursor() as cur:
                LOGGER.info('Fetching vdsvehicledata')
                cur.execute(raw_sql)
                raw_data = cur.fetchall()
    except Error as exc:
        LOGGER.critical('Error fetching vdsvehicledata.')
        LOGGER.critical(exc)
        con.close()
    
    try:
        with rds_conn.get_conn() as con: 
            with con.cursor() as cur:            
                # Insert cleaned data into the database
                insert_query = sql.SQL("""INSERT INTO vds.raw_vdsvehicledata (
                                    division_id, vds_id, dt, lane, sensor_occupancy_ds, speed_kmh, length_meter
                                    ) VALUES %s;""")
                LOGGER.info('Inserting into vds.raw_vdsvehicledata')
                execute_values(cur, insert_query, raw_data)           
    except Error as exc:
        LOGGER.critical('Error inserting vds.raw_vdsvehicledata.')
        LOGGER.critical(exc)
        con.close()

def pull_detector_inventory(rds_conn, itsc_conn):
#pull the detector inventory table (`vdsconfig`) from ITS Central and insert into RDS `vds.vdsconfig` as is. 
#very small table so OK to pull entire table daily. 

    # Pull data from the detector_inventory table
    detector_sql = sql.SQL("""
    SELECT 
        divisionid,
        vdsid,
        UPPER(sourceid) AS detector_id, --match what we already have
        TIMEZONE('UTC', starttimestamputc) AT TIME ZONE 'EST5EDT' AS starttimestamp,
        TIMEZONE('UTC', endtimestamputc) AT TIME ZONE 'EST5EDT' AS endtimestamp,
        lanes,
        hasgpsunit,
        managementurl,
        description,
        fssdivisionid,
        fssid,
        rtmsfromzone,
        rtmstozone,
        detectortype,
        createdby,
        createdbystaffid,
        signalid,
        signaldivisionid,
        movement
    FROM public.vdsconfig
    WHERE divisionid IN (2, 8001) --only these have data in 'vdsdata' table""")

    try: 
        with itsc_conn.get_conn() as con:
            with con.cursor() as cur:
                LOGGER.info('Fetching vdsconfig')
                cur.execute(detector_sql)
                vds_config_data = cur.fetchall()
    except Error as exc:
        LOGGER.critical('Error fetching vdsconfig.')
        LOGGER.critical(exc)
        con.close()

    LOGGER.info(f"Number of rows fetched from vdsconfig table: {len(vds_config_data)}")

    # upsert data
    insert_query = sql.SQL("""
        INSERT INTO vds.vdsconfig (
            division_id, vds_id, detector_id, start_timestamp, end_timestamp, lanes, has_gps_unit, 
            management_url, description, fss_division_id, fss_id, rtms_from_zone, rtms_to_zone, detector_type, 
            created_by, created_by_staffid, signal_id, signal_division_id, movement)
        VALUES %s
        ON CONFLICT DO NOTHING;
    """)

    try: 
        with rds_conn.get_conn() as con:
            with con.cursor() as cur:
                LOGGER.info('Inserting vdsconfig')
                execute_values(cur, insert_query, vds_config_data)
    except Error as exc:
        LOGGER.critical('Error inserting vdsconfig.')
        LOGGER.critical(exc)
        con.close()

def pull_entity_locations(rds_conn, itsc_conn):
#pull the detector locations table (`entitylocations`) from ITS Central and insert new rows into RDS `vds.entity_locations`.
#very small table so OK to pull entire table daily. 

    # Pull data from the detector_inventory table
    entitylocation_sql = sql.SQL("""
    SELECT 
         divisionid,
         entitytype,
         entityid,
         TIMEZONE('UTC', locationtimestamputc) AT TIME ZONE 'EST5EDT' AS locationtimestamp,
         latitude,
         longitude,
         altitudemetersasl,
         headingdegrees,
         speedkmh,
         numsatellites,
         dilutionofprecision,
         mainroadid,
         crossroadid,
         secondcrossroadid,
         mainroadname,
         crossroadname,
         secondcrossroadname,
         streetnumber,
         offsetdistancemeters,
         offsetdirectiondegrees,
         locationsource,
         locationdescriptionoverwrite
    FROM public.entitylocation
    WHERE divisionid IN (2, 8001) --only these have data in 'vdsdata' table
    """)

    try:
        with itsc_conn.get_conn() as con:
            with con.cursor() as cur:
                LOGGER.info('Fetching entitylocation')
                cur.execute(entitylocation_sql)
                entitylocations = cur.fetchall()
    except Error as exc:
        LOGGER.critical('Error fetching entitylocation.')
        LOGGER.critical(exc)
        con.close()

    LOGGER.info(f"Number of rows fetched from entitylocations table: {len(entitylocations)}")

    # upsert data
    upsert_query = sql.SQL("""
        INSERT INTO vds.entity_locations (
            division_id, entity_type, entity_id, location_timestamp, latitude, longitude, altitude_meters_asl, 
            heading_degrees, speed_kmh, num_satellites, dilution_of_precision, main_road_id, cross_road_id,
            second_cross_road_id, main_road_name, cross_road_name, second_cross_road_name, street_number,
            offset_distance_meters, offset_direction_degrees, location_source, location_description_overwrite)
        VALUES %s
        ON CONFLICT DO NOTHING;
    """)

    try:
        with rds_conn.get_conn() as con:
            with con.cursor() as cur:
                LOGGER.info('Inserting vds_entity_locations')
                execute_values(cur, upsert_query, entitylocations)
    except Error as exc:
        LOGGER.critical('Error inserting vds_entity_locations.')
        LOGGER.critical(exc)
        con.close()


def parse_lane_data(laneData):
# Parse binary vdsdata.lanedata column

    result = []

    with memoryview(laneData) as mv:
        i = 0 #index within memoryview
        while i < len(mv):
            # Get lane
            lane = mv[i][0]

            # Get speed
            #Stored in km/h * 100. 65535 for null value. Convert 0 to null to maintain backward compatibility
            speed = struct.unpack('<H', mv[i + 1] + mv[i + 2])[0] #<H denotes stored in little-endian format
            speedKmh = None if speed == 65535 or speed == 0 else speed / 100.0

            # Get volume
            #Stored in vehicles per hour. 65535 for null value.
            volume = struct.unpack('<H', mv[i + 3] + mv[i + 4])[0]
            volumeVehiclesPerHour = None if volume == 65535 else volume

            # Get occupancy
            #Stored in percent * 100. 65535 for null value.
            occupancy = struct.unpack('<H', mv[i + 5] + mv[i + 6])[0]
            occupancyPercent = None if occupancy == 65535 else occupancy / 100.0

            # Get volume by vehicle lengths - these columns are empty 
            #Each class stored in vehicles per hour. 65535 for null value.
            #passengerVolume = struct.unpack('<H', mv[i + 7] + mv[i + 8])[0]
            #volumePassengerVehiclesPerHour = None if passengerVolume == 65535 else passengerVolume
            #singleUnitTrucksVolume = struct.unpack('<H', mv[i + 9] + mv[i + 10])[0]
            #volumeSingleUnitTrucksPerHour = None if singleUnitTrucksVolume == 65535 else singleUnitTrucksVolume
            #comboTrucksVolume = struct.unpack('<H', mv[i + 11] + mv[i + 12])[0]
            #volumeComboTrucksPerHour = None if comboTrucksVolume == 65535 else comboTrucksVolume
            #multiTrailerTrucksVolume = struct.unpack('<H', mv[i + 13] + mv[i + 14])[0]
            #volumeMultiTrailerTrucksPerHour = None if multiTrailerTrucksVolume == 65535 else multiTrailerTrucksVolume

            # Increment i by 15 to move to the next lane
            i += 15

            result.append([lane, speedKmh, volumeVehiclesPerHour, occupancyPercent])
                    #Extra columns, not used:, volumePassengerVehiclesPerHour, volumeSingleUnitTrucksPerHour, volumeComboTrucksPerHour, volumeMultiTrailerTrucksPerHour])
            
    return result

def transform_raw_data(df):
#transform vdsdata for inserting into RDS.

    #get number of lanes in the binary data stream for each row
    empty_rows = df[[len(x) == 0 for x in df['lanedata']]]

    if empty_rows.empty is False:
        print(f'Rows with empty lanedata: {empty_rows.shape[0]}, ({(empty_rows.shape[0] / df.shape[0]):.2f}% of total).')
    else: 
        print(f'No empty rows in vdsdata.')

    #drop empty rows? #decided to keep
    #df_clean = df.drop(empty_rows.index) #remove empty rows
    UTC_to_EDTEST = lambda a: datetime.fromtimestamp(a, tz = pytz.timezone("EST5EDT"))

    df['datetime'] = df['timestamputc'].map(UTC_to_EDTEST) #convert from integer to timestamp
    df['datetime'] = df['datetime'].dt.tz_localize(None) #remove timezone before inserting into no tz column

    floor_15 = lambda a: 60 * 15 * (a // (60 * 15)) #very fast 15min binning using integer dtype
    df['datetime_15min'] = df['timestamputc'].map(floor_15).map(UTC_to_EDTEST) 
    df['datetime_15min'] = df['datetime_15min'].dt.tz_localize(None) #remove timezone before inserting into no tz column

    #parse each `lanedata` column entry 
    lane_data = df['lanedata'].map(parse_lane_data)
    n_rows = lane_data.map(len)

    #flatten the nested list structure
    lane_data = [item for sublist in lane_data for item in sublist]

    #convert list structure to df
    cols = ['lane', 'speedKmh', 'volumeVehiclesPerHour', 'occupancyPercent']
    lane_data_df = pd.DataFrame(lane_data, columns = cols) 

    #repeat original index based on number of rows in result as a join column
    lane_data_df.set_index(df.index.repeat(n_rows), inplace=True)

    #join with other columns on index 
    raw_20sec = df[['divisionid', 'vdsid', 'datetime', 'datetime_15min']].join(lane_data_df)
    
    return raw_20sec


