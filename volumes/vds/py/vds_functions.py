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
            LOGGER.info("Fetching vdsdata")
            raw_data = pd.read_sql(raw_sql, con)
            LOGGER.info(f"Number of rows fetched from ITSC vdsdata table: {raw_data.shape[0]}")
    except Error as exc:
        LOGGER.critical("Error fetching vdsdata.")
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
                LOGGER.info("Inserting vdsdata into RDS.")
                execute_values(cur, insert_query, data_tuples)
                LOGGER.info(f"Inserted {len(data_tuples)} rows (lanedata expanded) into vds.raw_vdsdata.")
    except Error as exc:
        LOGGER.critical("Error inserting vdsdata into RDS.")
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
                LOGGER.info("Fetching vdsvehicledata")
                cur.execute(raw_sql)
                raw_data = cur.fetchall()
                LOGGER.info(f"Number of rows fetched from vdsvehicledata table: {len(raw_data)}")
    except Error as exc:
        LOGGER.critical("Error fetching vdsvehicledata.")
        LOGGER.critical(exc)
        con.close()
    
    try:
        with rds_conn.get_conn() as con: 
            with con.cursor() as cur:            
                # Insert cleaned data into the database
                insert_query = sql.SQL("""INSERT INTO vds.raw_vdsvehicledata (
                                    division_id, vds_id, dt, lane, sensor_occupancy_ds, speed_kmh, length_meter
                                    ) VALUES %s;""")
                LOGGER.info("Inserting into vds.raw_vdsvehicledata")
                execute_values(cur, insert_query, raw_data)
                LOGGER.info(f"Inserted {len(raw_data)} rows into vds.raw_vdsvehicledata.")
    except Error as exc:
        LOGGER.critical("Error inserting vds.raw_vdsvehicledata.")
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
                LOGGER.info("Fetching vdsconfig")
                cur.execute(detector_sql)
                vds_config_data = cur.fetchall()
                LOGGER.info(f"Number of rows fetched from vdsconfig table: {len(vds_config_data)}")
    except Error as exc:
        LOGGER.critical("Error fetching vdsconfig.")
        LOGGER.critical(exc)
        con.close()

    # upsert data
    insert_query = sql.SQL("""
        INSERT INTO vds.vdsconfig (
            division_id, vds_id, detector_id, start_timestamp, end_timestamp, lanes, has_gps_unit, 
            management_url, description, fss_division_id, fss_id, rtms_from_zone, rtms_to_zone, detector_type, 
            created_by, created_by_staffid, signal_id, signal_division_id, movement)
        VALUES %s
        ON CONFLICT DO NOTHING;
    """)
    select_count = sql.SQL("""SELECT COUNT(1) FROM vds.vdsconfig""")

    try: 
        with rds_conn.get_conn() as con:
            with con.cursor() as cur:
                cur.execute(select_count)
                original_count = cur.fetchone()[0]
                LOGGER.info("Inserting vdsconfig")
                execute_values(cur, insert_query, vds_config_data)
                cur.execute(select_count)
                new_count = cur.fetchone()[0]
                LOGGER.info(f"{new_count - original_count} new records inserted into vds.vdsconfig.")
    except Error as exc:
        LOGGER.critical("Error inserting vdsconfig.")
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
                LOGGER.info("Fetching entitylocation")
                cur.execute(entitylocation_sql)
                entitylocations = cur.fetchall()
    except Error as exc:
        LOGGER.critical("Error fetching entitylocation.")
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
    select_count = sql.SQL("""SELECT COUNT(1) FROM vds.entity_locations""")

    try:
        with rds_conn.get_conn() as con:
            with con.cursor() as cur:
                cur.execute(select_count)
                original_count = cur.fetchone()[0]
                LOGGER.info("Inserting into vds.entity_locations")
                execute_values(cur, upsert_query, entitylocations)
                cur.execute(select_count)
                new_count = cur.fetchone()[0]
                LOGGER.info(f"{new_count - original_count} new records inserted into vds.entity_locations.")
    except Error as exc:
        LOGGER.critical("Error inserting vds_entity_locations.")
        LOGGER.critical(exc)
        con.close()

def parse_lane_data(laneData):
# Parse binary vdsdata.lanedata column
# Function adapted from SF's C# code stored at: "K:\tra\GM Office\Big Data Group\Data Sources\VDS\RE Back-end Connection to ITS Central data.msg"

    result = []

    with memoryview(laneData) as mv:
        i = 0 #index within memoryview
        while i < len(mv):
            # Get lane
            lane = mv[i][0] #single byte

            # Get speed
            #Stored in km/h * 100. Null value represented by 65535. Convert 0 to null to maintain backward compatibility
            speed = struct.unpack('<H', mv[i + 1] + mv[i + 2])[0] #two bytes. '<H' denotes stored in little-endian format
            speedKmh = None if speed == 65535 or speed == 0 else speed / 100.0

            # Get volume
            #Stored in vehicles per hour. Null value represented by 65535.
            volume = struct.unpack('<H', mv[i + 3] + mv[i + 4])[0] #two bytes
            volumeVehiclesPerHour = None if volume == 65535 else volume

            # Get occupancy
            #Stored in percent * 100. Null value represented by 65535.
            occupancy = struct.unpack('<H', mv[i + 5] + mv[i + 6])[0] #two bytes
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

    #identify empty rows by length of binary data stream = 0.
    empty_rows = df[[len(x) == 0 for x in df['lanedata']]]

    if empty_rows.empty is False:
        LOGGER.info(f"Rows with empty vdsdata.lanedata: {empty_rows.shape[0]}, ({(empty_rows.shape[0] / df.shape[0]):.2f}% of total).")
    else: 
        LOGGER.info("No empty rows in vdsdata.lanedata.")

    #drop empty rows? #decided to keep
    #df_clean = df.drop(empty_rows.index) #remove empty rows

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


def monitor_vdsdata(rds_conn, itsc_conn, start_date): 
# compare row counts for vdsdata table in ITSC vs RDS and clear tasks to rerun if additional rows found. 
      
    itsc_query = sql.SQL("""
        SELECT
            TIMEZONE('EST5EDT', TO_TIMESTAMP(timestamputc))::date AS dt, 
            COUNT(*) AS count_itsc
        FROM public.vdsdata
        WHERE
            timestamputc >= extract(epoch from timestamp with time zone {start} - INTERVAL '7 DAY')
            AND timestamputc < extract(epoch from timestamp with time zone {start})
            AND divisionid = 2 --other is 8001 which are traffic signal detectors and are mostly empty
        GROUP BY 1
    """).format(
        start = sql.Literal(start_date + " 00:00:00 EST5EDT")
    )
    
    try:
        with itsc_conn.get_conn() as con:
            LOGGER.info("Fetching vdsdata daily count.")
            itsc_rows = pd.read_sql(itsc_query, con)
            LOGGER.info(f"Number of rows fetched from ITSC: {itsc_rows.shape[0]}")
    except Error as exc:
        LOGGER.critical("Error fetching row count for ITSC vdsdata.")
        LOGGER.critical(exc)
        con.close()
    
    rds_query = sql.SQL("""
        SELECT
            date_trunc('day', datetime_15min)::date AS dt,
            COUNT(DISTINCT vds_id::text || datetime_20sec::text) AS count_rds
                --due to lanedata expansion, need to count distinct vds_id + dt
        FROM vds.raw_vdsdata
        WHERE
            datetime_20sec >= {start}::timestamp - INTERVAL '7 DAY'
            AND datetime_20sec < {start}::timestamp
        GROUP BY 1
        """).format(
        start = sql.Literal(start_date + " 00:00:00")
    )

    try:
        with rds_conn.get_conn() as con: 
            LOGGER.info("Fetching vds.raw_vdsdata daily count")
            rds_rows = pd.read_sql(rds_query, con)
            LOGGER.info(f"Number of rows fetched from RDS: {rds_rows.shape[0]}.")
    except Error as exc:
        LOGGER.critical("Error fetching row count for vds.raw_vdsdata.")
        LOGGER.critical(exc)
        con.close()

    #find days with more rows in ITSC than RDS (existing pull). 
    combined_rows = rds_rows.merge(itsc_rows, on='dt')
    dates_dif = combined_rows[combined_rows['count_itsc'] > combined_rows['count_rds']]['dt'].map(lambda a: datetime(a))

    for dt in dates_dif:
        LOGGER.info(f"Clearing vds_pull.vdsdata_complete for {dt}")       
        """ clear_tasks = BashOperator(
            task_id='clear_tasks',
            bash_command=f"airflow tasks clear -s {dt} -e {dt} -y -t vdsdata_complete vdspull"
        )
        clear_tasks.execute(context=context) """

def monitor_vdsvehicledata(rds_conn, itsc_conn, start_date): 
# compare row counts for vdsvehicledata table in ITSC vs RDS and clear tasks to rerun if additional rows found. 

    itsc_query = sql.SQL("""
        SELECT
            (TIMEZONE('UTC', d.timestamputc) AT TIME ZONE 'EST5EDT')::date AS dt, --convert timestamp (without timezone) at UTC to EDT/EST
            count(*) AS count
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
            AND TIMEZONE('UTC', d.timestamputc) >= {start}::timestamptz - INTERVAL '7 DAY'
            AND TIMEZONE('UTC', d.timestamputc) < {start}::timestamptz
            AND substring(c.sourceid, 1, 3) <> 'BCT' --bluecity.ai sensors have no data
        GROUP BY 1
    """).format(
        start = sql.Literal(start_date + " 00:00:00 EST5EDT")
    )
    
    try:
        with itsc_conn.get_conn() as con:
            LOGGER.info("Fetching vdsvehicledata daily count.")
            itsc_rows = pd.read_sql(itsc_query, con)
            LOGGER.info(f"Number of rows fetched from ITSC: {itsc_rows.shape[0]}")
    except Error as exc:
        LOGGER.critical("Error fetching row count for ITSC vdsvehicledata.")
        LOGGER.critical(exc)
        con.close()
    
    rds_query = sql.SQL("""
        SELECT
            d.dt::date AS dt, --convert timestamp (without timezone) at UTC to EDT/EST
            count(*) AS count
        FROM vds.raw_vdsvehicledata AS d
        WHERE
            d.division_id = 2 --8001 and 8046 have only null values for speed/length/occupancy
            AND dt >= {start}::timestamp - INTERVAL '7 DAY'
            AND dt < {start}::timestamp
        GROUP BY 1
        """).format(
        start = sql.Literal(start_date + " 00:00:00")
    )

    try:
        with rds_conn.get_conn() as con: 
            LOGGER.info("Fetching vds.raw_vdsvehicledata daily count")
            rds_rows = pd.read_sql(rds_query, con)
            LOGGER.info(f"Number of rows fetched from RDS: {rds_rows.shape[0]}.")
    except Error as exc:
        LOGGER.critical("Error fetching row count for vds.raw_vdsvehicledata.")
        LOGGER.critical(exc)
        con.close()

    #find days with more rows in ITSC than RDS (existing pull). 
    #from airflow.operators.bash import BashOperator
    combined_rows = rds_rows.merge(itsc_rows, on='dt')
    dates_dif = combined_rows[combined_rows['count_itsc'] > combined_rows['count_rds']]['dt'].map(lambda a: datetime(a))

    for dt in dates_dif:
        LOGGER.info(f"Clearing vds_pull.vdsdata_complete for {dt}")       
        """ clear_tasks = BashOperator(
            task_id='clear_tasks',
            bash_command=f"airflow tasks clear -s {dt} -e {dt} -y -t vdsdata_complete vdspull"
        )
        clear_tasks.execute(context=context) """