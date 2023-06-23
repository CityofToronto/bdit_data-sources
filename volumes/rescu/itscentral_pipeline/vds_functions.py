def pull_raw_vdsdata(start_date, end_date):

    # Pull raw data from Postgres database
    raw_sql = sql.SQL(f'''SELECT 
        d.divisionid,
        d.vdsid,
        d.timestamputc,
        d.lanedata
    FROM public.vdsdata AS d
    WHERE 
        timestamputc >= extract(epoch from timestamptz {start_date})
        AND timestamputc < extract(epoch from timestamptz {end_date}) + 86400
        --AND d.divisionid IN 2; --other is 8001 which are traffic signal detectors
    ''').format(
        start_date = sql.Literal(start_date + ' 00:00:00'),
        end_date = sql.Literal(end_date + ' 00:00:00')
    )

    #this old sql was needed to control which sensors to pull data for based 
    # on additional attributes in the vdsconfig and entitylocation tables
    old_sql = f'''SELECT 
        d.divisionid,
        d.vdsid,
        d.timestamputc,
        d.lanedata
    FROM public.vdsdata AS d
    JOIN public.vdsconfig AS c ON
        d.vdsid = c.vdsid
        AND d.divisionid = c.divisionid
        AND to_timestamp(d.timestamputc) >= c.starttimestamputc
        AND (
            to_timestamp(d.timestamputc) <= c.endtimestamputc
            OR c.endtimestamputc IS NULL) --no end date
    JOIN public.EntityLocation AS e ON
        c.vdsid = e.entityid
        AND c.divisionid = e.divisionid
    WHERE 
        timestamputc >= extract(epoch from timestamptz '{{ds}}')
        AND timestamputc < extract(epoch from timestamptz '{{ds}}') + 86400
        --AND e.entityid IS NOT NULL --we only have locations for these ids
        --AND d.divisionid IN 2 --other is 8001 which are traffic signal detectors
        --AND substring(sourceid, 1, 3) <> 'BCT' --bluecity.ai sensors; 
    '''

    with itsc_conn.get_conn() as con:
        with con.cursor() as cur:
            raw_data = pd.read_sql(con, sql=raw_sql)
    
    # Transform raw data
    transformed_data = transform_raw_data(raw_data)

    with rds_conn.get_conn() as con:
        with con.cursor() as cur:
            # Drop records for the current date
            #drop_query = sql.SQL(f"DELETE FROM gwolofs.raw_vdsdata WHERE datetime_bin::date = '{dt}'")
            #con.execute(drop_query)
            
            # Insert cleaned data into the database
            insert_query = sql.SQL('''INSERT INTO gwolofs.raw_vdsdata (
                                    divisionid, vdsid, datetime_20sec, datetime_15min, lane, speedKmh, volumeVehiclesPerHour, occupancyPercent
                                    ) VALUES %s''')
            con.execute(insert_query)
            execute_values(cur, insert_query, transformed_data)


def pull_raw_vdsvehicledata(start_date, end_date): 

    raw_sql = sql.SQL('''
    SELECT
        divisionid,
        vdsid,
        timestamputc, --timestamp without tz
        lane,
        sensoroccupancyds,
        round(speedkmhdiv100 / 100, 1) AS speed_kmh,
        round(lengthmeterdiv100 / 100, 1) AS length_meter
    FROM public.vdsvehicledata
    WHERE
        timestamputc >= {}::timestamp
        AND timestamputc < {}::timestamp + INTERVAL '1 DAY';
    ''').format(sql.Literal(start_date + ' 00:00:00'), 
                sql.Literal(end_date + ' 00:00:00'))

    with itsc_conn.get_conn() as con:
        with con.cursor() as cur:
            cur.execute(raw_sql)
            raw_data = cur.fetchall()
    
    print(raw_data[0])

    with rds_conn.get_conn() as con: 
        with con.cursor() as cur:
    
            # Drop records for the current date
            #drop_query = sql.SQL(f"DELETE FROM gwolofs.raw_vdsvehicledata WHERE timestamptz::date = '{dt}'")
            #cur.execute(drop_query)
            
            # Insert cleaned data into the database
            insert_query = sql.SQL('''INSERT INTO gwolofs.raw_vdsvehicledata (
                                   divisionid, vdsid, timestamputc, lane, sensoroccupancyds, speed_kmh, length_meter
                                   ) VALUES %s;''')
            execute_values(cur, insert_query, raw_data)

def summarize_into_v15(start_date, end_date):
        
    with rds_conn.get_conn() as con:
        with con.cursor() as cur:

            #delete_v15 = sql.SQL(f"DELETE FROM gwolofs.vds_volumes_15min WHERE datetime_bin::date = '{dt}'")
            #cur.execute(delete_v15)
            
            #add sourceid to this query.
            insert_v15 = sql.SQL(f'''
                INSERT INTO gwolofs.vds_volumes_15min (detector_id, datetime_bin, volume_15min)
                SELECT detector_id, datetime_bin, SUM(volume) AS volume_15min
                FROM gwolofs.raw_20sec
                WHERE datetime_bin >= '{dt}'
                    AND datetime_bin < 
                GROUP BY detector_id, datetime_bin
            ''').format(
                start_date,
                end_date
            )
            con.execute(insert_v15)

def pull_detector_inventory():
    # Pull data from the detector_inventory table
    detector_sql = sql.SQL('''
    SELECT 
        divisionid,
        vdsid,
        sourceid,
        starttimestamputc,
        endtimestamputc,
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
    WHERE divisionid IN (2, 8001) --only these have data in 'vdsdata' table''')

    with itsc_conn.get_conn() as con:
        with con.cursor() as cur:
            cur.execute(detector_sql)
            vds_config_data = cur.fetchall()

    print("Number of rows fetched from vdsconfig table: {}".format(vds_config_data.shape[0]))

    # upsert data
    upsert_query = sql.SQL('''
        INSERT INTO gwolofs.vdsconfig (
            divisionid, vdsid, sourceid, starttimestamputc, endtimestamputc, lanes, hasgpsunit, 
            managementurl, description, fssdivisionid, fssid, rtmsfromzone, rtmstozone, detectortype, 
            createdby, createdbystaffid, signalid, signaldivisionid, movement)
        VALUES %s
        ON CONFLICT (column1) DO UPDATE
        SET column2 = EXCLUDED.column2, column3 = EXCLUDED.column3
    ''')
    with rds_conn.get_conn() as con:
        with con.cursor() as cur:
            execute_values(cur, upsert_query, vds_config_data)

def pull_EntityLocationLatest():
    # Pull data from the detector_inventory table
    entitylocation_sql = sql.SQL('''
    SELECT 
         divisionid,
         entitytype,
         entityid,
         locationtimestamputc,
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
    ''')

    with itsc_conn.get_conn() as con:
        with con.cursor() as cur:
            cur.execute(entitylocation_sql)
            entitylocations = cur.fetchall()
    
    print("Number of rows fetched from entitylocations table: {}".format(entitylocations.shape[0]))

    # upsert data
    upsert_query = sql.SQL('''
        INSERT INTO vds.entitylocation (
            divisionid, entitytype, entityid, locationtimestamputc, latitude, longitude, altitudemetersasl,
            headingdegrees, speedkmh, numsatellites, dilutionofprecision, mainroadid, crossroadid,
            secondcrossroadid, mainroadname, crossroadname, secondcrossroadname, streetnumber,
            offsetdistancemeters, offsetdirectiondegrees, locationsource, locationdescriptionoverwrite)
        VALUES %s
        --ON CONFLICT (...) DO UPDATE
        --SET column2 = EXCLUDED.column2, column3 = EXCLUDED.column3
    ''')

    with rds_conn.get_conn() as con:
        with con.cursor() as cur:
            execute_values(cur, upsert_query, entitylocations)


def transform_raw_data(df):
    import pandas as pd
    from datetime import datetime

    #get number of lanes in the binary data stream for each row
    empty_rows = df[[len(x) == 0 for x in df['lanedata']]]

    if empty_rows.empty is False:
        print(f'Rows with empty lanedata: {empty_rows.shape[0]}, ({(empty_rows.shape[0] / df.shape[0]):.2f}% of total).')
    else: 
        print(f'No empty rows discarded.')

    #drop empty rows #keep??? 
    #df_clean = df.drop(empty_rows.index) #remove empty rows

    df['datetime'] = df['timestamputc'].map(datetime.fromtimestamp)
    floor_15 = lambda a: 60 * 15 * (a // (60 * 15)) #very fast 15min binning using integer dtype
    df['datetime_15min'] = df['timestamputc'].map(floor_15).map(datetime.fromtimestamp) 

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

# Parse lane data
def parse_lane_data(laneData):
    import struct
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

