def pull_raw_data():
    # Pull raw data from Postgres database
    raw_sql = f'''SELECT 
        d.divisionid,
        d.vdsid,
        d.timestamputc,
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
        timestamputc >= extract(epoch from timestamptz '{{ds}}')
        AND timestamputc < extract(epoch from timestamptz '{{ds}}') + 86400
        AND e.entityid IS NOT NULL --we only have locations for these ids
        AND d.divisionid = 2 --other is 8001 which are traffic signal detectors
        AND substring(sourceid, 1, 3) <> 'BCT' --bluecity.ai sensors; 
    '''

    with itsc_postgres as con: 
        raw_data = con.get_records(raw_sql)
    
    # Transform raw data
    transformed_data = transform_raw_data(raw_data)

    with rds_postgres as con:     
        # Drop records for the current date
        drop_query = f"DELETE FROM gwolofs.raw_20sec WHERE datetime_bin::date = '{dt}'"
        con.run(drop_query)
        
        # Insert cleaned data into the database
        insert_query = f"INSERT INTO gwolofs.raw_20sec VALUES {transformed_data}"
        con.run(insert_query)

def summarize_into_v15():
        
    with rds_postgres as con: 
        delete_v15 = f"DELETE FROM gwolofs.volumes_15min WHERE datetime_bin::date = '{dt}'"
        con.run(delete_v15)

        insert_v15 = f'''
            INSERT INTO gwolofs.volumes_15min (detector_id, datetime_bin, volume_15min)
            SELECT detector_id, datetime_bin, SUM(volume) AS volume_15min
            FROM gwolofs.raw_20sec
            WHERE datetime_bin::date = '{dt}'
            GROUP BY detector_id, datetime_bin
        '''
        con.run(insert_v15)


def pull_detector_inventory():
    # Pull data from the detector_inventory table
    with open(repo_path + 'volumes/rescu/itscentral_pipeline/itsc_detector_inventory.sql') as f:
        detector_sql = f.readlines()  

    with itsc_postgres as con:
        detector_data = con.get_records(detector_sql)
    
    # upsert data
    upsert_query = '''
        INSERT INTO detector_inventory (column1, column2, column3)
        VALUES %s
        ON CONFLICT (column1) DO UPDATE
        SET column2 = EXCLUDED.column2, column3 = EXCLUDED.column3
    '''
    with rds_postgres as con: 
        con.run(upsert_query, parameters=detector_data)

def pull_EntityLocationLatest():
    # Pull data from the detector_inventory table
    detector_sql = '''
    SELECT 
        e.divisionid,
        e.entitytype,
        e.entityid,
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
    FROM entitylocation AS e
    WHERE divisionid IN (2, 8001) --only these have data in 'vdsdata' table
    '''  

    with itsc_postgres as con:
        entitylocations = con.get_records(detector_sql)
    
    print("Number of rows fetched from entitylocations table: {}".format(entitylocations.shape[0]))

    # upsert data
    upsert_query = '''
        INSERT INTO vds.entitylocation (column1, column2, column3)
        VALUES %s
        ON CONFLICT (column1) DO UPDATE
        SET column2 = EXCLUDED.column2, column3 = EXCLUDED.column3
    '''
    with rds_postgres as con: 
        con.run(upsert_query, parameters=detector_data)


def transform_raw_data(df):
    import pandas as pd
    from datetime import datetime

    #get number of lanes in the binary data stream for each row
    empty_rows = df[[len(x) == 0 for x in df['lanedata']]]

    if empty_rows.empty is False:
        print(f'Rows with empty lanedata: {empty_rows.shape[0]}')
    else: 
        print(f'No empty rows discarded.')

    #drop empty rows
    df_clean = df.drop(empty_rows.index) #remove empty rows

    df_clean['datetime'] = df_clean['timestamputc'].map(datetime.fromtimestamp)
    floor_15 = lambda a: 60 * 15 * (a // (60 * 15)) #very fast 15min binning using integer dtype
    df_clean['datetime_15min'] = df_clean['timestamputc'].map(floor_15).map(datetime.fromtimestamp) 

    #parse each `lanedata` column entry 
    lane_data = df_clean['lanedata'].map(parse_lane_data)
    n_rows = lane_data.map(len)

    #flatten the nested list structure
    lane_data = [item for sublist in lane_data for item in sublist]

    #convert list structure to df
    cols = ['lane', 'speedKmh', 'volumeVehiclesPerHour', 'occupancyPercent']
    lane_data_df = pd.DataFrame(lane_data, columns = cols) 

    #repeat original index based on number of rows in result as a join column
    lane_data_df.set_index(df_clean.index.repeat(n_rows), inplace=True)

    #join with other columns on index 
    raw_20sec = df_clean[['divisionid', 'vdsid', 'datetime', 'datetime_15min']].join(lane_data_df)
    
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

