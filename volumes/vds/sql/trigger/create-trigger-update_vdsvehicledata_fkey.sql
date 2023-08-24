CREATE OR REPLACE FUNCTION vds.vdsvehicledata_process_staging()
RETURNS TRIGGER AS $$
BEGIN
    RAISE NOTICE 'Trigger function to move vdsvehicledata from staging to raw.';
       
    WITH new_data AS (
        SELECT
            rv.division_id,
            rv.vds_id,
            c.uid AS vdsconfig_uid,
            e.uid AS entity_locations_uid,
            rv.dt,
            rv.lane,
            rv.sensor_occupancy_ds,
            rv.speed_kmh,
            rv.length_meter
        FROM vds.vdsvehicledata_staging AS rv
        LEFT JOIN vds.vdsconfig AS c ON
            rv.vds_id = c.vds_id
            AND c.division_id = 2 --vdsvehicledata has only 2. 
            AND rv.dt >= c.start_timestamp
            AND (
                rv.dt < c.end_timestamp
                OR c.end_timestamp IS NULL) --no end date
        LEFT JOIN vds.entity_locations_new AS e ON
            rv.vds_id = e.entity_id
            AND e.division_id = 2 --vdsvehicledata has only 2. 
            AND rv.dt >= e.start_timestamp
            AND (
                rv.dt < e.end_timestamp
                OR e.end_timestamp IS NULL)
    ), 

    good_data AS (
        INSERT INTO vds.raw_vdsvehicledata (
            division_id, vds_id, vdsconfig_uid, entity_location_uid, dt, 
            lane, sensor_occupancy_ds, speed_kmh, length_meter
        ) 

        SELECT
            division_id, vds_id, vdsconfig_uid, entity_location_uid, dt, 
            lane, sensor_occupancy_ds, speed_kmh, length_meter
        FROM new_data
        WHERE
            vdsconfig_uid IS NOT NULL
            AND entity_location_uid IS NOT NULL
    ),

    quarantine AS (
        INSERT INTO vds.raw_vdsvehicledata_quarantine (
            division_id, vds_id, vdsconfig_uid, entity_location_uid, dt, 
            lane, sensor_occupancy_ds, speed_kmh, length_meter
        ) 

        SELECT
            division_id, vds_id, vdsconfig_uid, entity_location_uid, dt, 
            lane, sensor_occupancy_ds, speed_kmh, length_meter
        FROM new_data
        WHERE
            vdsconfig_uid IS NULL
            OR entity_location_uid IS NULL
    )
    
    DELETE FROM vds.vdsvehicledata_staging;

END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER vdsvehicledata_process_staging
AFTER INSERT ON vds.vdsvehicledata_staging
FOR EACH STATEMENT
EXECUTE FUNCTION vds.vdsvehicledata_process_staging();
