CREATE OR REPLACE FUNCTION vds.update_vdsvehicledata_fkey()
RETURNS TRIGGER AS $$
BEGIN
    RAISE NOTICE 'Trigger function update_vdsvehicledata_fkey is activating to add vdsconfig_uid.';
    
    UPDATE vds.raw_vdsvehicledata AS d
    SET vdsconfig_uid = c.uid
    FROM vds.vdsconfig AS c
    WHERE
        d.vdsconfig_uid IS NULL 
        AND d.vds_id = c.vds_id
        AND d.division_id = c.division_id
        AND d.dt >= c.start_timestamp
        AND (
            d.dt < c.end_timestamp
            OR c.end_timestamp IS NULL); --no end date

    RAISE NOTICE 'Trigger function update_vdsvehicledata_fkey is activating to add entity_location_uid.';

    UPDATE vds.raw_vdsvehicledata AS d
    SET entity_location_uid = e.uid
    FROM vds.entity_locations AS e
    WHERE
        d.entity_location_uid IS NULL    
        AND d.vds_id = e.vds_id
        AND d.division_id = e.division_id
        AND d.dt >= e.start_timestamp
        AND (
            d.dt < e.end_timestamp
            OR e.end_timestamp IS NULL); --no end date

    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER update_vdsvehicledata_fkey_trigger
AFTER INSERT ON vds.raw_vdsvehicledata
FOR EACH STATEMENT
EXECUTE FUNCTION vds.update_vdsvehicledata_fkey();
