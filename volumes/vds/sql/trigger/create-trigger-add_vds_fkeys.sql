CREATE OR REPLACE FUNCTION vds.add_vds_fkeys()
RETURNS trigger AS
$BODY$
    BEGIN

    --add fkey to vdsconfig table
    IF NEW.vdsconfig_uid IS NULL THEN 
        NEW.vdsconfig_uid := (
            SELECT c.uid
            FROM vds.vdsconfig AS c
            WHERE
                NEW.vds_id = c.vds_id
                AND NEW.division_id = c.division_id
                AND NEW.dt >= c.start_timestamp
                AND (
                    NEW.dt < c.end_timestamp
                    OR c.end_timestamp IS NULL) --no end date
            ORDER BY c.start_timestamp DESC
            LIMIT 1
        ); 
    END IF; 

    IF NEW.entity_location_uid IS NULL THEN 
        --add fkey to entity_locations table
        NEW.entity_location_uid := (
            SELECT e.uid
            FROM vds.entity_locations AS e
            WHERE
                NEW.vds_id = e.entity_id
                AND NEW.division_id = e.division_id
                AND NEW.dt >= e.start_timestamp
                AND (
                    NEW.dt < e.end_timestamp
                    OR e.end_timestamp IS NULL) --no end date
            ORDER BY e.start_timestamp DESC
            LIMIT 1
        ); 
    END IF;

    RETURN NEW;

END;
$BODY$
LANGUAGE plpgsql
VOLATILE SECURITY DEFINER
COST 100;

COMMENT ON FUNCTION vds.add_vds_fkeys IS 'Before Insert/For Each row trigger to add
foreign keys referencing vdsconfig and entity_locations tables. Used for both raw_vdsdata 
and raw_vdsvehicledata.';

ALTER FUNCTION vds.add_vds_fkeys OWNER TO vds_admins;