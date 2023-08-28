CREATE OR REPLACE FUNCTION vds.add_vds_fkeys()
RETURNS TRIGGER AS
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
            FROM vds.entity_locations_new AS e
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

CREATE TRIGGER add_vds_fkeys_vdsdata
BEFORE INSERT ON vds.raw_vdsdata
FOR EACH ROW
EXECUTE PROCEDURE vds.add_vds_fkeys();

CREATE TRIGGER add_vds_fkeys_vdsvehicledata
BEFORE INSERT ON vds.raw_vdsvehicledata
FOR EACH ROW
EXECUTE PROCEDURE vds.add_vds_fkeys();
