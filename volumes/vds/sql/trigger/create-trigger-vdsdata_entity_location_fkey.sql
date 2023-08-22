CREATE OR REPLACE FUNCTION vds.update_vdsdata_entity_location_fkey()
RETURNS TRIGGER AS $$
BEGIN

    UPDATE vds.raw_vdsdata AS d
    SET entity_location_uid = e.uid
    FROM vds.entity_locations AS e
    WHERE
        NEW.volume_uid = d.volume_uid    
        AND d.vds_id = e.vds_id
        AND d.division_id = e.division_id
        AND d.dt >= e.start_timestamp
        AND (
            d.dt < e.end_timestamp
            OR e.end_timestamp IS NULL); --no end date
  
  RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER update_vdsdata_entity_location_fkey
BEFORE INSERT ON vds.raw_vdsdata
FOR EACH STATEMENT
EXECUTE FUNCTION vds.update_vdsdata_entity_location_fkey();