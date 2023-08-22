CREATE OR REPLACE FUNCTION vds.update_vdsconfig_fkey()
RETURNS TRIGGER AS $$
BEGIN
    UPDATE vds.raw_vdsdata AS d
    SET vdsconfig_uid = c.uid
    FROM vds.vdsconfig AS c
    WHERE
        NEW.volume_uid = d.volume_uid    
        AND NEW.vds_id = c.vds_id
        AND NEW.division_id = c.division_id
        AND NEW.datetime_15min >= c.start_timestamp
        AND (
            NEW.datetime_15min < c.end_timestamp
            OR c.end_timestamp IS NULL); --no end date
  
  RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER update_vdsconfig_fkey
BEFORE INSERT ON vds.raw_vdsdata
FOR EACH STATEMENT
EXECUTE FUNCTION vds.update_vdsconfig_fkey();
