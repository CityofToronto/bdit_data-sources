--Add unique constraint to bluetooth observations tables
--To be used by data_utils https://github.com/CityofToronto/bdit_data-sources/tree/master/data_utils 
CREATE OR REPLACE FUNCTION bluetooth.create_obs_unique(tablename text)
RETURNS integer AS
$BODY$
BEGIN
    EXECUTE format('ALTER TABLE bluetooth.%I ADD UNIQUE(user_id, analysis_id, measured_time, measured_timestamp);', tablename);
    RETURN 1;
END;
$BODY$
LANGUAGE plpgsql VOLATILE SECURITY DEFINER
COST 100;
ALTER FUNCTION bluetooth.create_obs_unique(text)
OWNER TO bt_admins;