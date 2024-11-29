CREATE OR REPLACE FUNCTION ecocounter.open_data_locations_insert()
RETURNS void
LANGUAGE sql
COST 100
VOLATILE

AS $BODY$

    INSERT INTO open_data.cycling_permanent_counts_locations (
        location_name, direction, linear_name_full, side_street, longitude, latitude,
        centreline_id, bin_size, latest_calibration_study, first_active, last_active,
        date_decommissioned, technology
    )
    SELECT
        location_name,
        direction,
        linear_name_full,
        side_street,
        lng,
        lat,
        centreline_id,
        bin_size::text,
        latest_calibration_study,
        first_active,
        last_active,
        date_decommissioned,
        technology
    FROM ecocounter.open_data_locations
    ON CONFLICT (location_name, direction)
    DO UPDATE SET
        linear_name_full = EXCLUDED.linear_name_full,
        side_street = EXCLUDED.side_street,
        longitude = EXCLUDED.longitude,
        latitude = EXCLUDED.latitude,
        centreline_id = EXCLUDED.centreline_id,
        bin_size = EXCLUDED.bin_size,
        latest_calibration_study = EXCLUDED.latest_calibration_study,
        first_active = EXCLUDED.first_active,
        last_active = EXCLUDED.last_active,
        date_decommissioned = EXCLUDED.date_decommissioned,
        technology = EXCLUDED.technology;

$BODY$;

ALTER FUNCTION ecocounter.open_data_locations_insert() OWNER TO ecocounter_admins;

GRANT EXECUTE ON FUNCTION ecocounter.open_data_locations_insert() TO ecocounter_admins;
GRANT EXECUTE ON FUNCTION ecocounter.open_data_locations_insert() TO ecocounter_bot;
REVOKE EXECUTE ON FUNCTION ecocounter.open_data_locations_insert() FROM bdit_humans;

COMMENT ON FUNCTION ecocounter.open_data_locations_insert() IS
'Function to insert locations Ecocounter locations into '
'`open_data.cycling_permanent_counts_locations`. ';
