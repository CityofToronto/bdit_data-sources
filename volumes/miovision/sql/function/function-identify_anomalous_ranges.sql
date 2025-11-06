CREATE OR REPLACE FUNCTION miovision_api.identify_anomalous_ranges(
    ds date,
    intersections integer [] DEFAULT ARRAY[]::integer []
)
RETURNS void AS $$

DECLARE
    target_intersections integer [] = miovision_api.get_intersections_uids(intersections);

BEGIN
    PERFORM miovision_api.delete_anomalous_ranges(identify_anomalous_ranges.ds, target_intersections);
    PERFORM miovision_api.identify_zero_counts(identify_anomalous_ranges.ds, target_intersections);
    PERFORM miovision_api.merge_consecutive_anomalous_ranges();
END;

$$
language plpgsql;

ALTER FUNCTION miovision_api.identify_anomalous_ranges
OWNER TO miovision_admins;

GRANT EXECUTE ON FUNCTION miovision_api.identify_anomalous_ranges TO miovision_api_bot;
