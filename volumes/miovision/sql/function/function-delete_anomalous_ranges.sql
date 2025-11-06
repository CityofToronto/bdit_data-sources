CREATE OR REPLACE FUNCTION miovision_api.delete_anomalous_ranges(
    ds date,
    intersections integer [] DEFAULT ARRAY[]::integer []
)
RETURNS void AS $$

DECLARE
    target_intersections integer [] = miovision_api.get_intersections_uids(intersections);

BEGIN

    --delete one day anomalous_ranges
    DELETE FROM miovision_api.anomalous_ranges AS ar
    WHERE
        ar.range_start = delete_anomalous_ranges.ds
        AND ar.range_end = delete_anomalous_ranges.ds + 1
        AND ar.investigation_level = 'auto_flagged'
        AND ar.intersection_uid = ANY(target_intersections);

    --roll back range ends by one day
    UPDATE miovision_api.anomalous_ranges AS ar
    SET range_end = range_end - interval '1 day'
    WHERE
        ar.range_end = delete_anomalous_ranges.ds + 1
        AND ar.investigation_level = 'auto_flagged'
        AND ar.intersection_uid = ANY(target_intersections);

    --roll forward range starts by one day
    UPDATE miovision_api.anomalous_ranges AS ar
    SET range_start = range_start + interval '1 day'
    WHERE
        ar.range_start = delete_anomalous_ranges.ds
        AND ar.investigation_level = 'auto_flagged'
        AND ar.intersection_uid = ANY(target_intersections);
END;

$$ LANGUAGE plpgsql;

ALTER FUNCTION miovision_api.get_intersections_uids
OWNER TO miovision_admins;

GRANT EXECUTE ON FUNCTION miovision_api.get_intersections_uids TO miovision_api_bot;
