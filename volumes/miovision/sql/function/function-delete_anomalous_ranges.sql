CREATE OR REPLACE FUNCTION miovision_api.delete_anomalous_ranges(
    ds date,
    intersections integer [] DEFAULT ARRAY[]::integer []
)
RETURNS void AS $$

DECLARE
    target_intersections integer [] = miovision_api.get_intersections_uids(intersections);

BEGIN

    WITH ranges_to_split AS (
        DELETE FROM miovision_api.anomalous_ranges
        WHERE
            investigation_level = 'auto_flagged'
            AND intersection_uid = ANY(target_intersections)
            --overlaps
            AND delete_anomalous_ranges.ds::timestamp <@ tsrange(range_start, range_end, '[)')
        RETURNING intersection_uid, classification_uid, notes, uid, investigation_level, problem_level, range_start, range_end, leg
    )

    --inserts 0-2 anomalous ranges to replace deleted one
    INSERT INTO miovision_api.anomalous_ranges (
        intersection_uid, classification_uid, notes, investigation_level, problem_level, leg, range_start, range_end
    )
    SELECT intersection_uid, classification_uid, notes, investigation_level, problem_level, leg,
        --rolls end back
        range_start, LEAST(delete_anomalous_ranges.ds, range_end) AS range_end
    FROM ranges_to_split
    --new range is still valid after roll back
    WHERE range_start < LEAST(delete_anomalous_ranges.ds, range_end)
    UNION
    SELECT intersection_uid, classification_uid, notes, investigation_level, problem_level, leg,
        --rolls start forward
        GREATEST(delete_anomalous_ranges.ds + 1, range_start) AS range_start, range_end
    FROM ranges_to_split
    --new range is still valid after roll forward
    WHERE GREATEST(delete_anomalous_ranges.ds + 1, range_start) < range_end;

END;

$$ LANGUAGE plpgsql;

ALTER FUNCTION miovision_api.delete_anomalous_ranges
OWNER TO miovision_admins;

GRANT EXECUTE ON FUNCTION miovision_api.delete_anomalous_ranges TO miovision_api_bot;
