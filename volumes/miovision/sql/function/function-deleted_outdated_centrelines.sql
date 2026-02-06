CREATE OR REPLACE FUNCTION miovision_api.delete_outdated_centreline_ids()
RETURNS void AS $$

    WITH outdated AS (
        SELECT
            cm.centreline_id,
            cm.intersection_uid,
            cm.leg
        FROM miovision_api.centreline_miovision AS cm
        LEFT JOIN gis_core.centreline_latest AS latest USING (centreline_id)
        WHERE
            cm.centreline_id IS NOT NULL
            --entry exists, but is no longer valid
            AND latest.centreline_id IS NULL
    )
    
    DELETE FROM miovision_api.centreline_miovision AS cm
    USING outdated AS od
    WHERE od.centreline_id = cm.centreline_id
    AND od.intersection_uid = cm.intersection_uid
    AND od.leg = cm.leg;

$$
LANGUAGE sql
SECURITY DEFINER;

COMMENT ON FUNCTION miovision_api.delete_outdated_centreline_ids
IS 'Function to deleted outdated centrelines from `miovision_api.centreline_miovision`.';

ALTER FUNCTION miovision_api.delete_outdated_centreline_ids
OWNER TO miovision_admins;

GRANT EXECUTE ON FUNCTION miovision_api.delete_outdated_centreline_ids
TO miovision_api_bot;
