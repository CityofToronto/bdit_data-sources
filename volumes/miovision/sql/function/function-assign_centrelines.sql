CREATE OR REPLACE FUNCTION miovision_api.assign_centrelines()
RETURNS void AS $$

    WITH valid_legs AS (
        SELECT
            intersection_uid,
            UNNEST(array_remove(ARRAY[
                CASE WHEN e_leg_restricted IS NULL THEN 'E' END,
                CASE WHEN n_leg_restricted IS NULL THEN 'N' END,
                CASE WHEN s_leg_restricted IS NULL THEN 'S' END,
                CASE WHEN w_leg_restricted IS NULL THEN 'W' END
            ], NULL)) AS leg,
            px
        FROM miovision_api.intersections
    )
    
    INSERT INTO miovision_api.centreline_miovision (centreline_id, intersection_uid, leg)
    SELECT
        cld.leg_centreline_id,
        vl.intersection_uid,
        vl.leg
    FROM valid_legs AS vl
    --anti join existing rows
    LEFT JOIN miovision_api.centreline_miovision AS cl USING (intersection_uid, leg)
    --get centreline-legs using px -> intersection_id -> centreline_leg_directions
    JOIN traffic.traffic_signal AS ts USING (px)
    JOIN gis_core.centreline_leg_directions AS cld
        ON cld.intersection_centreline_id = ts."centrelineId"
        AND vl.leg = UPPER(LEFT(cld.leg, 1))
    WHERE cl.intersection_uid IS NULL --not in table (allowing for nulls)

$$ LANGUAGE sql
SECURITY DEFINER;

COMMENT ON FUNCTION miovision_api.assign_centrelines
IS 'Function to insert new centrelines into `miovision_api.centreline_miovision` based on closest aligned values from `gis_core.centreline_leg_directions`.';

ALTER FUNCTION miovision_api.assign_centrelines
OWNER TO miovision_admins;

GRANT EXECUTE ON FUNCTION miovision_api.assign_centrelines
TO miovision_api_bot;
