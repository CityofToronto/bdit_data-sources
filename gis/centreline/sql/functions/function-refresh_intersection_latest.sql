CREATE FUNCTION gis_core.refresh_intersection_latest()
RETURNS VOID AS $$

TRUNCATE gis_core.intersection_latest;

INSERT INTO gis_core.intersection_latest (version_date, intersection_id, date_effective, date_expiry, trans_id_create, trans_id_expire, x, y, longitude, latitude, centreline_id_from, linear_name_full_from, linear_name_id_from, turn_direction, centreline_id_to, linear_name_full_to, linear_name_id_to, connected, objectid, elevation_id, elevation_level, classification, classification_desc, number_of_elevations, elevation_feature_code, elevation_feature_code_desc, elevation, elevation_unit, height_restriction, height_restriction_unit, feature_class_from, feature_class_to, geom)
SELECT version_date, intersection_id, date_effective, date_expiry, trans_id_create, trans_id_expire, x, y, longitude, latitude, centreline_id_from, linear_name_full_from, linear_name_id_from, turn_direction, centreline_id_to, linear_name_full_to, linear_name_id_to, connected, objectid, elevation_id, elevation_level, classification, classification_desc, number_of_elevations, elevation_feature_code, elevation_feature_code_desc, elevation, elevation_unit, height_restriction, height_restriction_unit, feature_class_from, feature_class_to, geom
FROM gis_core.intersection
WHERE
    intersection_id IN (
        SELECT from_intersection_id
        FROM gis_core.centreline_latest
        UNION
        SELECT to_intersection_id
        FROM gis_core.centreline_latest
    )
    AND version_date = (
        SELECT MAX(version_date)
        FROM gis_core.intersection
    );

COMMENT ON FUNCTION gis_core.refresh_intersection_latest IS E''
'Materialized view containing the latest version of intersection,'
'derived from gis_core.intersection.'
|| ' Last updated: ' || CURRENT_DATE;

$$ LANGUAGE sql;

ALTER FUNCTION gis_core.refresh_intersection_latest OWNER TO gis_admins;

GRANT EXECUTE ON gis_core.refresh_intersection_latest TO gcc_bot;

REVOKE ALL ON FUNCTION gis_core.refresh_intersection_latest FROM public;

COMMENT ON FUNCTION gis_core.refresh_intersection_latest
IS 'Function to refresh gis_core.centreline_latest_all_feature with truncate/insert.';
