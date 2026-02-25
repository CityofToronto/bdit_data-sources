CREATE FUNCTION gis_core.refresh_centreline_intersection_point_latest AS

TRUNCATE gis_core.centreline_intersection_point_latest;

--in case of duplicate intersection_id (rare), only take the latest by objectid
SELECT DISTINCT ON (intersection_id) *
FROM gis_core.centreline_intersection_point
WHERE
    intersection_id IN (
        SELECT cent_f.from_intersection_id
        FROM gis_core.centreline_latest AS cent_f
        UNION
        SELECT cent_t.to_intersection_id
        FROM gis_core.centreline_latest AS cent_t
    )
    AND version_date = (
        SELECT MAX(centreline_intersection_point.version_date)
        FROM gis_core.centreline_intersection_point
    )
ORDER BY intersection_id ASC, objectid DESC;

COMMENT ON TABLE gis_core.centreline_intersection_point_latest IS E''
'Materialized view containing the latest version of centreline intersection point,'
'derived from gis_core.centreline_intersection_point. Removes some (rare) duplicate intersection_ids.'

$$ LANGUAGE sql;

ALTER FUNCTION gis_core.refresh_centreline_intersection_point_latest OWNER TO gis_admins;

GRANT EXECUTE ON gis_core.refresh_centreline_intersection_point_latest TO gcc_bot;

REVOKE ALL ON FUNCTION gis_core.refresh_centreline_intersection_point_latest FROM public;

COMMENT ON FUNCTION gis_core.refresh_centreline_intersection_point_latest
IS 'Function to refresh gis_core.centreline_intersection_point_latest with truncate/insert.';
