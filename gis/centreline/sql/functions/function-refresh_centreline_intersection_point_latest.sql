CREATE FUNCTION gis_core.refresh_centreline_intersection_point_latest()
RETURNS void
LANGUAGE sql
COST 100
VOLATILE SECURITY DEFINER PARALLEL UNSAFE
AS $$

TRUNCATE gis_core.centreline_intersection_point_latest;

--in case of duplicate intersection_id (rare), only take the latest by objectid
INSERT INTO gis_core.centreline_intersection_point_latest (version_date, intersection_id, date_effective, date_expiry, intersection_desc, ward_number, ward, municipality, classification, classification_desc, number_of_elevations, x, y, longitude, latitude, trans_id_create, trans_id_expire, objectid, geom)
SELECT DISTINCT ON (intersection_id) version_date, intersection_id, date_effective, date_expiry, intersection_desc, ward_number, ward, municipality, classification, classification_desc, number_of_elevations, x, y, longitude, latitude, trans_id_create, trans_id_expire, objectid, geom
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
ORDER BY
    intersection_id ASC,
    objectid DESC;

COMMENT ON TABLE gis_core.centreline_intersection_point_latest IS E''
'Table containing the latest version of centreline intersection point,'
'derived from gis_core.centreline_intersection_point. Removes some (rare) duplicate intersection_ids.'
|| ' Last refreshed: ' || CURRENT_DATE || '.';

$$;

ALTER FUNCTION gis_core.refresh_centreline_intersection_point_latest OWNER TO gis_admins;

GRANT EXECUTE ON gis_core.refresh_centreline_intersection_point_latest TO gcc_bot;

REVOKE ALL ON FUNCTION gis_core.refresh_centreline_intersection_point_latest FROM public;

COMMENT ON FUNCTION gis_core.refresh_centreline_intersection_point_latest
IS 'Function to refresh gis_core.centreline_intersection_point_latest with truncate/insert.';
