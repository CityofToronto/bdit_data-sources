--DROP FUNCTION miovision_api.cordon_counts_daily(date, date, text[]);

CREATE OR REPLACE FUNCTION miovision_api.cordon_counts_daily(
    p_start date, p_end date, camera_groups text[]
)
RETURNS TABLE (
    camera_group text,
    label text,
    dt date,
    auto_volume numeric,
    surface_transit_volume numeric,
    ped_volume numeric,
    bike_volume numeric,
    total_volume numeric,
    auto_intersections_missing int[],
    surface_transit_intersections_missing int[],
    ped_intersections_missing int[],
    bike_intersections_missing int[],
    total_intersections_missing int[],
    auto_intersections_present int,
    surface_transit_intersections_present int,
    ped_intersections_present int,
    bike_intersections_present int,
    total_intersections_present int
)
AS $$
    WITH cordon_camera_counts AS (
        SELECT
            camera_group,
            label,
            SUM(array_length(intersection_uids, 1)) AS camera_count
        FROM miovision_api.cordons
        GROUP BY
            camera_group,
            label
        
    )
    SELECT
        c15.camera_group,
        c15.label,
        c15.datetime_bin::date AS dt,
        SUM(c15.auto_volume) AS auto_volume,
        SUM(c15.surface_transit_volume) AS surface_transit_volume,
        SUM(c15.ped_volume) AS ped_volume,
        SUM(c15.bike_volume) AS bike_volume,
        SUM(c15.total_volume) AS total_volume,
        array_intersect_agg(c15.auto_intersections_missing) AS auto_intersections_missing,
        array_intersect_agg(c15.surface_transit_intersections_missing) AS surface_transit_intersections_missing,
        array_intersect_agg(c15.ped_intersections_missing) AS ped_intersections_missing,
        array_intersect_agg(c15.bike_intersections_missing) AS bike_intersections_missing,
        array_intersect_agg(c15.total_intersections_missing) AS total_intersections_missing,
        c.camera_count - COALESCE(array_length(array_intersect_agg(c15.auto_intersections_missing), 1), 0) AS auto_intersections_present,
        c.camera_count - COALESCE(array_length(array_intersect_agg(c15.surface_transit_intersections_missing), 1), 0) AS surface_transit_intersections_present,
        c.camera_count - COALESCE(array_length(array_intersect_agg(c15.ped_intersections_missing), 1), 0) AS ped_intersections_present,
        c.camera_count - COALESCE(array_length(array_intersect_agg(c15.bike_intersections_missing), 1), 0) AS bike_intersections_present,
        c.camera_count - COALESCE(array_length(array_intersect_agg(c15.total_intersections_missing), 1), 0) AS total_intersections_present
    FROM miovision_api.cordon_counts_15min AS c15
    LEFT JOIN cordon_camera_counts AS c USING (camera_group, label)
    WHERE
        c15.datetime_bin >= p_start
        AND c15.datetime_bin < p_end
        AND (
            c15.camera_group = ANY(camera_groups)
            OR camera_groups IS NULL
        )
    GROUP BY
        c15.camera_group,
        c15.label,
        c15.datetime_bin::date,
        c.camera_count
$$ LANGUAGE sql STABLE;

ALTER FUNCTION miovision_api.cordon_counts_daily OWNER TO miovision_admins;

GRANT EXECUTE ON FUNCTION miovision_api.cordon_counts_daily TO bdit_humans;

--example:
--3.5s
--SELECT * FROM miovision_api.cordon_counts_daily('2026-05-15', '2026-05-27', ARRAY['Bloor/Danforth', 'Eglinton'])