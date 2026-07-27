CREATE OR REPLACE FUNCTION miovision_api.cordon_counts_daily(
    p_start date, p_end date
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
    total_intersections_missing int[]
)
AS $$
    SELECT
        camera_group,
        label,
        datetime_bin::date AS dt,
        SUM(auto_volume) AS auto_volume,
        SUM(surface_transit_volume) AS surface_transit_volume,
        SUM(ped_volume) AS ped_volume,
        SUM(bike_volume) AS bike_volume,
        SUM(total_volume) AS total_volume,
        array_intersect_agg(auto_intersections_missing) AS auto_intersections_missing,
        array_intersect_agg(surface_transit_intersections_missing) AS surface_transit_intersections_missing,
        array_intersect_agg(ped_intersections_missing) AS ped_intersections_missing,
        array_intersect_agg(bike_intersections_missing) AS bike_intersections_missing,
        array_intersect_agg(total_intersections_missing) AS total_intersections_missing
    FROM miovision_api.cordon_counts_15min
    WHERE
        datetime_bin >= p_start
        AND datetime_bin < p_end
    GROUP BY
        camera_group,
        label,
        datetime_bin::date
$$ LANGUAGE sql STABLE;

ALTER FUNCTION miovision_api.cordon_counts_daily OWNER TO miovision_admins;

GRANT SELECT ON FUNCTION miovision_api.cordon_counts_daily TO bdit_humans;
