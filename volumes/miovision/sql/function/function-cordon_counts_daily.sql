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
    total_volume numeric
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
        SUM(total_volume) AS total_volume
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
