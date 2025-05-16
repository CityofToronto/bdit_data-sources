INSERT INTO vds.volumes_daily (
    vdsconfig_uid, entity_location_uid, detector_id, dt, is_wkdy, daily_count, count_60day,
    daily_obs, daily_obs_expected, avg_lanes_present, distinct_dt_bins_present
)
SELECT
    c15.vdsconfig_uid,
    c15.entity_location_uid,
    di.detector_id,
    dts.dt,
    dts.is_wkdy,
    SUM(c15.count_15min) AS daily_count,
    AVG(SUM(c15.count_15min)) OVER (
        PARTITION BY c15.vdsconfig_uid, c15.entity_location_uid, dts.is_wkdy
        ORDER BY dts.dt
        RANGE BETWEEN '60 days'::interval PRECEDING AND CURRENT ROW
    ) AS count_60day,
    SUM(c15.num_obs) AS daily_obs,
    SUM(c15.expected_bins * c15.num_lanes) AS daily_obs_expected,
    round(AVG(c15.num_distinct_lanes), 1) AS avg_lanes_present,
    COUNT(DISTINCT c15.datetime_15min) AS distinct_dt_bins_present
FROM vds.counts_15min_div2 AS c15
JOIN vds.detector_inventory AS di USING (vdsconfig_uid, entity_location_uid),
LATERAL (
    SELECT
        c15.datetime_15min::date AS dt,
        date_part('isodow'::text, c15.datetime_15min) <= 5::double precision AS is_wkdy
    ) AS dts
WHERE
    c15.datetime_15min >= '{{ ds }} 00:00:00'::timestamp -- noqa: TMP
    AND c15.datetime_15min < '{{ ds }} 00:00:00'::timestamp + interval '1 DAY' -- noqa: TMP
GROUP BY
    c15.vdsconfig_uid,
    c15.entity_location_uid,
    di.detector_id,
    dts.dt,
    dts.is_wkdy;
