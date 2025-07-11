INSERT INTO vds.volumes_daily (
    vdsconfig_uid, entity_location_uid, detector_id, dt, is_wkdy, is_holiday,
    daily_volume, count_60day, daily_obs, daily_obs_expected, avg_lanes_present, 
    num_lanes_expected, distinct_dt_bins_present
)
SELECT
    c15.vdsconfig_uid,
    c15.entity_location_uid,
    di.detector_id,
    dts.dt,
    dts.is_wkdy,
    dts.is_holiday,
    SUM(c15.count_15min) AS daily_volume,
    AVG(SUM(c15.count_15min)) OVER (
        PARTITION BY c15.vdsconfig_uid, c15.entity_location_uid, dts.is_wkdy
        ORDER BY dts.dt
        RANGE BETWEEN '60 days'::interval PRECEDING AND CURRENT ROW --noqa: PRS
    ) AS count_60day,
    SUM(c15.num_obs) AS daily_obs,
    SUM(c15.expected_bins * c15.num_lanes) AS daily_obs_expected,
    round(AVG(c15.num_distinct_lanes), 1) AS avg_lanes_present,
    c15.num_lanes AS num_lanes_expected,
    COUNT(DISTINCT c15.datetime_15min) AS distinct_dt_bins_present
FROM vds.counts_15min_div2 AS c15
JOIN vds.detector_inventory AS di USING (vdsconfig_uid, entity_location_uid),
    LATERAL (
        SELECT
            c15.datetime_15min::date AS dt,
            EXISTS (SELECT 1 FROM ref.holiday WHERE holiday.dt = c15.datetime_15min::date) AS is_holiday,
            date_part('isodow'::text, c15.datetime_15min) <= 5::double precision AS is_wkdy
    ) AS dts
WHERE
    c15.datetime_15min >= '{{ ds }} 00:00:00'::timestamp -- noqa: TMP
    AND c15.datetime_15min < '{{ ds }} 00:00:00'::timestamp + interval '1 DAY' -- noqa: TMP
GROUP BY
    c15.vdsconfig_uid,
    c15.entity_location_uid,
    di.detector_id,
    c15.num_lanes,
    dts.dt,
    dts.is_wkdy,
    dts.is_holiday;
