-- Aggregate `vds.raw_vdsvehicledata` into table `vds.veh_length_15min`
-- by detector / 15min bins / MTO length bins.
INSERT INTO vds.veh_length_15min (
    division_id, vdsconfig_uid, entity_location_uid,
    datetime_15min, mto_class_uid, count, total_count
)

SELECT
    v.division_id,
    v.vdsconfig_uid,
    v.entity_location_uid,
    datetime_bin(v.dt, 15) AS datetime_15min, --uses floor
    mto.mto_class_uid,
    COUNT(v.*) AS count,
    SUM(COUNT(v.*)) OVER (
        PARTITION BY v.division_id, v.vdsconfig_uid, datetime_bin(v.dt, 15)
    ) AS total_count
FROM vds.raw_vdsvehicledata AS v
--left join to preserve null lengths in summary table
LEFT JOIN traffic.mto_length_bin_classification AS mto
    ON v.length_meter::numeric <@ mto.length_range
WHERE
    v.dt >= '{{ ds }} 00:00:00'::timestamp -- noqa: TMP
    AND v.dt < '{{ ds }} 00:00:00'::timestamp + interval '1 DAY' -- noqa: TMP
    AND v.vdsconfig_uid IS NOT NULL
    AND v.entity_location_uid IS NOT NULL
GROUP BY
    v.division_id,
    v.vdsconfig_uid,
    v.entity_location_uid,
    datetime_15min,
    mto.mto_class_uid
ON CONFLICT DO NOTHING;