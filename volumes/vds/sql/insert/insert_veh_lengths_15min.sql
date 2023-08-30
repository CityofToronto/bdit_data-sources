--Aggregate `vds.raw_vdsvehicledata` into table `vds.veh_length_15min` by detector / 15min bins / 1m length bins.
INSERT INTO vds.veh_length_15min (division_id, vdsconfig_uid, entity_location_uid, datetime_15min, length_meter, count,
    total_count)

SELECT
    division_id,
    vdsconfig_uid,
    entity_location_uid, 
    datetime_bin(dt, 15) AS datetime_15min, --uses floor
    FLOOR(length_meter) AS length_meter,
    COUNT(*) AS count,
    SUM(COUNT(*)) OVER (
        PARTITION BY division_id, vdsconfig_uid, datetime_bin(dt, 15)
    ) AS total_count
FROM vds.raw_vdsvehicledata
WHERE
    dt >= '{{ ds }} 00:00:00'::timestamp -- noqa: TMP
    AND dt < '{{ ds }} 00:00:00'::timestamp + interval '1 DAY' -- noqa: TMP
    AND length_meter IS NOT NULL
    AND vdsconfig_uid IS NOT NULL
    AND entity_location_uid IS NOT NULL
GROUP BY 
    division_id,
    vdsconfig_uid,
    entity_location_uid,
    datetime_15min,
    length_meter
ON CONFLICT DO NOTHING;