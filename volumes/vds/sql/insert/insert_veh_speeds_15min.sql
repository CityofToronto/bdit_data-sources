--Aggregate `vds.raw_vdsvehicledata` into table `vds.veh_speeds_15min` by detector / 15min bins / 5kph speed bins.

INSERT INTO vds.veh_speeds_15min (division_id, vdsconfig_uid, entity_location_uid, datetime_15min, speed_5kph, count, total_count)

SELECT
    division_id,
    vdsconfig_uid,
    entity_location_uid, 
    datetime_bin(dt, 15) AS datetime_15min,
    FLOOR(speed_kmh / 5.0) * 5 AS speed_5kph,
    COUNT(*) AS count,
    SUM(COUNT(*)) OVER (
        PARTITION BY division_id, vdsconfig_uid, entity_location_uid, datetime_bin(dt, 15)
    ) AS total_count
FROM vds.raw_vdsvehicledata
WHERE
    dt >= '{{ ds }} 00:00:00'::timestamp -- noqa: TMP
    AND dt < '{{ ds }} 00:00:00'::timestamp + interval '1 DAY' -- noqa: TMP
    AND speed_kmh IS NOT NULL
    AND vdsconfig_uid IS NOT NULL
    AND entity_location_uid IS NOT NULL
GROUP BY 
    division_id,
    vdsconfig_uid,
    entity_location_uid,
    datetime_15min,
    speed_5kph
ON CONFLICT DO NOTHING;
