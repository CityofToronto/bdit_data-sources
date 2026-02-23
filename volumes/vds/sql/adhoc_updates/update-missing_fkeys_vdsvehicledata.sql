/*
You will need to follow this adhoc process if you discover records in raw_vdsvehicledata
that are missing vdsconfig_uid or entity_locations_uid foreign keys, in order to
add fkeys and then summarize those records in to veh_length_15min, veh_speeds_15min.

This particular case occured when raw table was backfilled without latest lookups. 
*/

WITH updates AS ( --noqa: L045
    SELECT
        d.volume_uid,
        c.uid AS vdsconfig_uid,
        e.uid AS entity_location_uid
    FROM vds.raw_vdsvehicledata AS d
    JOIN vds.vdsconfig AS c ON
        d.vds_id = c.vds_id
        AND d.division_id = c.division_id
        AND d.dt >= c.start_timestamp
        AND (
            d.dt < c.end_timestamp
            OR c.end_timestamp IS NULL) --no end date
    JOIN vds.entity_locations AS e ON
        d.vds_id = e.entity_id
        AND d.division_id = e.division_id
        AND d.dt >= e.start_timestamp
        AND (
            d.dt < e.end_timestamp
            OR e.end_timestamp IS NULL) --no end date
    WHERE (
            d.vdsconfig_uid IS NULL 
            OR d.entity_location_uid IS NULL
        )
        --want both to be not null so we can safely insert into counts_15min
        AND c.uid IS NOT NULL
        AND e.uid IS NOT NULL
),

--update this foreign keys in the raw table.
updated AS (
    UPDATE vds.raw_vdsvehicledata AS dest --noqa: PRS
    SET
        vdsconfig_uid = updates.vdsconfig_uid,
        entity_location_uid = updates.entity_location_uid
    FROM updates
    WHERE dest.volume_uid = updates.volume_uid
    --return updated rows to summarize
    RETURNING
        dest.division_id,
        dest.vdsconfig_uid,
        dest.entity_location_uid,
        dest.dt,
        dest.length_meter,
        dest.speed_kmh
),

--insert updated records into veh_length_15min
veh_length_15min_update AS ( --noqa: L045

    INSERT INTO vds.veh_length_15min (division_id, vdsconfig_uid, entity_location_uid, datetime_15min, length_meter, count,  --noqa: PRS
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
    FROM updated
    WHERE
        length_meter IS NOT NULL
        AND vdsconfig_uid IS NOT NULL
        AND entity_location_uid IS NOT NULL
    GROUP BY 
        division_id,
        vdsconfig_uid,
        entity_location_uid,
        datetime_15min,
        length_meter
    ON CONFLICT DO NOTHING
)

--insert updated records into veh_speeds_15min
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
FROM updated
WHERE
    speed_kmh IS NOT NULL
    AND vdsconfig_uid IS NOT NULL
    AND entity_location_uid IS NOT NULL
GROUP BY 
    division_id,
    vdsconfig_uid,
    entity_location_uid,
    datetime_15min,
    speed_5kph
ON CONFLICT DO NOTHING;
