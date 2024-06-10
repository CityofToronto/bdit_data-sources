WITH inserted AS (
    INSERT INTO vds.raw_vdsdata (
        division_id, vds_id, dt, datetime_15min, lane, speed_kmh, volume_veh_per_hr, occupancy_percent
    ) VALUES %s
    RETURNING division_id, vdsconfig_uid, entity_location_uid, dt
),

updated_dates AS (
    SELECT
        division_id,
        vdsconfig_uid,
        entity_location_uid,
        MIN(dt) AS min_dt,
        MAX(dt) AS max_dt
    FROM inserted
    WHERE
        division_id + vdsconfig_uid + entity_location_uid IS NOT NULL
    GROUP BY
        division_id,
        vdsconfig_uid,
        entity_location_uid
)

UPDATE vds.last_active AS x
SET
    first_active = LEAST(ud.min_dt, x.first_active),
    last_active = GREATEST(ud.max_dt, x.last_active)
FROM updated_dates AS ud
WHERE
    x.division_id = ud.division_id
    AND x.vdsconfig_uid = ud.vdsconfig_uid
    AND x.entity_location_uid = ud.entity_location_uid;