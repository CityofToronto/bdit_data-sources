WITH inserted AS (
    INSERT INTO vds.raw_vdsdata (
        division_id, vds_id, dt, datetime_15min, lane, speed_kmh, volume_veh_per_hr,
        occupancy_percent
    ) VALUES %s
    RETURNING division_id, vdsconfig_uid, entity_location_uid, dt
)

INSERT INTO vds.last_active AS la (
    division_id, vdsconfig_uid, entity_location_uid, first_active, last_active
)
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
ON CONFLICT (vdsconfig_uid, entity_location_uid, division_id)
DO UPDATE SET
first_active = LEAST(excluded.first_active, la.first_active),
last_active = GREATEST(excluded.last_active, la.last_active);