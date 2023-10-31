CREATE TABLE mio_staging.volumes_daily (
    intersection_uid integer NOT NULL,
    dt date NOT NULL,
    volume_1 integer, 
    volume_2 integer, 
    volume_3 integer, 
    volume_4 integer, 
    volume_5 integer, 
    volume_6 integer, 
    volume_7 integer, 
    volume_8 integer, 
    volume_9 integer, 
    volume_10 integer, 
    volume_total integer,
    CONSTRAINT volumes_daily_pkey
        PRIMARY KEY (intersection_uid, dt)
);

CREATE INDEX volumes_intersection_idx
ON mio_staging.volumes_daily
USING btree(intersection_uid);

CREATE INDEX volumes_dt_idx
ON mio_staging.volumes_daily
USING brin(dt);

INSERT INTO mio_staging.volumes_daily
SELECT
    intersection_uid,
    datetime_bin::date,
    SUM(volume) FILTER (WHERE classification_uid = 1) AS volume_1,
    SUM(volume) FILTER (WHERE classification_uid = 2) AS volume_2,
    SUM(volume) FILTER (WHERE classification_uid = 3) AS volume_3,
    SUM(volume) FILTER (WHERE classification_uid = 4) AS volume_4,
    SUM(volume) FILTER (WHERE classification_uid = 5) AS volume_5,
    SUM(volume) FILTER (WHERE classification_uid = 6) AS volume_6,
    SUM(volume) FILTER (WHERE classification_uid = 7) AS volume_7,
    SUM(volume) FILTER (WHERE classification_uid = 8) AS volume_8,
    SUM(volume) FILTER (WHERE classification_uid = 9) AS volume_9,
    SUM(volume) FILTER (WHERE classification_uid = 10) AS volume_10,
    SUM(volume) AS volume_total
FROM miovision_api.volumes_15min
GROUP BY 
    intersection_uid,
    datetime_bin::date;