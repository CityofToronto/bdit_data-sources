CREATE TABLE miovision_api.volumes_daily (
    intersection_uid integer NOT NULL,
    dt date NOT NULL,
    period_start timestamp without time zone NOT NULL,
    period_end timestamp without time zone NOT NULL,
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
ON miovision_api.volumes_daily
USING btree(intersection_uid);

CREATE INDEX volumes_dt_idx
ON miovision_api.volumes_daily
USING btree(dt);

COMMENT ON COLUMN miovision_api.volumes_daily.volume_1 IS 'Daily volume for classification_uid 1';
COMMENT ON COLUMN miovision_api.volumes_daily.volume_2 IS 'Daily volume for classification_uid 2';
COMMENT ON COLUMN miovision_api.volumes_daily.volume_3 IS 'Daily volume for classification_uid 3';
COMMENT ON COLUMN miovision_api.volumes_daily.volume_4 IS 'Daily volume for classification_uid 4';
COMMENT ON COLUMN miovision_api.volumes_daily.volume_5 IS 'Daily volume for classification_uid 5';
COMMENT ON COLUMN miovision_api.volumes_daily.volume_6 IS 'Daily volume for classification_uid 6';
COMMENT ON COLUMN miovision_api.volumes_daily.volume_7 IS 'Daily volume for classification_uid 7';
COMMENT ON COLUMN miovision_api.volumes_daily.volume_8 IS 'Daily volume for classification_uid 8';
COMMENT ON COLUMN miovision_api.volumes_daily.volume_9 IS 'Daily volume for classification_uid 9';
COMMENT ON COLUMN miovision_api.volumes_daily.volume_10 IS 'Daily volume for classification_uid 10';
COMMENT ON COLUMN miovision_api.volumes_daily.volume_total IS 'Daily volume for all classification_uids.';

--insert data
SELECT miovision_api.aggregate_volumes_daily('2019-01-01'::date, '2023-11-06'::date);