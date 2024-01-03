-- Create staging schema for transitioning 
CREATE SCHEMA IF NOT EXISTS mio_staging AUTHORIZATION gwolofs; --noqa: PRS

COMMENT ON SCHEMA mio_staging
IS 'A staging schema for transitioning miovision_api.volumes table from inheritance partitioning to declarative partitioning. 
This schema should get dropped once the transition is finished. Created on 2023-10-31.';

--DROP TABLE mio_staging.volumes;
CREATE TABLE mio_staging.volumes (
    --create staging table based on original.
    LIKE miovision_api.volumes INCLUDING DEFAULTS,
    --change primary key to include datetime_bin for partioning
    CONSTRAINT volumes_intersection_uid_datetime_bin_classification_pkey 
    PRIMARY KEY (intersection_uid, datetime_bin, classification_uid, leg, movement_uid)
)
PARTITION BY RANGE (datetime_bin);

ALTER TABLE mio_staging.volumes ALTER COLUMN volume_uid SET DEFAULT nextval('miovision_api.volumes_volume_uid_seq'::regclass);

CREATE INDEX volumes_datetime_bin_idx
ON mio_staging.volumes USING brin(datetime_bin);

-- Index: mio_staging.volumes_intersection_uid_classification_uid_leg_movement_ui_idx
-- DROP INDEX mio_staging.volumes_intersection_uid_classification_uid_leg_movement_ui_idx;
CREATE INDEX volumes_intersection_uid_classification_uid_leg_movement_ui_idx
ON mio_staging.volumes
USING btree(intersection_uid, classification_uid, leg COLLATE pg_catalog."default", movement_uid);

-- Index: mio_staging.volumes_intersection_uid_idx
-- DROP INDEX mio_staging.volumes_intersection_uid_idx;
CREATE INDEX volumes_intersection_uid_idx
ON mio_staging.volumes
USING btree(intersection_uid);

-- Index: mio_staging.volumes_volume_15min_mvt_uid_idx
-- DROP INDEX mio_staging.volumes_volume_15min_mvt_uid_idx;
CREATE INDEX volumes_volume_15min_mvt_uid_idx
ON mio_staging.volumes
USING btree(volume_15min_mvt_uid);

--the old pkey becomes a unique constraint
-- Index: mio_staging.volume_uid_unique
-- DROP INDEX mio_staging.volume_uid_unique;
CREATE INDEX volume_uid_unique
ON mio_staging.volumes
USING btree(volume_uid);

--create year + month partitions
DO $do$
DECLARE
	yyyy TEXT;
BEGIN
	FOR yyyy IN 2019..2023 LOOP
        EXECUTE FORMAT($$SELECT gwolofs.create_yyyymm_nested_partitions(
                            'volumes'::text,
                            %L::int,
                            'datetime_bin'::text,
                            'mio_staging'::text,
                            'gwolofs'::text
                        )$$,
                       yyyy);
	END LOOP;
END;
$do$ LANGUAGE plpgsql

--grant permissions
DO $do$
DECLARE
	yyyy TEXT;
    month_table TEXT;
    year_table TEXT;
BEGIN
	FOR yyyy IN 2019..2023 LOOP
        year_table := 'volumes_'||yyyy::text;
        --grant permissions on outer partitions
        EXECUTE FORMAT($$
            GRANT INSERT, SELECT ON TABLE mio_staging.%I TO miovision_api_bot;
            REVOKE ALL ON TABLE mio_staging.%I FROM bdit_humans;
            GRANT SELECT, REFERENCES ON TABLE mio_staging.%I TO bdit_humans WITH GRANT OPTION;
            GRANT ALL ON TABLE mio_staging.%I TO miovision_admins;
            $$, year_table, year_table, year_table, year_table);
        FOR mm IN 01..12 LOOP
            month_table:= year_table||lpad(mm::text, 2, '0');
            --grant permissions on inner partitions
            EXECUTE FORMAT($$
                GRANT INSERT, SELECT ON TABLE mio_staging.%I TO miovision_api_bot;
                REVOKE ALL ON TABLE mio_staging.%I FROM bdit_humans;
                GRANT SELECT, REFERENCES ON TABLE mio_staging.%I TO bdit_humans WITH GRANT OPTION;
                GRANT ALL ON TABLE mio_staging.%I TO miovision_admins;
                $$, month_table, month_table, month_table, month_table);
    	END LOOP;
	END LOOP;
END;
$do$ LANGUAGE plpgsql

--Inserted in batches by year like so:
INSERT INTO mio_staging.volumes
SELECT * FROM miovision_api.volumes
WHERE
    datetime_bin >= '2019-01-01'::timestamp
    AND datetime_bin < '2020-01-01'::timestamp
    AND classification_uid IS NOT NULL --some rows violated this new pkey constraint

/*may need to insert newest data
INSERT INTO mio_staging.volumes
SELECT * FROM miovision_api.volumes WHERE datetime_bin >= '2023-11-01'::timestamp
*/

--CHECK ROW COUNT BEFORE PROCEEDING
--check row count is the same, insert any new rows above if not.
WITH a AS (
    SELECT COUNT(*) AS staging_count
    FROM mio_staging.volumes
),
b AS (
    SELECT COUNT(*) AS miovision_api_count
    FROM miovision_api.volumes
    WHERE
        datetime_bin >= '2019-01-01'::timestamp
        AND classification_uid IS NOT NULL
)

SELECT a.staging_count, b.miovision_api_count
FROM a,
b;

--change index ownership: 
ALTER SEQUENCE IF EXISTS miovision_api.volumes_volume_uid_seq OWNED BY NONE; --noqa
ALTER SEQUENCE IF EXISTS miovision_api.volumes_volume_uid_seq OWNER TO gwolofs;
ALTER SEQUENCE IF EXISTS miovision_api.volumes_volume_uid_seq SET SCHEMA mio_staging;
ALTER SEQUENCE IF EXISTS mio_staging.volumes_volume_uid_seq OWNED BY volumes.volume_uid;
ALTER SEQUENCE IF EXISTS mio_staging.volumes_volume_uid_seq OWNER TO miovision_admins;

--truncate and drop all the volumes tables; Point of no return!
/*
ALTER TABLE miovision_api.volumes_2018 NO INHERIT miovision_api.volumes; TRUNCATE TABLE miovision_api.volumes_2018; DROP TABLE miovision_api.volumes_2018;
ALTER TABLE miovision_api.volumes_2019 NO INHERIT miovision_api.volumes; TRUNCATE TABLE miovision_api.volumes_2019; DROP TABLE miovision_api.volumes_2019;
ALTER TABLE miovision_api.volumes_2020 NO INHERIT miovision_api.volumes; TRUNCATE TABLE miovision_api.volumes_2020; DROP TABLE miovision_api.volumes_2020;
ALTER TABLE miovision_api.volumes_2021 NO INHERIT miovision_api.volumes; TRUNCATE TABLE miovision_api.volumes_2021; DROP TABLE miovision_api.volumes_2021;
ALTER TABLE miovision_api.volumes_2022 NO INHERIT miovision_api.volumes; TRUNCATE TABLE miovision_api.volumes_2022; DROP TABLE miovision_api.volumes_2022;
ALTER TABLE miovision_api.volumes_2023 NO INHERIT miovision_api.volumes; TRUNCATE TABLE miovision_api.volumes_2023; DROP TABLE miovision_api.volumes_2023;
DROP TABLE miovision_api.volumes;
*/

ALTER TABLE mio_staging.volumes SET SCHEMA miovision_api;
ALTER TABLE miovision_api.volumes OWNER TO miovision_admins;

--CHANGE OWNER OF PARTITIONS
DO $do$
DECLARE
	yyyy TEXT;
    month_table TEXT;
    year_table TEXT;
BEGIN
	FOR yyyy IN 2019..2023 LOOP
        year_table := 'volumes_'||yyyy::text;
        --grant permissions on outer partitions
        EXECUTE FORMAT($$
            ALTER TABLE IF EXISTS mio_staging.%I SET SCHEMA miovision_api;
            ALTER TABLE miovision_api.%I OWNER TO miovision_admins;
            $$, year_table, year_table);
        FOR mm IN 01..12 LOOP
            month_table:= year_table||lpad(mm::text, 2, '0');
            --grant permissions on inner partitions
            EXECUTE FORMAT($$
            ALTER TABLE IF EXISTS mio_staging.%I SET SCHEMA miovision_api;
            ALTER TABLE miovision_api.%I OWNER TO miovision_admins;
                $$, month_table, month_table);
    	END LOOP;
	END LOOP;
END;
$do$ LANGUAGE plpgsql

ALTER SEQUENCE IF EXISTS mio_staging.volumes_volume_uid_seq SET SCHEMA miovision_api;
ALTER SEQUENCE IF EXISTS miovision_api.volumes_volume_uid_seq OWNED BY volumes.volume_uid;
