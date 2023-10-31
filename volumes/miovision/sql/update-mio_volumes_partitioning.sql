-- Create staging schema for transitioning 
CREATE SCHEMA IF NOT EXISTS mio_staging AUTHORIZATION gwolofs;

COMMENT ON SCHEMA mio_staging
IS 'A staging schema for transitioning miovision_api.volumes table from inheritance partitioning to declarative partitioning. 
This schema should get dropped once the transition is finished. Created on 2023-10-31.';

--DROP TABLE mio_staging.volumes;
CREATE TABLE mio_staging.volumes (
    --create staging table based on original.
    LIKE miovision_api.volumes INCLUDING DEFAULTS,
    --change primary key to include datetime_bin for partioning
    CONSTRAINT volumes_intersection_uid_datetime_bin_classification_pkey 
        PRIMARY KEY (intersection_uid, datetime_bin, classification_uid, leg, movement_uid),
    CONSTRAINT volumes_volume_15min_mvt_uid_fkey
        FOREIGN KEY (volume_15min_mvt_uid)
        REFERENCES miovision_api.volumes_15min_mvt (volume_15min_mvt_uid) MATCH SIMPLE
        ON UPDATE NO ACTION
        ON DELETE SET NULL
)
PARTITION BY RANGE (datetime_bin);

CREATE INDEX volumes_datetime_bin_idx
  ON mio_staging.volumes
  USING brin(datetime_bin);

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

--create partitions
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
        EXECUTE FORMAT($$ GRANT INSERT, SELECT ON TABLE mio_staging.%I TO miovision_api_bot $$, year_table);
        FOR mm IN 01..12 LOOP
            month_table:= year_table||lpad(mm::text, 2, '0');
            --grant permissions on outer partitions
            EXECUTE FORMAT($$ GRANT INSERT, SELECT ON TABLE mio_staging.%I TO miovision_api_bot $$, month_table);
    	END LOOP;
	END LOOP;
END;
$do$ LANGUAGE plpgsql

--Test: 54min to insert 136,297,577 rows
INSERT INTO mio_staging.volumes
SELECT * FROM miovision_api.volumes_2020

--check row counts are the same

--TRUNCATE miovision_api.volumes

--ALTER TABLE mio_staging.volumes SET SCHEMA miovision_api;
