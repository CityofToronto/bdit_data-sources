--DROP TABLE mio_staging.volumes_15min;
CREATE TABLE mio_staging.volumes_15min (
    --create staging table based on original.
    LIKE miovision_api.volumes_15min INCLUDING DEFAULTS,
    --change primary key to include datetime_bin for partioning
    CONSTRAINT volumes_15min_intersection_uid_datetime_bin_classification_pkey
        PRIMARY KEY (intersection_uid, datetime_bin, classification_uid, leg, dir)
)
PARTITION BY RANGE (datetime_bin);
    
COMMENT ON COLUMN mio_staging.volumes_15min.leg
    IS 'leg location, e.g. E leg means both entry and exit traffic across the east side of the intersection. ';

-- Index: volumes_15min_classification_uid_idx
-- DROP INDEX IF EXISTS mio_staging.volumes_15min_classification_uid_idx;
CREATE INDEX IF NOT EXISTS volumes_15min_classification_uid_idx
    ON mio_staging.volumes_15min USING btree
    (classification_uid ASC NULLS LAST);

-- Index: volumes_15min_datetime_bin_idx
-- DROP INDEX IF EXISTS mio_staging.volumes_15min_datetime_bin_idx;
CREATE INDEX IF NOT EXISTS volumes_15min_datetime_bin_idx
    ON mio_staging.volumes_15min USING brin(datetime_bin);

-- Index: volumes_15min_intersection_uid_idx
-- DROP INDEX IF EXISTS mio_staging.volumes_15min_intersection_uid_idx;
CREATE INDEX IF NOT EXISTS volumes_15min_intersection_uid_idx
    ON mio_staging.volumes_15min USING btree
    (intersection_uid ASC NULLS LAST);

-- Index: volumes_15min_intersection_uid_leg_dir_idx
-- DROP INDEX IF EXISTS mio_staging.volumes_15min_intersection_uid_leg_dir_idx;
CREATE INDEX IF NOT EXISTS volumes_15min_intersection_uid_leg_dir_idx
    ON mio_staging.volumes_15min USING btree
    (intersection_uid ASC NULLS LAST, leg COLLATE pg_catalog."default" ASC NULLS LAST, dir COLLATE pg_catalog."default" ASC NULLS LAST);

-- Index: volumes_15min_volume_15min_uid_idx
-- DROP INDEX IF EXISTS mio_staging.volumes_15min_volume_15min_uid_idx;
CREATE INDEX IF NOT EXISTS volumes_15min_volume_15min_uid_idx
    ON mio_staging.volumes_15min USING btree
    (volume_15min_uid ASC NULLS LAST);
    
--create partitions
DO $do$
DECLARE
	yyyy TEXT;
BEGIN
	FOR yyyy IN 2019..2023 LOOP
        EXECUTE FORMAT($$SELECT gwolofs.create_yyyy_partition(
                            'volumes_15min'::text,
                            %L::int,
                            'mio_staging'::text,
                            'gwolofs'::text
                        )$$,
                       yyyy);
	END LOOP;
END;
$do$ LANGUAGE plpgsql;

--grant permissions
DO $do$
DECLARE
	yyyy TEXT;
    month_table TEXT;
    year_table TEXT;
BEGIN
	FOR yyyy IN 2019..2023 LOOP
        year_table := 'volumes_15min_'||yyyy::text;
        --grant permissions on outer partitions
        EXECUTE FORMAT($$ 
            --ALTER TABLE IF EXISTS mio_staging.%I OWNER to miovision_admins;
            REVOKE ALL ON TABLE mio_staging.%I FROM bdit_humans;
            GRANT ALL ON TABLE mio_staging.%I TO bdit_bots;
            GRANT TRIGGER, SELECT, REFERENCES ON TABLE mio_staging.%I TO bdit_humans WITH GRANT OPTION;
            GRANT ALL ON TABLE mio_staging.%I TO miovision_admins;
            GRANT ALL ON TABLE mio_staging.%I TO rds_superuser WITH GRANT OPTION;
           $$, year_table, year_table, year_table, year_table, year_table, year_table);
	END LOOP;
END;
$do$ LANGUAGE plpgsql;

/*
--test: 10min, inserted 40,051,723 rows 
INSERT INTO mio_staging.volumes_15min
SELECT * FROM miovision_api.volumes_15min
WHERE
    datetime_bin >= '2020-01-01'::date
    AND datetime_bin < '2021-01-01'::date
*/

INSERT INTO mio_staging.volumes_15min
SELECT * FROM miovision_api.volumes_15min
WHERE datetime_bin >= '2019-01-01'::date;