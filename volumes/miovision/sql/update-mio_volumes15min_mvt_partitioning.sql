--DROP TABLE mio_staging.volumes_15min_mvt;
CREATE TABLE mio_staging.volumes_15min_mvt (
    --create staging table based on original.
    LIKE miovision_api.volumes_15min_mvt INCLUDING DEFAULTS,
    --change primary key to include datetime_bin for partioning
    CONSTRAINT volumes_15min_mvt_intersection_uid_datetime_bin_classificat_pk
        PRIMARY KEY (intersection_uid, datetime_bin, classification_uid, leg, movement_uid)
)
PARTITION BY RANGE (datetime_bin);

COMMENT ON COLUMN mio_staging.volumes_15min_mvt.leg
    IS 'Leg of approach or entry point ';

-- Index: volumes_15min_mvt_classification_uid_idx
-- DROP INDEX IF EXISTS mio_staging.volumes_15min_mvt_classification_uid_idx;
CREATE INDEX IF NOT EXISTS volumes_15min_mvt_classification_uid_idx
    ON mio_staging.volumes_15min_mvt USING btree
    (classification_uid ASC NULLS LAST);

-- Index: volumes_15min_mvt_datetime_bin_idx
-- DROP INDEX IF EXISTS mio_staging.volumes_15min_mvt_datetime_bin_idx;
CREATE INDEX IF NOT EXISTS volumes_15min_mvt_datetime_bin_idx
    ON mio_staging.volumes_15min_mvt USING brin
    (datetime_bin);

-- Index: volumes_15min_mvt_intersection_uid_idx
-- DROP INDEX IF EXISTS mio_staging.volumes_15min_mvt_intersection_uid_idx;
CREATE INDEX IF NOT EXISTS volumes_15min_mvt_intersection_uid_idx
    ON mio_staging.volumes_15min_mvt USING btree
    (intersection_uid ASC NULLS LAST);

-- Index: volumes_15min_mvt_leg_movement_uid_idx
-- DROP INDEX IF EXISTS mio_staging.volumes_15min_mvt_leg_movement_uid_idx;
CREATE INDEX IF NOT EXISTS volumes_15min_mvt_leg_movement_uid_idx
    ON mio_staging.volumes_15min_mvt USING btree
    (leg COLLATE pg_catalog."default" ASC NULLS LAST, movement_uid ASC NULLS LAST);

-- Index: volumes_15min_mvt_processed_idx
-- DROP INDEX IF EXISTS mio_staging.volumes_15min_mvt_processed_idx;
CREATE INDEX IF NOT EXISTS volumes_15min_mvt_processed_idx
    ON mio_staging.volumes_15min_mvt USING btree
    (processed ASC NULLS LAST)
    WHERE processed IS NULL;

-- Index: volumes_15min_mvt_volume_15min_mvt_uid_idx
-- DROP INDEX IF EXISTS mio_staging.volumes_15min_mvt_volume_15min_mvt_uid_idx;
CREATE INDEX IF NOT EXISTS volumes_15min_mvt_volume_15min_mvt_uid_idx
    ON mio_staging.volumes_15min_mvt USING btree
    (volume_15min_mvt_uid ASC NULLS LAST);
    
--create partitions
DO $do$
DECLARE
	yyyy TEXT;
BEGIN
	FOR yyyy IN 2019..2023 LOOP
        EXECUTE FORMAT($$SELECT gwolofs.create_yyyy_partition(
                            'volumes_15min_mvt'::text,
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
    year_table TEXT;
BEGIN
	FOR yyyy IN 2019..2023 LOOP
        year_table:= 'volumes_15min_mvt_'||yyyy::text;
        --grant permissions on outer partitions
        EXECUTE FORMAT($$ 
            --ALTER TABLE IF EXISTS mio_staging.%I OWNER to miovision_admins;
            REVOKE ALL ON TABLE mio_staging.%I FROM bdit_humans;
            GRANT ALL ON TABLE mio_staging.%I TO bdit_bots;
            GRANT REFERENCES, TRIGGER, SELECT ON TABLE mio_staging.%I TO bdit_humans WITH GRANT OPTION;
            GRANT ALL ON TABLE mio_staging.%I TO miovision_admins;
            GRANT ALL ON TABLE mio_staging.%I TO rds_superuser WITH GRANT OPTION;
           $$, year_table, year_table, year_table, year_table, year_table, year_table
          );
	END LOOP;
END;
$do$ LANGUAGE plpgsql;

INSERT INTO mio_staging.volumes_15min_mvt
SELECT * FROM miovision_api.volumes_15min_mvt
WHERE datetime_bin >= '2019-01-01'::date;