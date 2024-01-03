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
ON mio_staging.volumes_15min_mvt USING btree(classification_uid ASC nulls last);

-- Index: volumes_15min_mvt_datetime_bin_idx
-- DROP INDEX IF EXISTS mio_staging.volumes_15min_mvt_datetime_bin_idx;
CREATE INDEX IF NOT EXISTS volumes_15min_mvt_datetime_bin_idx
ON mio_staging.volumes_15min_mvt USING brin(datetime_bin);

-- Index: volumes_15min_mvt_intersection_uid_idx
-- DROP INDEX IF EXISTS mio_staging.volumes_15min_mvt_intersection_uid_idx;
CREATE INDEX IF NOT EXISTS volumes_15min_mvt_intersection_uid_idx
ON mio_staging.volumes_15min_mvt USING btree(intersection_uid ASC nulls last);

-- Index: volumes_15min_mvt_leg_movement_uid_idx
-- DROP INDEX IF EXISTS mio_staging.volumes_15min_mvt_leg_movement_uid_idx;
CREATE INDEX IF NOT EXISTS volumes_15min_mvt_leg_movement_uid_idx
ON mio_staging.volumes_15min_mvt USING btree(
    leg COLLATE pg_catalog."default" ASC nulls last, --noqa: PRS
    movement_uid ASC nulls last
);

-- Index: volumes_15min_mvt_processed_idx
-- DROP INDEX IF EXISTS mio_staging.volumes_15min_mvt_processed_idx;
CREATE INDEX IF NOT EXISTS volumes_15min_mvt_processed_idx
ON mio_staging.volumes_15min_mvt USING btree(processed ASC nulls last)
WHERE processed IS NULL;

-- Index: volumes_15min_mvt_volume_15min_mvt_uid_idx
-- DROP INDEX IF EXISTS mio_staging.volumes_15min_mvt_volume_15min_mvt_uid_idx;
CREATE INDEX IF NOT EXISTS volumes_15min_mvt_volume_15min_mvt_uid_idx
ON mio_staging.volumes_15min_mvt USING btree(volume_15min_mvt_uid ASC nulls last);
    
--create partitions
--note this partition create function has been altered since.
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
            REVOKE ALL ON TABLE mio_staging.%I FROM bdit_humans;
            GRANT SELECT, INSERT, UPDATE ON TABLE mio_staging.%I TO miovision_api_bot;
            GRANT REFERENCES, SELECT ON TABLE mio_staging.%I TO bdit_humans WITH GRANT OPTION;
            GRANT ALL ON TABLE mio_staging.%I TO miovision_admins;
           $$, year_table, year_table, year_table, year_table
          );
	END LOOP;
END;
$do$ LANGUAGE plpgsql;

--this table was smaller, so inserted all at once, about 1hr. 
INSERT INTO mio_staging.volumes_15min_mvt
SELECT * FROM miovision_api.volumes_15min_mvt --noqa: L044
WHERE datetime_bin >= '2019-01-01'::date;

/*insert any new data 
INSERT INTO mio_staging.volumes_15min_mvt
SELECT * FROM miovision_api.volumes_15min_mvt WHERE datetime_bin >= '2023-10-30 23:00:00'::timestamp
*/

--CHECK ROW COUNT BEFORE PROCEEDING
--complete up until 2023-10-31 23:00:00. Will need to insert any new data before we changeover using script above. 
WITH a AS (
    SELECT COUNT(*) AS staging_count
    FROM mio_staging.volumes_15min_mvt
),

b AS (
    SELECT COUNT(*) AS miovision_api_count
    FROM miovision_api.volumes_15min_mvt
    WHERE datetime_bin >= '2019-01-01'::timestamp
)

SELECT
    a.staging_count,
    b.miovision_api_count
FROM a,
b;

--change home/owner of sequence
ALTER SEQUENCE IF EXISTS miovision_api.volumes_15min_mvt_volume_15min_mvt_uid_seq OWNED BY NONE; --noqa
ALTER SEQUENCE IF EXISTS miovision_api.volumes_15min_mvt_volume_15min_mvt_uid_seq SET SCHEMA mio_staging;
ALTER SEQUENCE IF EXISTS mio_staging.volumes_15min_mvt_volume_15min_mvt_uid_seq OWNED BY volumes_15min_mvt.volume_15min_mvt_uid;
ALTER SEQUENCE IF EXISTS mio_staging.volumes_15min_mvt_volume_15min_mvt_uid_seq OWNER TO miovision_admins;

/*Careful with these!!!

--save and drop dependencies
SELECT public.deps_save_and_drop_dependencies('miovision_api','volumes_15min_mvt');
--truncate and drop old volumes_15min_mvt
TRUNCATE TABLE miovision_api.volumes_15min_mvt;
DROP TABLE miovision_api.volumes_15min_mvt;
--switch over schema of new table:
ALTER TABLE mio_staging.volumes_15min_mvt SET SCHEMA miovision_api;
--restore dependencies
SELECT public.deps_restore_dependencies('miovision_api','volumes_15min_mvt');

*/

--parent table needs further permissions.
ALTER TABLE IF EXISTS miovision_api.volumes_15min_mvt OWNER TO miovision_admins;
REVOKE ALL ON TABLE miovision_api.volumes_15min_mvt FROM bdit_humans;
GRANT SELECT, INSERT, UPDATE ON TABLE miovision_api.volumes_15min_mvt TO miovision_api_bot;
GRANT REFERENCES, SELECT ON TABLE miovision_api.volumes_15min_mvt TO bdit_humans WITH GRANT OPTION;
GRANT ALL ON TABLE miovision_api.volumes_15min_mvt TO miovision_admins;

--need to change the schema and owners of all the partitions.
DO $do$
DECLARE
	yyyy TEXT;
    month_table TEXT;
    year_table TEXT;
BEGIN
	FOR yyyy IN 2019..2023 LOOP
        year_table := 'volumes_15min_mvt_'||yyyy::text;
        EXECUTE FORMAT($$ 
            ALTER TABLE IF EXISTS mio_staging.%I SET SCHEMA miovision_api;
            ALTER TABLE IF EXISTS miovision_api.%I OWNER TO miovision_admins;
           $$, year_table, year_table);
	END LOOP;
END;
$do$ LANGUAGE plpgsql;

ALTER SEQUENCE IF EXISTS mio_staging.volumes_15min_mvt_volume_15min_mvt_uid_seq SET SCHEMA miovision_api;
ALTER SEQUENCE IF EXISTS miovision_api.volumes_15min_mvt_volume_15min_mvt_uid_seq OWNED BY volumes_15min_mvt.volume_15min_mvt_uid;

--add volume_15min_mvt_uid to pkey so it can be used in fkey
ALTER TABLE miovision_api.volumes_15min_mvt DROP CONSTRAINT volumes_15min_mvt_int_uid_dt_bin_class_leg_mvmt_uid_pkey;
ALTER TABLE miovision_api.volumes_15min_mvt ADD CONSTRAINT volumes_15min_mvt_int_uid_dt_bin_class_leg_mvmt_uid_pkey
PRIMARY KEY (volume_15min_mvt_uid, intersection_uid, datetime_bin, classification_uid, leg, movement_uid);