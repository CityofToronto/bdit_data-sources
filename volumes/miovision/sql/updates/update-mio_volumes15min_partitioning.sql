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
ON mio_staging.volumes_15min USING btree(classification_uid ASC nulls last);

-- Index: volumes_15min_datetime_bin_idx
-- DROP INDEX IF EXISTS mio_staging.volumes_15min_datetime_bin_idx;
CREATE INDEX IF NOT EXISTS volumes_15min_datetime_bin_idx
ON mio_staging.volumes_15min USING brin(datetime_bin);

-- Index: volumes_15min_intersection_uid_idx
-- DROP INDEX IF EXISTS mio_staging.volumes_15min_intersection_uid_idx;
CREATE INDEX IF NOT EXISTS volumes_15min_intersection_uid_idx
ON mio_staging.volumes_15min USING btree(intersection_uid ASC nulls last);

-- Index: volumes_15min_intersection_uid_leg_dir_idx
-- DROP INDEX IF EXISTS mio_staging.volumes_15min_intersection_uid_leg_dir_idx;
CREATE INDEX IF NOT EXISTS volumes_15min_intersection_uid_leg_dir_idx
ON mio_staging.volumes_15min USING btree(
    intersection_uid ASC nulls last,
    leg COLLATE pg_catalog."default" ASC nulls last, --noqa: PRS
    dir COLLATE pg_catalog."default" ASC nulls last
);

-- Index: volumes_15min_volume_15min_uid_idx
-- DROP INDEX IF EXISTS mio_staging.volumes_15min_volume_15min_uid_idx;
CREATE INDEX IF NOT EXISTS volumes_15min_volume_15min_uid_idx
ON mio_staging.volumes_15min USING btree(volume_15min_uid ASC nulls last);
    
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
            --ALTER TABLE IF EXISTS mio_staging.%I OWNER TO miovision_admins;
            REVOKE ALL ON TABLE mio_staging.%I FROM bdit_humans;
            GRANT SELECT, REFERENCES ON TABLE mio_staging.%I TO bdit_humans WITH GRANT OPTION;
            GRANT ALL ON TABLE mio_staging.%I TO miovision_admins;
           $$, year_table, year_table, year_table);
	END LOOP;
END;
$do$ LANGUAGE plpgsql;

--this table was smaller, so inserted all at once, about 1hr. 
INSERT INTO mio_staging.volumes_15min
SELECT * FROM miovision_api.volumes_15min --noqa: L044
WHERE datetime_bin >= '2019-01-01'::date;

/*insert any new data 
INSERT INTO mio_staging.volumes_15min
SELECT * FROM miovision_api.volumes_15min WHERE datetime_bin >= '2023-10-30 23:00:00'::timestamp
*/

--CHECK ROW COUNT BEFORE PROCEEDING
--complete up until 2023-10-31 23:00:00. Will need to insert any new data before we changeover using script above. 
WITH a AS (
    SELECT COUNT(*) AS staging_count
    FROM mio_staging.volumes_15min
),

b AS (
    SELECT COUNT(*) AS miovision_api_count
    FROM miovision_api.volumes_15min
    WHERE datetime_bin >= '2019-01-01'::timestamp
)

SELECT
    a.staging_count,
    b.miovision_api_count
FROM a,
b;

--change home/owner of sequence
ALTER SEQUENCE IF EXISTS miovision_api.volumes_15min_volume_15min_uid_seq OWNED BY NONE; --noqa
ALTER SEQUENCE IF EXISTS miovision_api.volumes_15min_volume_15min_uid_seq SET SCHEMA mio_staging;
ALTER SEQUENCE IF EXISTS mio_staging.volumes_15min_volume_15min_uid_seq OWNED BY volumes_15min.volume_15min_uid;
ALTER SEQUENCE IF EXISTS mio_staging.volumes_15min_volume_15min_uid_seq OWNER TO miovision_admins;

/*Careful with these!!!

--save and drop dependencies
SELECT public.deps_save_and_drop_dependencies('miovision_api','volumes_15min');
--truncate and drop old volumes_15min
TRUNCATE TABLE miovision_api.volumes_15min;
DROP TABLE miovision_api.volumes_15min;
--switch over schema of new table:
ALTER TABLE mio_staging.volumes_15min SET SCHEMA miovision_api;
--restore dependencies
SELECT public.deps_restore_dependencies('miovision_api','volumes_15min');

*/

--parent table needs further permissions.
ALTER TABLE IF EXISTS miovision_api.volumes_15min OWNER TO miovision_admins;
REVOKE ALL ON TABLE miovision_api.volumes_15min FROM bdit_humans;
GRANT ALL ON TABLE miovision_api.volumes_15min TO miovision_api_bot;
GRANT SELECT, REFERENCES ON TABLE miovision_api.volumes_15min TO bdit_humans WITH GRANT OPTION;
GRANT ALL ON TABLE miovision_api.volumes_15min TO miovision_admins;

--need to change the schema and owners of all the partitions since wasn't able to do that previously. 
DO $do$
DECLARE
	yyyy TEXT;
    year_table TEXT;
BEGIN
	FOR yyyy IN 2019..2023 LOOP
        year_table := 'volumes_15min_'||yyyy::text;
        EXECUTE FORMAT($$ 
            ALTER TABLE IF EXISTS mio_staging.%I SET SCHEMA miovision_api;
            ALTER TABLE IF EXISTS miovision_api.%I OWNER TO miovision_admins;
           $$, year_table, year_table);
	END LOOP;
END;
$do$ LANGUAGE plpgsql;

ALTER SEQUENCE IF EXISTS mio_staging.volumes_15min_volume_15min_uid_seq SET SCHEMA miovision_api;
ALTER SEQUENCE IF EXISTS miovision_api.volumes_15min_volume_15min_uid_seq OWNED BY volumes_15min.volume_15min_uid;