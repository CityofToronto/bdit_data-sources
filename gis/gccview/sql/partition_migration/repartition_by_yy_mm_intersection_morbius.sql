--store the names of `gis.intersection` partitions
DROP TABLE IF EXISTS gis.intersection_partitions;
CREATE TABLE gis.intersection_partitions AS
SELECT child.relname, substring(child.relname, 0, 18) AS yr, substring(child.relname, 14, 8)::date AS version_date
FROM pg_inherits
JOIN pg_class AS parent ON pg_inherits.inhparent = parent.oid
JOIN pg_class AS child ON pg_inherits.inhrelid = child.oid
JOIN pg_namespace AS nmsp_parent ON nmsp_parent.oid = parent.relnamespace
WHERE
    nmsp_parent.nspname = 'gis'
    AND parent.relname = 'intersection';

--save dependencies
SELECT public.deps_save_and_drop_dependencies_dryrun(
	p_view_schema:= 'gis'::character varying COLLATE "C",
	p_view_name:= 'intersection'::character varying COLLATE "C", 
	dryrun := False::boolean, 
	max_depth := 20::integer
);

ALTER TABLE gis.intersection RENAME TO intersection_old;

--create new partitioned table
CREATE TABLE IF NOT EXISTS gis.intersection
(
    version_date date,
    intersection_id integer,
    date_effective timestamp without time zone,
    date_expiry timestamp without time zone,
    trans_id_create integer,
    trans_id_expire integer,
    x numeric,
    y numeric,
    longitude numeric,
    latitude numeric,
    centreline_id_from integer,
    linear_name_full_from text COLLATE pg_catalog."default",
    linear_name_id_from numeric,
    turn_direction text COLLATE pg_catalog."default",
    centreline_id_to integer,
    linear_name_full_to text COLLATE pg_catalog."default",
    linear_name_id_to numeric,
    connected text COLLATE pg_catalog."default",
    objectid integer,
    elevation_id integer,
    elevation_level integer,
    classification text COLLATE pg_catalog."default",
    classification_desc text COLLATE pg_catalog."default",
    number_of_elevations integer,
    elevation_feature_code integer,
    elevation_feature_code_desc text COLLATE pg_catalog."default",
    elevation numeric,
    elevation_unit text COLLATE pg_catalog."default",
    height_restriction numeric,
    height_restriction_unit text COLLATE pg_catalog."default",
    feature_class_from text COLLATE pg_catalog."default",
    feature_class_to text COLLATE pg_catalog."default",
    geom geometry
) PARTITION BY RANGE (version_date);

ALTER TABLE IF EXISTS gis.intersection
    OWNER to gis_admins;

REVOKE ALL ON TABLE gis.intersection FROM collision_humans;
REVOKE ALL ON TABLE gis.intersection FROM ptc_airflow_bot;
REVOKE ALL ON TABLE gis.intersection FROM ptc_humans;

GRANT SELECT ON TABLE gis.intersection TO collision_humans;

GRANT ALL ON TABLE gis.intersection TO gis_admins;

GRANT ALL ON TABLE gis.intersection TO natalie;

GRANT SELECT ON TABLE gis.intersection TO ptc_airflow_bot;

GRANT SELECT ON TABLE gis.intersection TO ptc_humans;
-- Index: intersection_version_date_idx1

-- DROP INDEX IF EXISTS gis.intersection_version_date_idx1;

CREATE INDEX IF NOT EXISTS intersection_version_date_idx1
    ON gis.intersection USING brin
    (version_date)
;
-- Index: intersection_version_date_idx2

-- DROP INDEX IF EXISTS gis.intersection_version_date_idx2;

CREATE INDEX IF NOT EXISTS intersection_version_date_idx2
    ON gis.intersection USING btree
    (version_date ASC NULLS LAST);

--create new partitions, which are also subpartitioned
DO $$
DECLARE
    partition_rec RECORD;
BEGIN
    FOR partition_rec IN
        SELECT DISTINCT yr, version_date::date as vd 
        FROM gis.intersection_partitions
    LOOP 
        EXECUTE format('CREATE TABLE IF NOT EXISTS gis.%I PARTITION OF gis.intersection
                        FOR VALUES FROM (%L) TO (%L)
                        PARTITION BY LIST (version_date);', 
                partition_rec.yr,
                date_trunc('year', partition_rec.vd),
                date_trunc('year', partition_rec.vd) + interval '1 year');
    END LOOP;
END$$;

--detach each partition and attach to new table
DO $$
DECLARE
    partition_rec RECORD;
BEGIN
    FOR partition_rec IN
        SELECT relname, yr, version_date::date as vd 
        FROM gis.intersection_partitions
    LOOP 
        EXECUTE format('ALTER TABLE gis.intersection_old DETACH PARTITION gis.%I;', 
            partition_rec.relname);
        EXECUTE format('ALTER TABLE gis.%I ATTACH PARTITION gis.%I FOR VALUES IN (%L);',
            partition_rec.yr, partition_rec.relname, partition_rec.vd);
    END LOOP;
END$$;


--now restore dependencies:
SELECT public.deps_restore_dependencies(
	p_view_schema:= 'gis'::character varying COLLATE "C",
	p_view_name:= 'intersection'::character varying COLLATE "C"
)

--do this manually after checking no rows
--DROP TABLE gis.intersection_old;
