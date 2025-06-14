--store the names of `gis.centreline_intersection_point` partitions
DROP TABLE IF EXISTS gis.centreline_intersection_point_partitions;
CREATE TABLE gis.centreline_intersection_point_partitions AS
SELECT child.relname, substring(child.relname, 0, 35) AS yr, substring(child.relname, 31, 8)::date AS version_date
FROM pg_inherits
JOIN pg_class AS parent ON pg_inherits.inhparent = parent.oid
JOIN pg_class AS child ON pg_inherits.inhrelid = child.oid
JOIN pg_namespace AS nmsp_parent ON nmsp_parent.oid = parent.relnamespace
WHERE
    nmsp_parent.nspname = 'gis'
    AND parent.relname = 'centreline_intersection_point';

--save dependencies
SELECT public.deps_save_and_drop_dependencies_dryrun(
	p_view_schema:= 'gis'::character varying COLLATE "C",
	p_view_name:= 'centreline_intersection_point'::character varying COLLATE "C", 
	dryrun := False::boolean, 
	max_depth := 20::integer
);

ALTER TABLE gis.centreline_intersection_point RENAME TO centreline_intersection_point_old;

--create new partitioned table
CREATE TABLE IF NOT EXISTS gis.centreline_intersection_point
(
    version_date date,
    intersection_id integer,
    date_effective timestamp without time zone,
    date_expiry timestamp without time zone,
    intersection_desc text COLLATE pg_catalog."default",
    ward_number text COLLATE pg_catalog."default",
    ward text COLLATE pg_catalog."default",
    municipality text COLLATE pg_catalog."default",
    classification text COLLATE pg_catalog."default",
    classification_desc text COLLATE pg_catalog."default",
    number_of_elevations integer,
    x numeric,
    y numeric,
    longitude numeric,
    latitude numeric,
    trans_id_create integer,
    trans_id_expire integer,
    objectid integer NOT NULL,
    geom geometry
) PARTITION BY RANGE (version_date);
--partition by range this time!

ALTER TABLE IF EXISTS gis.centreline_intersection_point
    OWNER to gis_admins;

REVOKE ALL ON TABLE gis.centreline_intersection_point FROM ptc_humans, collision_humans;

GRANT SELECT ON TABLE gis.centreline_intersection_point TO ptc_humans, collision_humans;

GRANT ALL ON TABLE gis.centreline_intersection_point TO gis_admins;

COMMENT ON TABLE gis.centreline_intersection_point
    IS 'Intersection Layer pulled from (https://insideto-gis.toronto.ca/arcgis/rest/services/cot_geospatial/FeatureServer/19)
Contains additional boundary information such as ward, and municpality, as well as trails and ferry routes';


ALTER TABLE IF EXISTS gis.centreline_intersection_point OWNER TO gis_admins;
REVOKE ALL ON TABLE gis.centreline_intersection_point FROM ptc_humans, collision_humans;
GRANT SELECT ON TABLE gis.centreline_intersection_point TO ptc_humans, collision_humans;
GRANT ALL ON TABLE gis.centreline_intersection_point TO gis_admins;

--create new partitions, which are also subpartitioned
DO $$
DECLARE
    partition_rec RECORD;
BEGIN
    FOR partition_rec IN
        SELECT DISTINCT yr, version_date::date as vd 
        FROM gis.centreline_intersection_point_partitions
    LOOP 
        EXECUTE format('CREATE TABLE IF NOT EXISTS gis.%I PARTITION OF gis.centreline_intersection_point
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
        FROM gis.centreline_intersection_point_partitions
    LOOP 
        EXECUTE format('ALTER TABLE gis.centreline_intersection_point_old DETACH PARTITION gis.%I', 
            partition_rec.relname);
        EXECUTE format('ALTER TABLE gis.%I ATTACH PARTITION gis.%I FOR VALUES IN (%L)',
            partition_rec.yr, partition_rec.relname, partition_rec.vd);
    END LOOP;
END$$;


--now restore dependencies:
SELECT public.deps_restore_dependencies(
	p_view_schema:= 'gis'::character varying COLLATE "C",
	p_view_name:= 'centreline_intersection_point'::character varying COLLATE "C"
)

--do this manually after checking no rows
--DROP TABLE gis.centreline_intersection_point_old;
