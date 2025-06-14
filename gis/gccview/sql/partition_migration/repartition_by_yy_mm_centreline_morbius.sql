--store the names of `gis.centreline` partitions
DROP TABLE IF EXISTS gis.centreline_partitions;
CREATE TABLE gis.centreline_partitions AS
SELECT child.relname, substring(child.relname, 0, 16) AS yr, substring(child.relname, 12, 8)::date AS version_date
FROM pg_inherits
JOIN pg_class AS parent ON pg_inherits.inhparent = parent.oid
JOIN pg_class AS child ON pg_inherits.inhrelid = child.oid
JOIN pg_namespace AS nmsp_parent ON nmsp_parent.oid = parent.relnamespace
WHERE
    nmsp_parent.nspname = 'gis'
    AND parent.relname = 'centreline';

--save dependencies
SELECT public.deps_save_and_drop_dependencies_dryrun(
	p_view_schema:= 'gis'::character varying COLLATE "C",
	p_view_name:= 'centreline'::character varying COLLATE "C", 
	dryrun := False::boolean, 
	max_depth := 20::integer
);

ALTER TABLE gis.centreline RENAME TO centreline_old;

--create new partitioned table
CREATE TABLE IF NOT EXISTS gis.centreline
(
    version_date date,
    centreline_id integer,
    linear_name_id integer,
    linear_name_full text COLLATE pg_catalog."default",
    linear_name_full_legal text COLLATE pg_catalog."default",
    address_l text COLLATE pg_catalog."default",
    address_r text COLLATE pg_catalog."default",
    parity_l text COLLATE pg_catalog."default",
    parity_r text COLLATE pg_catalog."default",
    lo_num_l integer,
    hi_num_l integer,
    lo_num_r integer,
    hi_num_r integer,
    begin_addr_point_id_l integer,
    end_addr_point_id_l integer,
    begin_addr_point_id_r integer,
    end_addr_point_id_r integer,
    begin_addr_l integer,
    end_addr_l integer,
    begin_addr_r integer,
    end_addr_r integer,
    linear_name text COLLATE pg_catalog."default",
    linear_name_type text COLLATE pg_catalog."default",
    linear_name_dir text COLLATE pg_catalog."default",
    linear_name_desc text COLLATE pg_catalog."default",
    linear_name_label text COLLATE pg_catalog."default",
    from_intersection_id integer,
    to_intersection_id integer,
    oneway_dir_code integer,
    oneway_dir_code_desc text COLLATE pg_catalog."default",
    feature_code integer,
    feature_code_desc text COLLATE pg_catalog."default",
    jurisdiction text COLLATE pg_catalog."default",
    centreline_status text COLLATE pg_catalog."default",
    shape_length numeric,
    objectid integer,
    shape_len numeric,
    mi_prinx integer,
    low_num_odd integer,
    high_num_odd integer,
    low_num_even integer,
    high_num_even integer,
    geom geometry
) PARTITION BY RANGE (version_date);
--partition by range instead of list this time

ALTER TABLE IF EXISTS gis.centreline OWNER TO gis_admins;
REVOKE ALL ON TABLE gis.centreline FROM ptc_humans, collision_humans;
GRANT SELECT ON TABLE gis.centreline TO ptc_humans, collision_humans;
GRANT ALL ON TABLE gis.centreline TO gis_admins;

--create new partitions, which are also subpartitioned
DO $$
DECLARE
    partition_rec RECORD;
BEGIN
    FOR partition_rec IN
        SELECT DISTINCT yr, version_date::date as vd 
        FROM gis.centreline_partitions
    LOOP 
        EXECUTE format('CREATE TABLE IF NOT EXISTS gis.%I PARTITION OF gis.centreline
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
        FROM gis.centreline_partitions
    LOOP 
        EXECUTE format('ALTER TABLE gis.centreline_old DETACH PARTITION gis.%I', 
            partition_rec.relname);
        EXECUTE format('ALTER TABLE gis.%I ATTACH PARTITION gis.%I FOR VALUES IN (%L)',
            partition_rec.yr, partition_rec.relname, partition_rec.vd);
    END LOOP;
END$$;


--now restore dependencies:
SELECT public.deps_restore_dependencies(
	p_view_schema:= 'gis'::character varying COLLATE "C",
	p_view_name:= 'centreline'::character varying COLLATE "C"
);

--do this manually after checking no rows
--DROP TABLE gis.centreline_old;
