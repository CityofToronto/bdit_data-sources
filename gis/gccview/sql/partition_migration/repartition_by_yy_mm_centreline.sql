--deps save and drop doesn't support "-"; temporarily rename
--ALTER MATERIALIZED VIEW zghayen."mto-friction_data_joined_centreline" RENAME TO mto_friction_data_joined_centreline; 
--ALTER MATERIALIZED VIEW zghayen."mto-friction_data_joined_collisions" RENAME TO mto_friction_data_joined_collisions;

--store the names of `gis_core.centreline` partitions
DROP TABLE IF EXISTS gis_core.centreline_partitions;
CREATE TABLE gis_core.centreline_partitions AS
SELECT child.relname, substring(child.relname, 0, 16) AS yr, substring(child.relname, 12, 8)::date AS version_date
FROM pg_inherits
JOIN pg_class AS parent ON pg_inherits.inhparent = parent.oid
JOIN pg_class AS child ON pg_inherits.inhrelid = child.oid
JOIN pg_namespace AS nmsp_parent ON nmsp_parent.oid = parent.relnamespace
WHERE
    nmsp_parent.nspname = 'gis_core'
    AND parent.relname = 'centreline';

--save dependencies
SELECT public.deps_save_and_drop_dependencies_dryrun(
	p_view_schema:= 'gis_core'::character varying COLLATE "C",
	p_view_name:= 'centreline'::character varying COLLATE "C", 
	dryrun := False::boolean, 
	max_depth := 20::integer
);

ALTER TABLE gis_core.centreline RENAME TO centreline_old;

--create new partitioned table
CREATE TABLE IF NOT EXISTS gis_core.centreline
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

ALTER TABLE IF EXISTS gis_core.centreline OWNER TO gis_admins;
REVOKE ALL ON TABLE gis_core.centreline FROM bdit_humans;
REVOKE ALL ON TABLE gis_core.centreline FROM events_bot;
GRANT SELECT ON TABLE gis_core.centreline TO bdit_humans;
GRANT SELECT ON TABLE gis_core.centreline TO events_bot;
GRANT ALL ON TABLE gis_core.centreline TO gis_admins;

--create new partitions, which are also subpartitioned
DO $$
DECLARE
    partition_rec RECORD;
BEGIN
    FOR partition_rec IN
        SELECT DISTINCT yr, version_date::date as vd 
        FROM gis_core.centreline_partitions
    LOOP 
        EXECUTE format('CREATE TABLE IF NOT EXISTS gis_core.%I PARTITION OF gis_core.centreline
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
        FROM gis_core.centreline_partitions
    LOOP 
        EXECUTE format('ALTER TABLE gis_core.centreline_old DETACH PARTITION gis_core.%I', 
            partition_rec.relname);
        EXECUTE format('ALTER TABLE gis_core.%I ATTACH PARTITION gis_core.%I FOR VALUES IN (%L)',
            partition_rec.yr, partition_rec.relname, partition_rec.vd);
    END LOOP;
END$$;


--now restore dependencies:
SELECT public.deps_restore_dependencies(
	p_view_schema:= 'gis_core'::character varying COLLATE "C",
	p_view_name:= 'centreline'::character varying COLLATE "C"
);

ALTER MATERIALIZED VIEW zghayen.mto_friction_data_joined_centreline RENAME TO "mto-friction_data_joined_centreline"; 
ALTER MATERIALIZED VIEW zghayen.mto_friction_data_joined_collisions RENAME TO "mto-friction_data_joined_collisions";

--do this manually after checking no rows
--DROP TABLE gis_core.centreline_old;
