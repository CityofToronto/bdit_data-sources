-- View: collisions_replicator.collision_no

-- DROP MATERIALIZED VIEW IF EXISTS collisions_replicator.collision_no;

CREATE MATERIALIZED VIEW IF NOT EXISTS collisions_replicator.collision_no
TABLESPACE pg_default
AS
 WITH unique_collisions AS (
         SELECT DISTINCT a."ACCNB"::bigint AS accnb,
            date_part('year'::text, a."ACCDATE"::date) AS accyear
           FROM collisions_replicator.acc_safe_copy a
          WHERE a."ACCDATE"::date >= '1985-01-01'::date AND a."ACCDATE"::date <= 'now'::text::date
        )
 SELECT c.accyear,
    c.accnb,
    row_number() OVER (ORDER BY c.accyear, c.accnb) AS collision_no
   FROM unique_collisions c
WITH DATA;

ALTER TABLE IF EXISTS collisions_replicator.collision_no
    OWNER TO collision_admins;

COMMENT ON MATERIALIZED VIEW collisions_replicator.collision_no
    IS 'Collision number to link involved and events matviews. Data updated daily at 3am.';

GRANT SELECT ON TABLE collisions_replicator.collision_no TO kchan;
GRANT SELECT ON TABLE collisions_replicator.collision_no TO bdit_humans;
GRANT SELECT ON TABLE collisions_replicator.collision_no TO rsaunders;
GRANT SELECT ON TABLE collisions_replicator.collision_no TO ksun;
GRANT ALL ON TABLE collisions_replicator.collision_no TO collision_admins;

CREATE UNIQUE INDEX col_no_idx
    ON collisions_replicator.collision_no USING btree
    (collision_no)
    TABLESPACE pg_default;

CREATE INDEX collisions_collision_no_idx
    ON collisions_replicator.collision_no USING btree
    (accnb, accyear)
    TABLESPACE pg_default;

