-- Creation script for collisions.collision_no.

CREATE MATERIALIZED VIEW collisions.collision_no
TABLESPACE pg_default
AS
    -- Select all unique ACCNB and year combinations - these should be unique to each collision event.
    WITH unique_collisions AS (
        SELECT DISTINCT a."ACCNB"::bigint AS accnb,
                        date_part('year', a."ACCDATE"::date) AS accyear
        FROM collisions.acc a
        WHERE a."ACCDATE"::date >= '1985-01-01'::date AND a."ACCDATE"::date <= current_date
    )
    SELECT c.accyear,
           c.accnb,
           row_number() OVER (ORDER BY c.accyear, c.accnb) AS collision_no
    FROM unique_collisions c
WITH DATA;


ALTER TABLE collisions.collision_no
    OWNER TO czhu;

COMMENT ON MATERIALIZED VIEW collisions.collision_no
    IS 'Collision number to link involved and events matviews.';


GRANT ALL ON TABLE collisions.collision_no TO czhu;
GRANT SELECT ON TABLE collisions.collision_no TO bdit_humans;
GRANT SELECT ON TABLE collisions.collision_no TO rsaunders;
GRANT SELECT ON TABLE collisions.collision_no TO kchan;


CREATE INDEX collisions_collision_no_idx
    ON collisions.collision_no USING btree
    (accnb, accyear)
    TABLESPACE pg_default;
