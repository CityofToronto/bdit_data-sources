-- Script for creating a table of event-level variables from collisions.acc.

-- DROP MATERIALIZED VIEW collisions.events;

CREATE MATERIALIZED VIEW collisions.events
TABLESPACE pg_default
AS
    WITH valid_rows AS (
        SELECT *
        FROM collisions.acc
        WHERE "ACCDATE"::date >= '1985-01-01'::date AND "ACCDATE"::date <= current_date
    ), event_desc AS (
        SELECT a."ACCNB"::bigint accnb,
               date_part('year', a."ACCDATE"::date) accyear,
               -- Purposely not combining date and time to ensure compatibility with older queries.
               a."ACCDATE"::date accdate,
               a."ACCTIME"::time acctime,
               a."LONGITUDE" + 0.00021::double precision longitude,
               a."LATITUDE" + 0.000045::double precision latitude,
               a."STNAME1" stname1,
               a."STREETYPE1" streetype1,
               a."DIR1" dir1,
               a."STNAME2" stname2,
               a."STREETYPE2" streetype2,
               a."DIR2" dir2,
               a."STNAME3" stname3,
               a."STREETYPE3" streetype3,
               a."DIR3" dir3,
               a."ROAD_CLASS" road_class,
               upper(btrim(b.description)) location_type,
               upper(btrim(c.description)) location_class,
               upper(btrim(d.description)) collision_type,
               upper(btrim(e.description)) visibility,
               upper(btrim(f.description)) light,
               upper(btrim(g.description)) road_surface_cond,
               upper(btrim(h.description)) traffic_control,
               upper(btrim(i.description)) traffic_control_cond,
               CASE
                WHEN a."PRIVATE_PROPERTY" = 'Y' THEN True
                WHEN a."PRIVATE_PROPERTY" = 'N' THEN False
                ELSE NULL
               END on_private_property
        FROM valid_rows a
        LEFT JOIN collision_factors.accloc b ON a."ACCLOC"::text = b.accloc
        LEFT JOIN collision_factors.loccoord c ON a."LOCCOORD"::text = c.loccoord
        LEFT JOIN collision_factors.acclass d ON a."ACCLASS"::text = d.acclass
        LEFT JOIN collision_factors.visible e ON a."VISIBLE"::text = e.visible
        LEFT JOIN collision_factors.light f ON a."LIGHT"::text = f.light
        LEFT JOIN collision_factors.rdsfcond g ON a."RDSFCOND"::text = g.rdsfcond
        LEFT JOIN collision_factors.traffictl h ON a."TRAFFICTL"::text = h.traffictl
        LEFT JOIN collision_factors.trafctlcond i ON a."TRAFCTLCOND"::text = i.trafctlcond
    -- There's only one non-NULL description per collision event, so must select for it.
    ), events_desc AS (
        SELECT a."ACCNB"::bigint accnb,
               date_part('year', a."ACCDATE"::date) accyear,
               -- Will just retrieve the non-NULL result.
               MAX(a."DESCRIPTION") description
        FROM valid_rows a
        GROUP BY a."ACCNB"::bigint, date_part('year', a."ACCDATE"::date)
    )
    SELECT DISTINCT b.collision_no,
           a.accnb,
           a.accyear,
           a.accdate,
           a.acctime,
           a.longitude,
           a.latitude,
           a.stname1,
           a.streetype1,
           a.dir1,
           a.stname2,
           a.streetype2,
           a.dir2,
           a.stname3,
           a.streetype3,
           a.dir3,
           a.road_class,
           a.location_type,
           a.location_class,
           a.collision_type,
           a.visibility,
           a.light,
           a.road_surface_cond,
           a.traffic_control,
           a.traffic_control_cond,
           a.on_private_property,
           c.description
    FROM event_desc a
    LEFT JOIN collisions.collision_no b USING (accnb, accyear)
    LEFT JOIN events_desc c USING (accnb, accyear)
    -- This should only matter if collision.events is refreshed more than a day after collision.collision_no.
    WHERE b.collision_no IS NOT NULL
    ORDER BY b.collision_no
WITH DATA;

ALTER TABLE collisions.events
    OWNER TO czhu;

COMMENT ON MATERIALIZED VIEW collisions.events
    IS 'Event-level variables in collisions.acc.';

GRANT ALL ON TABLE collisions.events TO czhu;
GRANT SELECT ON TABLE collisions.events TO bdit_humans;

CREATE INDEX collision_events_idx
    ON collisions.events USING btree
    (collision_no)
    TABLESPACE pg_default;
