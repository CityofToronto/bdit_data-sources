-- Script for creating a table of event-level variables from collisions.acc.

-- View: collisions_replicator.events

-- DROP MATERIALIZED VIEW IF EXISTS collisions_replicator.events;

CREATE MATERIALIZED VIEW IF NOT EXISTS collisions_replicator.events
TABLESPACE pg_default
AS
WITH valid_rows AS (
    SELECT
        acc_safe_copy."ACCNB",
        acc_safe_copy."RELACCNB",
        acc_safe_copy."ACCDATE",
        acc_safe_copy."DAY_NO",
        acc_safe_copy."ACCTIME",
        acc_safe_copy."PATAREA",
        acc_safe_copy."STNAME1",
        acc_safe_copy."STREETYPE1",
        acc_safe_copy."DIR1",
        acc_safe_copy."STNAME2",
        acc_safe_copy."STREETYPE2",
        acc_safe_copy."DIR2",
        acc_safe_copy."STNAME3",
        acc_safe_copy."STREETYPE3",
        acc_safe_copy."DIR3",
        acc_safe_copy."PER_INV",
        acc_safe_copy."VEH_INV",
        acc_safe_copy."MUNICIPAL",
        acc_safe_copy."LOCCOORD",
        acc_safe_copy."IMPCTAREA",
        acc_safe_copy."ACCLASS",
        acc_safe_copy."ACCLOC",
        acc_safe_copy."TRAFFICTL",
        acc_safe_copy."DRIVAGE",
        acc_safe_copy."VEH_NO",
        acc_safe_copy."VEHTYPE",
        acc_safe_copy."TOWEDVEH",
        acc_safe_copy."INITDIR",
        acc_safe_copy."IMPACTYPE",
        acc_safe_copy."IMPLOC",
        acc_safe_copy."EVENT1",
        acc_safe_copy."EVENT2",
        acc_safe_copy."EVENT3",
        acc_safe_copy."PER_NO",
        acc_safe_copy."INVTYPE",
        acc_safe_copy."INVAGE",
        acc_safe_copy."INJURY",
        acc_safe_copy."SAFEQUIP",
        acc_safe_copy."DRIVACT",
        acc_safe_copy."DRIVCOND",
        acc_safe_copy."PEDCOND",
        acc_safe_copy."PEDACT",
        acc_safe_copy."CHARGE",
        acc_safe_copy."CHARGE2",
        acc_safe_copy."CHARGE3",
        acc_safe_copy."CHARGE4",
        acc_safe_copy."VISIBLE",
        acc_safe_copy."LIGHT",
        acc_safe_copy."RDSFCOND",
        acc_safe_copy."VEHIMPTYPE",
        acc_safe_copy."MANOEUVER",
        acc_safe_copy."ENTRY",
        acc_safe_copy."GEOCODE",
        acc_safe_copy."FACTOR_ERR",
        acc_safe_copy."REP_TYPE",
        acc_safe_copy."BADGE_NO",
        acc_safe_copy."POSTAL",
        acc_safe_copy."XCOORD",
        acc_safe_copy."YCOORD",
        acc_safe_copy."PRECISE_XY",
        acc_safe_copy."LONGITUDE",
        acc_safe_copy."LATITUDE",
        acc_safe_copy."CHANGED",
        acc_safe_copy."SENT_UNIT",
        acc_safe_copy."SENT_DATE",
        acc_safe_copy."STATUS",
        acc_safe_copy."CITY_AREA",
        acc_safe_copy."USER_ID",
        acc_safe_copy."CRC_UNIT",
        acc_safe_copy."COMMENTS",
        acc_safe_copy."MTP_DIVISION",
        acc_safe_copy."POLICE_AGENCY",
        acc_safe_copy."SUBMIT_BADGE_NUMBER",
        acc_safe_copy."SUBMIT_DATE",
        acc_safe_copy."BIRTHDATE",
        acc_safe_copy."PRIVATE_PROPERTY",
        acc_safe_copy."PERSON_ID",
        acc_safe_copy."USERID",
        acc_safe_copy."TS",
        acc_safe_copy."ROAD_CLASS",
        acc_safe_copy."SYMBOL_NUM",
        acc_safe_copy."ROTATION_NUM",
        acc_safe_copy."PX",
        acc_safe_copy."DISTRICT",
        acc_safe_copy."QUADRANT",
        acc_safe_copy."FAILTOREM",
        acc_safe_copy."YEAR",
        acc_safe_copy."REC_ID",
        acc_safe_copy."PEDTYPE",
        acc_safe_copy."CYCLISTYPE",
        acc_safe_copy."SIDEWALKCYCLE",
        acc_safe_copy."CYCACT",
        acc_safe_copy."CYCCOND",
        acc_safe_copy."MVAIMG",
        acc_safe_copy."WARDNUM",
        acc_safe_copy."FATAL_NO",
        acc_safe_copy."DESCRIPTION",
        acc_safe_copy."TAB_REPORT",
        acc_safe_copy."ACTUAL_SPEED",
        acc_safe_copy."POSTED_SPEED",
        acc_safe_copy."TRAFCTLCOND"
    FROM collisions_replicator.acc_safe_copy
    WHERE
        acc_safe_copy."ACCDATE"::date >= '1985-01-01'::date
        AND acc_safe_copy."ACCDATE"::date <= 'now'::text::date
),

events_dat AS (
    SELECT
        a_1."ACCNB"::bigint AS accnb,
        a_1."ACCDATE"::date AS accdate,
        a_1."ACCTIME"::time without time zone AS acctime,
        a_1."STNAME1" AS stname1,
        a_1."STREETYPE1" AS streetype1,
        a_1."DIR1" AS dir1,
        a_1."STNAME2" AS stname2,
        a_1."STREETYPE2" AS streetype2,
        a_1."DIR2" AS dir2,
        a_1."STNAME3" AS stname3,
        a_1."STREETYPE3" AS streetype3,
        a_1."DIR3" AS dir3,
        a_1."ROAD_CLASS" AS road_class,
        date_part('year'::text, a_1."ACCDATE"::date) AS accyear,
        a_1."LONGITUDE" + 0.00021::double precision AS longitude,
        a_1."LATITUDE" + 0.000045::double precision AS latitude,
        upper(btrim(b_1.description)) AS location_type,
        upper(btrim(c_1.description)) AS location_class,
        upper(btrim(d.description)) AS collision_type,
        upper(btrim(k.description)) AS impact_type,
        upper(btrim(e.description)) AS visibility,
        upper(btrim(f.description)) AS light,
        upper(btrim(g.description)) AS road_surface_cond,
        CASE
            WHEN btrim(a_1."PX"::text) ~ '^[0-9]+$'::text THEN btrim(a_1."PX"::text)::integer
            ELSE NULL::integer
        END AS px,
        upper(btrim(h.description)) AS traffic_control,
        upper(btrim(i.description)) AS traffic_control_cond,
        CASE
            WHEN a_1."PRIVATE_PROPERTY"::text = 'Y'::text THEN TRUE
            WHEN a_1."PRIVATE_PROPERTY"::text = 'N'::text THEN FALSE
            ELSE NULL::boolean
        END AS on_private_property,
        CASE
            WHEN
                "left"(a_1."ACCNB"::text, 1) = '0'::text
                AND length(a_1."ACCNB"::text) = 10
                AND a_1."ACCDATE" >= '2020-01-01 00:00:00'::timestamp without time zone
                AND a_1."ACCDATE" <= '2020-12-31 00:00:00'::timestamp without time zone
                THEN 'TPS'::text
            WHEN a_1."ACCNB"::bigint >= 1000000000 THEN 'TPS'::text
            WHEN a_1."ACCNB"::bigint >= 100000000 THEN 'CRC'::text
            ELSE NULL::text
        END AS data_source
    FROM valid_rows AS a_1
    LEFT JOIN collision_factors.accloc AS b_1 ON a_1."ACCLOC"::text = b_1.accloc
    LEFT JOIN collision_factors.loccoord AS c_1 ON a_1."LOCCOORD"::text = c_1.loccoord
    LEFT JOIN collision_factors.acclass AS d ON a_1."ACCLASS"::text = d.acclass
    LEFT JOIN collision_factors.impactype AS k ON a_1."IMPACTYPE"::text = k.impactype
    LEFT JOIN collision_factors.visible AS e ON a_1."VISIBLE"::text = e.visible
    LEFT JOIN collision_factors.light AS f ON a_1."LIGHT"::text = f.light
    LEFT JOIN collision_factors.rdsfcond AS g ON a_1."RDSFCOND"::text = g.rdsfcond
    LEFT JOIN collision_factors.traffictl AS h ON a_1."TRAFFICTL"::text = h.traffictl
    LEFT JOIN collision_factors.trafctlcond AS i ON a_1."TRAFCTLCOND"::text = i.trafctlcond
),

events_desc AS (
    SELECT
        a_1."ACCNB"::bigint AS accnb,
        date_part('year'::text, a_1."ACCDATE"::date) AS accyear,
        max(a_1."DESCRIPTION"::text) AS description
    FROM valid_rows AS a_1
    GROUP BY (a_1."ACCNB"::bigint), (date_part('year'::text, a_1."ACCDATE"::date))
)

SELECT DISTINCT
    b.collision_no,
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
    a.impact_type,
    a.visibility,
    a.light,
    a.road_surface_cond,
    a.px,
    a.traffic_control,
    a.traffic_control_cond,
    a.on_private_property,
    c.description,
    a.data_source,
    st_setsrid(st_makepoint(a.longitude, a.latitude), 4326) AS geom
FROM events_dat AS a
JOIN collisions_replicator.collision_no AS b USING (accnb, accyear)
LEFT JOIN events_desc AS c USING (accnb, accyear)
ORDER BY b.collision_no
WITH DATA;

ALTER TABLE IF EXISTS collisions_replicator.events
OWNER TO collision_admins;

COMMENT ON MATERIALIZED VIEW collisions_replicator.events
IS 'Event-level variables in collisions_replicator.acc. Based on collisions_replicator.acc_safe_copy. Data updated daily at 3am.';

GRANT SELECT ON TABLE collisions_replicator.events TO kchan;
GRANT SELECT ON TABLE collisions_replicator.events TO bdit_humans;
GRANT SELECT ON TABLE collisions_replicator.events TO rsaunders;
GRANT SELECT ON TABLE collisions_replicator.events TO ksun;
GRANT ALL ON TABLE collisions_replicator.events TO collision_admins;
GRANT SELECT ON TABLE collisions_replicator.events TO data_collection;

CREATE INDEX collision_events_gidx
ON collisions_replicator.events USING gist
(geom)
TABLESPACE pg_default;

CREATE INDEX collision_events_idx
ON collisions_replicator.events USING btree
(collision_no)
TABLESPACE pg_default;

CREATE UNIQUE INDEX events_idx
ON collisions_replicator.events USING btree
(collision_no)
TABLESPACE pg_default;