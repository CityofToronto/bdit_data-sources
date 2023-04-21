-- Script for creating a table of individual-level variables from collisions.acc.

-- View: collisions_replicator.involved

-- DROP MATERIALIZED VIEW IF EXISTS collisions_replicator.involved;

CREATE MATERIALIZED VIEW IF NOT EXISTS collisions_replicator.involved
TABLESPACE pg_default
AS
WITH i AS (SELECT DISTINCT ON (vehtype.vehtype)
    vehtype.vehtype,
    vehtype.description
FROM collision_factors.vehtype
ORDER BY vehtype.vehtype, (char_length(vehtype.description))),
involved_desc AS (
    SELECT
        a_1."ACCNB"::bigint AS accnb,
        a_1."REC_ID"::bigint AS rec_id,
        a_1."POSTED_SPEED"::integer AS posted_speed,
        a_1."ACTUAL_SPEED"::integer AS actual_speed,
        a_1."BIRTHDATE" AS birthdate,
        a_1."USERID" AS validation_userid,
        a_1."TS" AS time_last_edited,
        date_part('year'::text, a_1."ACCDATE"::date) AS accyear,
        CASE
            WHEN
                btrim(a_1."PER_NO"::text) ~ '^[0-9]+$'::text
                THEN btrim(a_1."PER_NO"::text)::integer
            ELSE NULL::integer
        END AS person_no,
        CASE
            WHEN
                btrim(a_1."VEH_NO"::text) ~ '^[0-9]+$'::text
                THEN btrim(a_1."VEH_NO"::text)::integer
            ELSE NULL::integer
        END AS vehicle_no,
        upper(btrim(i.description)) AS vehicle_class,
        upper(btrim(j.description)) AS initial_dir,
        upper(btrim(k.description)) AS impact_location,
        upper(btrim(l.description)) AS event1,
        upper(btrim(m.description)) AS event2,
        upper(btrim(n.description)) AS event3,
        upper(btrim(o.description)) AS involved_class,
        CASE
            WHEN
                btrim(a_1."INVAGE"::text) ~ '^[0-9]+$'::text
                THEN btrim(a_1."INVAGE"::text)::integer
            ELSE NULL::integer
        END AS involved_age,
        upper(btrim(p.description)) AS involved_injury_class,
        upper(btrim(q.description)) AS safety_equip_used,
        upper(btrim(r.description)) AS driver_action,
        upper(btrim(s.description)) AS driver_condition,
        upper(btrim(t.description)) AS pedestrian_action,
        upper(btrim(u.description)) AS pedestrian_condition,
        upper(btrim(v.description)) AS pedestrian_collision_type,
        upper(btrim(w.description)) AS cyclist_action,
        upper(btrim(x.description)) AS cyclist_condition,
        upper(btrim(y.description)) AS cyclist_collision_type,
        upper(btrim(z.description)) AS manoeuver,
        COALESCE (a_1."FAILTOREM" = 1, FALSE) AS failed_to_remain
    FROM (
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
    ) AS a_1
    LEFT JOIN i
        ON a_1."VEHTYPE"::text = i.vehtype
    LEFT JOIN collision_factors.initdir AS j ON a_1."INITDIR"::text = j.initdir
    LEFT JOIN collision_factors.imploc AS k ON a_1."IMPLOC"::text = k.imploc
    LEFT JOIN collision_factors.event1 AS l ON a_1."EVENT1"::text = l.event1
    LEFT JOIN collision_factors.event2 AS m ON a_1."EVENT2"::text = m.event2
    LEFT JOIN collision_factors.event3 AS n ON a_1."EVENT3"::text = n.event3
    LEFT JOIN collision_factors.invtype AS o ON a_1."INVTYPE"::text = o.invtype
    LEFT JOIN collision_factors.injury AS p ON a_1."INJURY"::text = p.injury
    LEFT JOIN collision_factors.safequip AS q ON a_1."SAFEQUIP"::text = q.safequip
    LEFT JOIN collision_factors.drivact AS r ON a_1."DRIVACT"::text = r.drivact
    LEFT JOIN collision_factors.drivcond AS s ON a_1."DRIVCOND"::text = s.drivcond
    LEFT JOIN collision_factors.pedact AS t ON a_1."PEDACT"::text = t.pedact
    LEFT JOIN collision_factors.pedcond AS u ON a_1."PEDCOND"::text = u.pedcond
    LEFT JOIN collision_factors.pedtype AS v ON a_1."PEDTYPE"::text = v.pedtype
    LEFT JOIN collision_factors.cycact AS w ON a_1."CYCACT"::text = w.cycact
    LEFT JOIN collision_factors.cyccond AS x ON a_1."CYCCOND"::text = x.cyccond
    LEFT JOIN collision_factors.cyclistype AS y ON a_1."CYCLISTYPE"::text = y.cyclistype
    LEFT JOIN collision_factors.manoeuver AS z ON a_1."MANOEUVER"::text = z.manoeuver
)

SELECT
    b.collision_no,
    a.rec_id,
    a.person_no,
    a.vehicle_no,
    a.vehicle_class,
    a.initial_dir,
    a.impact_location,
    a.event1,
    a.event2,
    a.event3,
    a.involved_class,
    a.involved_injury_class,
    a.safety_equip_used,
    a.driver_action,
    a.driver_condition,
    a.pedestrian_action,
    a.pedestrian_condition,
    a.pedestrian_collision_type,
    a.cyclist_action,
    a.cyclist_condition,
    a.cyclist_collision_type,
    a.manoeuver,
    a.posted_speed,
    a.actual_speed,
    a.failed_to_remain,
    a.validation_userid,
    a.time_last_edited,
    CASE
        WHEN a.involved_age > 0 THEN a.involved_age
        WHEN a.involved_age = 0 AND a.birthdate IS NOT NULL THEN a.involved_age
        ELSE NULL::integer
    END AS involved_age
FROM involved_desc AS a
JOIN collisions_replicator.collision_no AS b USING (accnb, accyear)
ORDER BY b.collision_no
WITH DATA;

ALTER TABLE IF EXISTS collisions_replicator.involved
OWNER TO collision_admins;

COMMENT ON MATERIALIZED VIEW collisions_replicator.involved
IS 'Individual-level variables in collisions_replcator.acc_safe_copy. Data refreshed daily at 3am.';

GRANT SELECT ON TABLE collisions_replicator.involved TO kchan;
GRANT SELECT ON TABLE collisions_replicator.involved TO bdit_humans;
GRANT SELECT ON TABLE collisions_replicator.involved TO rsaunders;
GRANT SELECT ON TABLE collisions_replicator.involved TO ksun;
GRANT ALL ON TABLE collisions_replicator.involved TO collision_admins;
GRANT SELECT ON TABLE collisions_replicator.involved TO data_collection;

CREATE INDEX collision_involved_idx
ON collisions_replicator.involved USING btree
(collision_no)
TABLESPACE pg_default;

CREATE UNIQUE INDEX inv_rec_id_idx
ON collisions_replicator.involved USING btree
(rec_id)
TABLESPACE pg_default;