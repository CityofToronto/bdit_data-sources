-- Script for creating a table of individual-level variables from collisions.acc.

-- DROP MATERIALIZED VIEW collisions.involved;

CREATE MATERIALIZED VIEW collisions.involved
TABLESPACE pg_default
AS
	WITH involved_desc AS (
        SELECT a."ACCNB"::bigint AS accnb,
               date_part('year', a."ACCDATE"::date) AS accyear,
               CASE
                   WHEN btrim(a."VEH_NO"::text) ~ '^[0-9]+$'::text THEN btrim(a."VEH_NO"::text)::integer
                   ELSE NULL::integer
               END vehicle_no,
               CASE
               	   WHEN btrim(a."PER_NO"::text) ~ '^[0-9]+$'::text THEN btrim(a."PER_NO"::text)::integer
               	   ELSE NULL::integer
               END person_no,
               upper(btrim(i.description)) AS vehicle_class,
               upper(btrim(j.description)) AS initial_dir,
               upper(btrim(k.description)) AS impact_type,
               upper(btrim(l.description)) AS event1,
               upper(btrim(m.description)) AS event2,
               upper(btrim(n.description)) AS event3,
               upper(btrim(o.description)) AS involved_class,
               CASE
                   WHEN btrim(a."INVAGE"::text) ~ '^[0-9]+$'::text THEN btrim(a."INVAGE"::text)::integer
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
               a."POSTED_SPEED"::integer AS posted_speed,
               a."ACTUAL_SPEED"::integer AS actual_speed,
               CASE
                   WHEN a."FAILTOREM" = 1 THEN true
                   ELSE false
               END AS failed_to_remain,
               a."BIRTHDATE" AS birthdate,
               -- Validation is sometimes partial, so keep it at an individual level.
               CASE
                 WHEN a."USERID" IS NOT NULL THEN True
                 ELSE False
               END is_validated
        FROM (SELECT *
              FROM collisions.acc
              WHERE "ACCDATE"::date >= '1985-01-01'::date AND "ACCDATE"::date <= current_date) a
        LEFT JOIN (SELECT DISTINCT ON (vehtype.vehtype) vehtype.vehtype,
                          vehtype.description
                   FROM collision_factors.vehtype
                   ORDER BY vehtype.vehtype, (char_length(vehtype.description))) i ON a."VEHTYPE"::text = i.vehtype
        LEFT JOIN collision_factors.initdir j ON a."INITDIR"::text = j.initdir
        LEFT JOIN collision_factors.impactype k ON a."IMPACTYPE"::text = k.impactype
        LEFT JOIN collision_factors.event1 l ON a."EVENT1"::text = l.event1
        LEFT JOIN collision_factors.event2 m ON a."EVENT2"::text = m.event2
        LEFT JOIN collision_factors.event3 n ON a."EVENT3"::text = n.event3
        LEFT JOIN collision_factors.invtype o ON a."INVTYPE"::text = o.invtype
        LEFT JOIN collision_factors.injury p ON a."INJURY"::text = p.injury
        LEFT JOIN collision_factors.safequip q ON a."SAFEQUIP"::text = q.safequip
        LEFT JOIN collision_factors.drivact r ON a."DRIVACT"::text = r.drivact
        LEFT JOIN collision_factors.drivcond s ON a."DRIVCOND"::text = s.drivcond
        LEFT JOIN collision_factors.pedact t ON a."PEDACT"::text = t.pedact
        LEFT JOIN collision_factors.pedcond u ON a."PEDCOND"::text = u.pedcond
        LEFT JOIN collision_factors.pedtype v ON a."PEDTYPE"::text = v.pedtype
        LEFT JOIN collision_factors.cycact w ON a."CYCACT"::text = w.cycact
        LEFT JOIN collision_factors.cyccond x ON a."CYCCOND"::text = x.cyccond
        LEFT JOIN collision_factors.cyclistype y ON a."CYCLISTYPE"::text = y.cyclistype
        LEFT JOIN collision_factors.manoeuver z ON a."MANOEUVER"::text = z.manoeuver
    )
	SELECT b.collision_no,
	       a.vehicle_no,
	       a.person_no,
	       a.vehicle_class,
	       a.initial_dir,
	       a.impact_type,
	       a.event1,
	       a.event2,
	       a.event3,
	       a.involved_class,
	       CASE
	           WHEN a.involved_age > 0 THEN a.involved_age
	           WHEN a.involved_age = 0 AND a.birthdate IS NOT NULL THEN a.involved_age
	           ELSE NULL::integer
	       END AS involved_age,
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
           a.is_validated
	FROM involved_desc a
	LEFT JOIN collisions.collision_no b USING (accnb, accyear)
    -- This should only matter if collision.events is refreshed more than a day after collision.collision_no.
    WHERE b.collision_no IS NOT NULL
	ORDER BY b.collision_no
WITH DATA;

ALTER TABLE collisions.involved
    OWNER TO czhu;

COMMENT ON MATERIALIZED VIEW collisions.involved
    IS 'Individual-level variables in collisions.acc.';

GRANT ALL ON TABLE collisions.involved TO czhu;
GRANT SELECT ON TABLE collisions.involved TO bdit_humans;

CREATE INDEX collision_involved_idx
    ON collisions.involved USING btree
    (collision_no)
    TABLESPACE pg_default;
