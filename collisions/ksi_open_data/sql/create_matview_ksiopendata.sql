CREATE MATERIALIZED VIEW open_data_staging.ksi AS
SELECT
    events.collision_id,
    events.accdate,
    events.stname1,
    events.stname2,
    events.stname3,
    events.per_inv,
    acclass.description AS acclass,
    accloc.description AS accloc,
    traffictl.description AS traffictl,
    impactype.description AS impactype,
    visible.description AS visible,
    light.description AS light,
    rdsfcond.description AS rdsfcond,
    events.changed,
    events.road_class,
    events.failtorem::text,
    events.longitude,
    events.latitude,
    involved.veh_no,
    vehtype.description AS vehtype,
    initdir.description AS initdir,
    involved.per_no,
    involved.invage,
    injury.description AS injury,
    safequip.description AS safequip,
    drivact.description AS drivact,
    drivcond.description AS drivcond,
    pedact.description AS pedact,
    pedcond.description AS pedcond,
    manoeuver.description AS manoeuver,
    pedtype.description AS pedtype,
    cyclistype.description AS cyclistype,
    cycact.description AS cycact,
    cyccond.description AS cyccond,
    involved.road_user,
    involved.fatal_no,
    ward.wardname,
    police_u.division,
    neighbourhood_table.neighbourhood,
    events.aggressive::text,
    events.distracted::text,
    events.cyclist::text,
    events.motorcyclist::text,
    events.other_micromobility::text,
    events.older_adult::text,
    events.pedestrian::text,
    events.red_light::text,
    events.school_child::text,
    events.heavy_truck::text
FROM (
    SELECT
        events.collision_id,
        events.accdate,
        concat_ws(' ', events.stname1, events.streetype1, events.dir1) AS stname1,
        concat_ws(' ', events.stname2, events.streetype2, events.dir2) AS stname2,
        concat_ws(' ', events.stname3, events.streetype3, events.dir3) AS stname3,
        events.per_inv,
        events.acclass,
        events.accloc,
        events.traffictl,
        events.impactype,
        events.visible,
        events.light,
        events.rdsfcond,
        events.changed,
        events.road_class,
        events.failtorem,
        events.longitude,
        events.latitude,
        events.aggressive,
        events.distracted,
        events.cyclist,
        events.motorcyclist,
        events.other_micromobility,
        events.older_adult,
        events.pedestrian,
        events.red_light,
        events.school_child,
        events.heavy_truck,
        ST_Setsrid(ST_makepoint(events.longitude, events.latitude), 4326) AS events_geom
    FROM collisions.events
    WHERE
        events.accdate >= '2006-01-01' -- only 2006 and above 
        AND events.ksi IS TRUE
) AS events -- only killed or seriously injuried
INNER JOIN collisions.involved USING (collision_id)
LEFT JOIN collision_factors.acclass ON events.acclass = acclass.acclass::int
LEFT JOIN
    collision_factors.accloc
    ON events.accloc = accloc.accloc::int
    AND (events.accdate > accloc.date_valid OR accloc.date_valid IS NULL)
LEFT JOIN
    collision_factors.traffictl
    ON events.traffictl = traffictl.traffictl::int
    AND (events.accdate > traffictl.date_valid OR traffictl.date_valid IS NULL)
LEFT JOIN
    collision_factors.visible
    ON events.visible = visible.visible::int
    AND (events.accdate > visible.date_valid OR visible.date_valid IS NULL)
LEFT JOIN collision_factors.light ON events.light = light.light::int
LEFT JOIN
    collision_factors.rdsfcond
    ON events.rdsfcond = rdsfcond.rdsfcond::int
    AND (events.accdate > rdsfcond.date_valid OR rdsfcond.date_valid IS NULL)
LEFT JOIN
    collision_factors.vehtype
    ON involved.vehtype = vehtype.vehtype::int
    AND (events.accdate > vehtype.date_valid OR vehtype.date_valid IS NULL)
LEFT JOIN collision_factors.initdir ON involved.initdir = initdir.initdir::int
LEFT JOIN collision_factors.injury ON involved.injury = injury.injury::int
LEFT JOIN
    collision_factors.safequip
    ON involved.safequip = safequip.safequip::int
    AND (events.accdate > safequip.date_valid OR safequip.date_valid IS NULL)
LEFT JOIN collision_factors.drivact ON involved.drivact = drivact.drivact::int
LEFT JOIN
    collision_factors.drivcond
    ON involved.drivcond = drivcond.drivcond::int
    AND (events.accdate > drivcond.date_valid OR drivcond.date_valid IS NULL)
LEFT JOIN collision_factors.pedact ON involved.pedact = pedact.pedact::int
LEFT JOIN
    collision_factors.pedcond
    ON involved.pedcond = pedcond.pedcond::int
    AND (events.accdate > pedcond.date_valid OR pedcond.date_valid IS NULL)
LEFT JOIN
    collision_factors.manoeuver
    ON involved.manoeuver = manoeuver.manoeuver::int
    AND (events.accdate > manoeuver.date_valid OR manoeuver.date_valid IS NULL)
LEFT JOIN collision_factors.pedtype ON involved.pedtype = pedtype.pedtype::int
LEFT JOIN collision_factors.cyclistype ON involved.cyclistype = cyclistype.cyclistype::int
LEFT JOIN collision_factors.cycact ON involved.cycact = cycact.cycact::int
LEFT JOIN
    collision_factors.cyccond
    ON involved.cyccond = cyccond.cyccond::int
    AND (events.accdate > cyccond.date_valid OR cyccond.date_valid IS NULL)
LEFT JOIN LATERAL (
    SELECT
        CASE
            WHEN events.accdate - impactype.date_valid > interval '0 days' THEN 'Y'
        END AS orders,
        impactype.description,
        impactype.impactype
    FROM collision_factors.impactype
    WHERE events.impactype = impactype.impactype::int
    ORDER BY orders, impactype.date_valid NULLS FIRST
    LIMIT 1
) AS impactype ON TRUE
LEFT JOIN LATERAL (
    SELECT pb.unit_name AS division
    FROM gis.police_boundary AS pb
    WHERE
        ST_Intersects(pb.geom, events.events_geom)
        AND pb.geom && ST_Expand(events.events_geom, 0.005)
    ORDER BY
        pb.geom <-> events.events_geom
    LIMIT 1
) AS police_u ON TRUE
LEFT JOIN LATERAL (
    SELECT ward.area_desc AS wardname
    FROM gis_core.city_ward AS ward
    WHERE
        ST_Intersects(ward.geom, events.events_geom)
        AND ward.geom && ST_Expand(events.events_geom, 0.005)
    ORDER BY
        ward.geom <-> events.events_geom
    LIMIT 1
) AS ward ON TRUE
LEFT JOIN LATERAL (
    SELECT neighbourhood_table.area_name AS neighbourhood
    FROM gis.neighbourhood AS neighbourhood_table
    WHERE
        ST_Intersects(neighbourhood_table.geom, events.events_geom)
        AND neighbourhood_table.geom && ST_Expand(events.events_geom, 0.005)
    ORDER BY
        neighbourhood_table.geom <-> events.events_geom
    LIMIT 1
) AS neighbourhood_table ON TRUE
ORDER BY events.accdate;

CREATE UNIQUE INDEX collisions_id_per_no_idx
ON open_data_staging.ksi USING btree
(collision_id, per_no);

ALTER MATERIALIZED VIEW open_data_staging.ksi
OWNER TO collisions_bot;

GRANT SELECT ON open_data_staging.ksi TO collision_admins;

REVOKE ALL ON open_data_staging.ksi FROM bdit_humans;

COMMENT ON MATERIALIZED VIEW open_data_staging.ksi
IS 'Staging materialized view for open_data.ksi, refreshes daily through airflow DAG ksi_opendata on ec2.';