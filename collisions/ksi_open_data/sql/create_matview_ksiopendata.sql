CREATE MATERIALIZED VIEW open_data_staging.ksi AS 
SELECT
    ROW_NUMBER() OVER (ORDER BY accdate, per_no) as uid,
    events.collision_id,
    events.accdate,
    events.stname1,
    events.streetype1,
    events.dir1,
    events.stname2,
    events.streetype2,
    events.dir2,
    events.stname3,
    events.streetype3,
    events.dir3,
    events.per_inv,
    acclass.description AS acclass,
    accloc.description AS accloc,
    traffictl.description AS traffictl,
    impactype.description AS impactype,
    visible.description AS visible,
    light.description AS light,
    rdsfcond.description AS rdsfcond,
    changed,
    road_class,
    events.failtorem,
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
    events.aggressive,
    events.distracted,
    events.city_damage,
    events.cyclist,
    events.motorcyclist,
    events.other_micromobility,
    events.older_adult,
    events.pedestrian,
    events.red_light,
    events.school_child,
    events.heavy_truck
FROM collisions.events
INNER JOIN collisions.involved USING (collision_id)
LEFT JOIN collision_factors.acclass ON events.acclass = acclass.acclass::int
LEFT JOIN collision_factors.accloc ON events.accloc = accloc.accloc::int
LEFT JOIN collision_factors.traffictl ON events.traffictl = traffictl.traffictl::int
LEFT JOIN collision_factors.visible ON events.visible = visible.visible::int
LEFT JOIN collision_factors.light ON events.light = light.light::int
LEFT JOIN collision_factors.rdsfcond ON events.rdsfcond = rdsfcond.rdsfcond::int
LEFT JOIN collision_factors.vehtype ON involved.vehtype = vehtype.vehtype::int 
LEFT JOIN collision_factors.initdir ON involved.initdir = initdir.initdir::int 
LEFT JOIN collision_factors.injury ON involved.injury = injury.injury::int 
LEFT JOIN collision_factors.safequip ON involved.safequip = safequip.safequip::int 
LEFT JOIN collision_factors.drivact ON involved.drivact = drivact.drivact::int 
LEFT JOIN collision_factors.drivcond ON involved.drivcond = drivcond.drivcond::int 
LEFT JOIN collision_factors.pedact ON involved.pedact = pedact.pedact::int 
LEFT JOIN collision_factors.pedcond ON involved.pedcond = pedcond.pedcond::int 
LEFT JOIN collision_factors.manoeuver ON involved.manoeuver = manoeuver.manoeuver::int 
LEFT JOIN collision_factors.pedtype ON involved.pedtype = pedtype.pedtype::int 
LEFT JOIN collision_factors.cyclistype ON involved.cyclistype = cyclistype.cyclistype::int 
LEFT JOIN collision_factors.cycact ON involved.cycact = cycact.cycact::int 
LEFT JOIN collision_factors.cyccond ON involved.cyccond = cyccond.cyccond::int
LEFT JOIN LATERAL (
		SELECT case when accdate - date_valid > INTERVAL '0 days' then 'Y' else null end as orders, description, impactype
		FROM collision_factors.impactype 
		WHERE events.impactype = impactype.impactype::int 
		ORDER BY orders, date_valid nulls first
		LIMIT 1) AS impactype ON TRUE
LEFT JOIN LATERAL (
		SELECT
			pb.unit_name AS division
		FROM gis.police_boundary AS pb
		WHERE
			ST_Intersects(pb.geom, ST_Setsrid(ST_makepoint(events.longitude, events.latitude), 4326))
			AND pb.geom && ST_Expand(ST_Setsrid(ST_makepoint(events.longitude, events.latitude), 4326), 0.005)
		ORDER BY
			pb.geom <-> ST_Setsrid(ST_makepoint(events.longitude, events.latitude), 4326)
		LIMIT 1
	) AS police_u ON TRUE
LEFT JOIN LATERAL (
		SELECT
			ward.area_name  AS wardname
		FROM gis_core.city_ward AS ward
		WHERE
			ST_Intersects(ward.geom, ST_Setsrid(ST_makepoint(events.longitude, events.latitude), 4326))
			AND ward.geom && ST_Expand(ST_Setsrid(ST_makepoint(events.longitude, events.latitude), 4326), 0.005)
		ORDER BY
			ward.geom <-> ST_Setsrid(ST_makepoint(events.longitude, events.latitude), 4326)
		LIMIT 1
	) AS ward ON TRUE
LEFT JOIN LATERAL (
		SELECT
			neighbourhood_table.area_name  AS neighbourhood
		FROM gis.neighbourhood AS neighbourhood_table
		WHERE
			ST_Intersects(neighbourhood_table.geom, ST_Setsrid(ST_makepoint(events.longitude, events.latitude), 4326))
			AND neighbourhood_table.geom && ST_Expand(ST_Setsrid(ST_makepoint(events.longitude, events.latitude), 4326), 0.005)
		ORDER BY
			neighbourhood_table.geom <-> ST_Setsrid(ST_makepoint(events.longitude, events.latitude), 4326)
		LIMIT 1
	) AS neighbourhood_table ON TRUE
WHERE 
	events.accdate >= '2000-01-01' AND -- only 2000 and above 
	events.ksi IS true -- only killed or seriously injuried
ORDER BY accdate;

CREATE UNIQUE INDEX ksi_uid_idx
    ON open_data_staging.ksi USING btree
    (uid);

ALTER MATERIALIZED VIEW open_data_staging.ksi
    OWNER to od_admins;

REVOKE ALL ON  open_data_staging.ksi FROM bdit_humans;

COMMENT ON MATERIALIZED VIEW open_data_staging.ksi
    IS 'Staging materialized view for open_data.ksi, refreshes daily through airflow DAG ksi_opendata on ec2.';