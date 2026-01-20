CREATE TABLE IF NOT EXISTS open_data.ksi
(
    uid int,
    collision_id text NOT NULL,
    accdate timestamp without time zone NOT NULL,
    stname1 text,
    streetype1 text,
    dir1 text,
    stname2 text,
    streetype2 text,
    dir2 text,
    stname3 text,
    streetype3 text,
    dir3 text,
    per_inv integer,
    acclass text,
    accloc text,
    traffictl text,
    impactype text,
    visible text,
    light text,
    rdsfcond text,
    changed smallint,
    road_class text,
    failtorem boolean,
    longitude double precision,
    latitude double precision,
    veh_no smallint,
    vehtype text,
    initdir text,
    per_no smallint,
    invage smallint,
    injury text,
    safequip text,
    drivact text,
    drivcond text,
    pedact text,
    pedcond text,
    manoeuver text,
    pedtype text,
    cyclistype text,
    cycact text,
    cyccond text,
    road_user text,
    fatal_no smallint,
    wardname text,
    division character varying,
    neighbourhood text,
    aggressive boolean,
    distracted boolean,
    city_damage boolean,
    cyclist boolean,
    motorcyclist boolean,
    other_micromobility boolean,
    older_adult boolean,
    pedestrian boolean,
    red_light boolean,
    school_child boolean,
    heavy_truck boolean,
    CONSTRAINT ksi_pkey PRIMARY KEY (uid)
);

ALTER TABLE IF EXISTS open_data.ksi
    OWNER to od_admins;

GRANT SELECT ON TABLE open_data.ksi TO od_extract_svc;

REVOKE ALL ON TABLE open_data.ksi FROM bdit_humans;

COMMENT ON TABLE open_data.ksi
    IS 'Table for KSI open data, link: ';