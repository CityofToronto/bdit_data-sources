-- Table: open_data.ksi_vz

-- DROP TABLE IF EXISTS open_data.ksi_vz;

CREATE TABLE IF NOT EXISTS open_data.ksi_vz
(
    collision_id text,
    accdate timestamp without time zone,
    stname1 text,
    stname2 text,
    stname3 text,
    per_inv integer,
    acclass text,
    accloc text,
    traffictl text,
    impactype text,
    visible text,
    light text,
    rdsfcond text,
    road_class text,
    failtorem text,
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
    manoeuvre text,
    pedtype text,
    cyclistype text,
    cycact text,
    cyccond text,
    road_user text,
    fatal_no smallint,
    wardname text,
    division character varying,
    neighbourhood text,
    aggressive text,
    distracted text,
    cyclist text,
    motorcyclist text,
    other_micromobility text,
    older_adult text,
    pedestrian text,
    red_light text,
    school_child text,
    heavy_truck text
);

ALTER TABLE IF EXISTS open_data.ksi_vz
    OWNER to od_admins;

GRANT SELECT ON TABLE open_data.ksi_vz TO bdit_humans;

GRANT ALL ON TABLE open_data.ksi_vz TO collisions_bot;

GRANT ALL ON TABLE open_data.ksi_vz TO od_admins;

GRANT SELECT ON TABLE open_data.ksi_vz TO od_extract_svc;

COMMENT ON TABLE open_data.ksi_vz
    IS 'Table for KSI VZ Dashboard, hosted on GCC.';