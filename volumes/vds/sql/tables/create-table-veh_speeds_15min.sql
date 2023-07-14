--DROP TABLE vds.veh_speeds_15min;

CREATE TABLE vds.veh_speeds_15min (
    uid bigserial,
    division_id smallint, 
    vds_id integer,
    datetime_15min timestamp,
    speed_5kph smallint,
    count smallint,
    total_count smallint,
    PRIMARY KEY uid,
    UNIQUE (division_id, vds_id, datetime_15min, speed_5kph)
); 

ALTER TABLE vds.veh_speeds_15min OWNER TO vds_admins;
GRANT INSERT, DELETE, SELECT ON TABLE vds.veh_speeds_15min TO vds_bot;
GRANT ALL ON SEQUENCE vds.veh_speeds_15min_uid_seq TO vds_bot;

COMMENT ON TABLE vds.veh_speeds_15min IS 'A count of vehicle speeds from `raw_vdsvehicledata` aggregated to detector / speeds floored to 5kph.';