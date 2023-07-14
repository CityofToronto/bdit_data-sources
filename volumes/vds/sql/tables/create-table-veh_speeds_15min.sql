--DROP TABLE vds.veh_speeds_15min;

CREATE TABLE vds.veh_speeds_15min (
    division_id smallint, 
    vds_id integer,
    datetime_15min timestamp,
    speed_5kph smallint,
    count smallint,
    total_count smallint,
    PRIMARY KEY (division_id, vds_id, datetime_15min, speed_5kph)
); 

ALTER TABLE vds.veh_speeds_15min OWNER TO vds_admins;
GRANT INSERT, DELETE, SELECT ON TABLE vds.veh_speeds_15min TO vds_bot;