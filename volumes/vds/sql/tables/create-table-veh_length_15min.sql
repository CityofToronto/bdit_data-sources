--DROP TABLE vds.veh_length_15min;

CREATE TABLE vds.veh_length_15min (
    division_id smallint, 
    vds_id integer,
    datetime_15min timestamp,
    length_meter smallint,
    count smallint,
    total_count smallint,
    PRIMARY KEY (division_id, vds_id, datetime_15min, length_meter)
); 

ALTER TABLE vds.veh_length_15min OWNER TO vds_admins;
GRANT INSERT, DELETE, SELECT ON TABLE vds.veh_length_15min TO vds_bot;