-- SEQUENCE: vds.veh_speeds_15min_uid_seq

-- DROP SEQUENCE IF EXISTS vds.veh_speeds_15min_uid_seq;

CREATE SEQUENCE IF NOT EXISTS vds.veh_speeds_15min_uid_seq
INCREMENT 1
START 1
MINVALUE 1
MAXVALUE 9223372036854775807
CACHE 1
OWNED BY veh_speeds_15min.uid;

ALTER SEQUENCE vds.veh_speeds_15min_uid_seq OWNER TO vds_admins;

GRANT ALL ON SEQUENCE vds.veh_speeds_15min_uid_seq TO vds_admins;

GRANT ALL ON SEQUENCE vds.veh_speeds_15min_uid_seq TO vds_bot;