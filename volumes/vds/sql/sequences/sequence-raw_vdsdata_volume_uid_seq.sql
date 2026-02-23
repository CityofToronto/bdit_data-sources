-- SEQUENCE: vds.raw_vdsdata_volume_uid_seq

-- DROP SEQUENCE IF EXISTS vds.raw_vdsdata_volume_uid_seq;

CREATE SEQUENCE IF NOT EXISTS vds.raw_vdsdata_volume_uid_seq
INCREMENT 1
START 1
MINVALUE 1
MAXVALUE 9223372036854775807
CACHE 1
OWNED BY raw_vdsdata_div2_2021.volume_uid;

ALTER SEQUENCE vds.raw_vdsdata_volume_uid_seq OWNER TO vds_admins;

GRANT ALL ON SEQUENCE vds.raw_vdsdata_volume_uid_seq TO vds_admins;

GRANT ALL ON SEQUENCE vds.raw_vdsdata_volume_uid_seq TO vds_bot;