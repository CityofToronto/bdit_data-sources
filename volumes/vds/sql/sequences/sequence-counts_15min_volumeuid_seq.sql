-- SEQUENCE: vds.counts_15min_volumeuid_seq

-- DROP SEQUENCE IF EXISTS vds.counts_15min_volumeuid_seq;

CREATE SEQUENCE IF NOT EXISTS vds.counts_15min_volumeuid_seq
INCREMENT 1
START 1
MINVALUE 1
MAXVALUE 9223372036854775807
CACHE 1
OWNED BY counts_15min.volumeuid;

ALTER SEQUENCE vds.counts_15min_volumeuid_seq OWNER TO vds_admins;

GRANT ALL ON SEQUENCE vds.counts_15min_volumeuid_seq TO vds_admins;

GRANT ALL ON SEQUENCE vds.counts_15min_volumeuid_seq TO vds_bot;