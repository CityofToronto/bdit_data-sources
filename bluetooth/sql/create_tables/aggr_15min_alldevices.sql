DROP TABLE IF EXISTS bluetooth.aggr_15min_alldevices;
CREATE TABLE bluetooth.aggr_15min_alldevices (
    id bigserial NOT NULL PRIMARY KEY,
    analysis_id bigint,
    datetime_bin timestamp without time zone,
    tt numeric,
    obs integer
);