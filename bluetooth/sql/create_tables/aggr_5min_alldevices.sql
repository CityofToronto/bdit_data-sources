DROP TABLE IF EXISTS bluetooth.aggr_5min_alldevices;
CREATE TABLE bluetooth.aggr_5min_alldevices (
    id bigserial NOT NULL PRIMARY KEY,
    analysis_id bigint,
    datetime_bin timestamp without time zone,
    tt numeric,
    obs integer
);
ALTER TABLE bluetooth.aggr_5min_alldevices
OWNER TO bt_admins;
GRANT SELECT, INSERT ON TABLE bluetooth.aggr_5min_alldevices TO bt_insert_bot;
GRANT SELECT ON TABLE bluetooth.aggr_5min_alldevices TO public;
