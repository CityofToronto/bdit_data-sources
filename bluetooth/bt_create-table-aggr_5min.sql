DROP TABLE IF EXISTS bluetooth.aggr_5min;
CREATE TABLE bluetooth.aggr_5min(
	id bigserial not null primary key,
	analysis_id bigint,
	datetime_bin timestamp without time zone,
	tt numeric,
	obs integer
);
ALTER TABLE bluetooth.aggr_5min
OWNER TO bt_admins;
GRANT SELECT, INSERT ON TABLE bluetooth.aggr_5min TO bt_insert_bot;
  