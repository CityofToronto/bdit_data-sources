DROP TABLE IF EXISTS bluetooth.aggr_15min;
CREATE TABLE bluetooth.aggr_15min(
	id bigserial not null primary key,
	analysis_id bigint,
	datetime_bin timestamp without time zone,
	tt numeric,
	obs integer
);