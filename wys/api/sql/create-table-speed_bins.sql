CREATE TABLE wys.speed_bins
(
  speed_id integer NOT NULL,
  speed_bin int4range,
  CONSTRAINT speed_bins_speed_id_pkey PRIMARY KEY (speed_id)
)
WITH (
  OIDS=FALSE
);
ALTER TABLE wys.speed_bins
  OWNER TO rliu