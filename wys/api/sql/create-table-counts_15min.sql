-- Table: wys.counts_15min

-- DROP TABLE wys.counts_15min;

CREATE TABLE wys.counts_15min
(
  counts_15min bigserial NOT NULL,
  api_id integer,
  datetime_bin timestamp without time zone,
  speed_id integer,
  count integer,
  volumes_15min_uid integer,
  CONSTRAINT wys_counts_15min_pkey PRIMARY KEY (counts_15min),
  UNIQUE(api_id, datetime_bin, speed_id)
)
WITH (
  OIDS=FALSE
);
ALTER TABLE wys.counts_15min
  OWNER TO rdumas;
GRANT ALL ON TABLE wys.counts_15min TO rds_superuser WITH GRANT OPTION;
GRANT ALL ON TABLE wys.counts_15min TO dbadmin;
GRANT SELECT, REFERENCES, TRIGGER ON TABLE wys.counts_15min TO bdit_humans WITH GRANT OPTION;


