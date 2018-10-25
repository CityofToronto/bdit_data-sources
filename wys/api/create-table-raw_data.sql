CREATE TABLE wys.raw_data
(
  raw_data_uid serial NOT NULL,
  api_id integer,
  datetime_bin timestamp without time zone,
  speed integer,
  count integer,
  counts_15min integer,
  CONSTRAINT wys_raw_data_uid_pkey PRIMARY KEY (raw_data_uid),
  CONSTRAINT wys_counts_15min_fkey FOREIGN KEY (counts_15min)
      REFERENCES wys.counts_15min (counts_15min) MATCH SIMPLE
      ON UPDATE NO ACTION ON DELETE SET NULL
)
WITH (
  OIDS=FALSE
);