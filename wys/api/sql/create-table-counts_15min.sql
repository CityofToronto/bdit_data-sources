CREATE TABLE wys.counts_15min
(
  counts_15min serial NOT NULL,
  api_id integer,
  datetime_bin timestamp without time zone,
  speed integer,
  count integer,
  volumes_15min_uid integer,
  CONSTRAINT wys_counts_15min_pkey PRIMARY KEY (counts_15min),
  CONSTRAINT wys_volumes_15min_fkey FOREIGN KEY (volumes_15min_uid)
      REFERENCES wys.volumes_15min (volumes_15min_uid) MATCH SIMPLE
      ON UPDATE NO ACTION ON DELETE SET NULL
)
WITH (
  OIDS=FALSE
);