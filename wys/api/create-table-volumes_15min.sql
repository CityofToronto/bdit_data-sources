CREATE TABLE wys.volumes_15min
(
  volumes_15min_uid serial NOT NULL,
  api_id integer,
  datetime_bin timestamp without time zone,
  count integer,
  CONSTRAINT wys_volumes_15min_pkey PRIMARY KEY (volumes_15min_uid),
  CONSTRAINT wys_volumes_15min_api_id_datetime_bin UNIQUE (api_id, datetime_bin)
)
WITH (
  OIDS=FALSE
);