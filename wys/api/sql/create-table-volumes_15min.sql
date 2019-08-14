-- Table: wys.volumes_15min

-- DROP TABLE wys.volumes_15min;

CREATE TABLE wys.volumes_15min
(
  volumes_15min_uid serial NOT NULL,
  api_id integer,
  datetime_bin timestamp without time zone,
  count integer,
  CONSTRAINT wys_volumes_15min_pkey PRIMARY KEY (volumes_15min_uid)
)
WITH (
  OIDS=FALSE
);
ALTER TABLE wys.volumes_15min
  OWNER TO rliu;
GRANT ALL ON TABLE wys.volumes_15min TO rds_superuser WITH GRANT OPTION;
GRANT ALL ON TABLE wys.volumes_15min TO dbadmin;
GRANT SELECT, REFERENCES, TRIGGER ON TABLE wys.volumes_15min TO bdit_humans WITH GRANT OPTION;
GRANT ALL ON TABLE wys.volumes_15min TO rliu;
