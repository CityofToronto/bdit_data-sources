-- Table: wys.counts_15min

-- DROP TABLE wys.counts_15min;

CREATE TABLE wys.counts_15min
(
  counts_15min serial NOT NULL,
  api_id integer,
  datetime_bin timestamp without time zone,
  speed_id integer,
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
ALTER TABLE wys.counts_15min
  OWNER TO rliu;
GRANT ALL ON TABLE wys.counts_15min TO rds_superuser WITH GRANT OPTION;
GRANT ALL ON TABLE wys.counts_15min TO dbadmin;
GRANT SELECT, REFERENCES, TRIGGER ON TABLE wys.counts_15min TO bdit_humans WITH GRANT OPTION;
GRANT ALL ON TABLE wys.counts_15min TO rliu;

-- Trigger: counts_15min_delete on wys.counts_15min

-- DROP TRIGGER counts_15min_delete ON wys.counts_15min;

CREATE TRIGGER counts_15min_delete
  AFTER DELETE
  ON wys.counts_15min
  FOR EACH ROW
  EXECUTE PROCEDURE wys.trgr_counts_15min_delete();

