-- Table: wys.speed_counts

-- DROP TABLE wys.speed_counts;

CREATE TABLE wys.speed_counts
(
  speed_count_uid serial NOT NULL,
  api_id integer,
  datetime_bin timestamp without time zone,
  speed_id integer,
  count integer,
  counts_15min integer,
  CONSTRAINT wys_speed_count_uid_pkey PRIMARY KEY (speed_count_uid),
  CONSTRAINT wys_counts_15min_fkey FOREIGN KEY (counts_15min)
      REFERENCES wys.counts_15min (counts_15min) MATCH SIMPLE
      ON UPDATE NO ACTION ON DELETE SET NULL
)
WITH (
  OIDS=FALSE
);
ALTER TABLE wys.speed_counts
  OWNER TO rliu;
GRANT ALL ON TABLE wys.speed_counts TO rds_superuser WITH GRANT OPTION;
GRANT ALL ON TABLE wys.speed_counts TO dbadmin;
GRANT SELECT, REFERENCES, TRIGGER ON TABLE wys.speed_counts TO bdit_humans WITH GRANT OPTION;
GRANT ALL ON TABLE wys.speed_counts TO rliu;

-- Trigger: speed_counts_delete on wys.speed_counts

-- DROP TRIGGER speed_counts_delete ON wys.speed_counts;

CREATE TRIGGER speed_counts_delete
  AFTER DELETE
  ON wys.speed_counts
  FOR EACH ROW
  EXECUTE PROCEDURE wys.trgr_raw_data_delete();

