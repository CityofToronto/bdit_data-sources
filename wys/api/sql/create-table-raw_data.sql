-- Table: wys.raw_data

-- DROP TABLE wys.raw_data;

CREATE TABLE wys.raw_data
(
  raw_data_uid serial NOT NULL,
  api_id integer,
  datetime_bin timestamp without time zone,
  speed integer,
  count integer,
  speed_count_uid integer,
  CONSTRAINT wys_raw_data_uid_pkey PRIMARY KEY (raw_data_uid)
)
WITH (
  OIDS=FALSE
);
ALTER TABLE wys.raw_data
  OWNER TO rliu;
GRANT ALL ON TABLE wys.raw_data TO rds_superuser WITH GRANT OPTION;
GRANT ALL ON TABLE wys.raw_data TO dbadmin;
GRANT SELECT, REFERENCES, TRIGGER ON TABLE wys.raw_data TO bdit_humans WITH GRANT OPTION;
GRANT ALL ON TABLE wys.raw_data TO rliu;

-- Trigger: insert_raw_data_trigger on wys.raw_data

-- DROP TRIGGER insert_raw_data_trigger ON wys.raw_data;

CREATE TRIGGER insert_raw_data_trigger
  BEFORE INSERT
  ON wys.raw_data
  FOR EACH ROW
  EXECUTE PROCEDURE wys.speed_bins();

-- Trigger: raw_data_delete on wys.raw_data

-- DROP TRIGGER raw_data_delete ON wys.raw_data;

CREATE TRIGGER raw_data_delete
  AFTER DELETE
  ON wys.raw_data
  FOR EACH ROW
  EXECUTE PROCEDURE wys.trgr_raw_data_delete();

