-- Table: wys.raw_data

-- DROP TABLE wys.raw_data;

CREATE TABLE wys.raw_data
(
  raw_data_uid bigserial NOT NULL,
  api_id integer,
  datetime_bin timestamp without time zone,
  speed integer,
  count integer,
  speed_count_uid integer,
  PRIMARY KEY (raw_data_uid),
  UNIQUE(api_id, datetime_bin, speed)

)
WITH (
  OIDS=FALSE
);
ALTER TABLE wys.raw_data
  OWNER TO wys_admins;
GRANT SELECT, REFERENCES, TRIGGER ON TABLE wys.raw_data TO bdit_humans WITH GRANT OPTION;
-- Trigger: insert_raw_data_trigger on wys.raw_data

-- DROP TRIGGER insert_raw_data_trigger ON wys.raw_data;
