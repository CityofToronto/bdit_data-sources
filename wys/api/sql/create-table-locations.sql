-- Table: wys.locations

-- DROP TABLE wys.locations;

CREATE TABLE wys.locations
(
  api_id integer NOT NULL,
  address text,
  sign_name text,
  dir text,
  start_date date
)
WITH (
  OIDS=FALSE
);
ALTER TABLE wys.locations
  OWNER TO rliu;
GRANT ALL ON TABLE wys.locations TO rds_superuser WITH GRANT OPTION;
GRANT ALL ON TABLE wys.locations TO dbadmin;
GRANT SELECT, REFERENCES, TRIGGER ON TABLE wys.locations TO bdit_humans WITH GRANT OPTION;
GRANT ALL ON TABLE wys.locations TO rliu;
