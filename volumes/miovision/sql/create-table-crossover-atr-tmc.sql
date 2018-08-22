-- Table: atr_tmc_uid

-- DROP TABLE atr_tmc_uid;

CREATE TABLE atr_tmc_uid
(
  volume_15min_tmc_uid integer,
  volume_15min_uid integer,
  CONSTRAINT atr_tmc_uid_volume_15min_tmc_uid_fkey FOREIGN KEY (volume_15min_tmc_uid)
      REFERENCES volumes_15min_tmc (volume_15min_tmc_uid) MATCH SIMPLE
      ON UPDATE RESTRICT ON DELETE CASCADE,
  CONSTRAINT atr_tmc_uid_volume_15min_uid_fkey FOREIGN KEY (volume_15min_uid)
      REFERENCES volumes_15min (volume_15min_uid) MATCH SIMPLE
      ON UPDATE RESTRICT ON DELETE CASCADE
)
WITH (
  OIDS=FALSE
);
ALTER TABLE atr_tmc_uid
  OWNER TO rliu;
GRANT ALL ON TABLE atr_tmc_uid TO rds_superuser WITH GRANT OPTION;
GRANT ALL ON TABLE atr_tmc_uid TO dbadmin;
GRANT SELECT, REFERENCES, TRIGGER ON TABLE atr_tmc_uid TO bdit_humans WITH GRANT OPTION;
GRANT ALL ON TABLE atr_tmc_uid TO rliu;
