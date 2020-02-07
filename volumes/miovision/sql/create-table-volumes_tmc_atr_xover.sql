-- Table: atr_tmc_uid

-- DROP TABLE atr_tmc_uid;
SET SCHEMA 'miovision_api';

CREATE TABLE volumes_tmc_atr_xover
(
  volume_15min_tmc_uid integer,
  volume_15min_uid integer,
  CONSTRAINT atr_tmc_uid_volume_15min_tmc_uid_fkey FOREIGN KEY (volume_15min_tmc_uid)
      REFERENCES volumes_15min_tmc (volume_15min_tmc_uid) MATCH SIMPLE
      ON DELETE CASCADE,
  CONSTRAINT atr_tmc_uid_volume_15min_uid_fkey FOREIGN KEY (volume_15min_uid)
      REFERENCES volumes_15min (volume_15min_uid) MATCH SIMPLE
      ON DELETE CASCADE
)
WITH (
  OIDS=FALSE
);
ALTER TABLE volumes_tmc_atr_xover
  OWNER TO miovision_admins;

CREATE INDEX ON volumes_tmc_atr_xover USING btree
    (volume_15min_tmc_uid);

CREATE INDEX ON volumes_tmc_atr_xover USING btree
    (volume_15min_uid);

GRANT ALL ON TABLE volumes_tmc_atr_xover TO rds_superuser WITH GRANT OPTION;
GRANT ALL ON TABLE volumes_tmc_atr_xover TO dbadmin;
GRANT SELECT, REFERENCES, TRIGGER ON TABLE volumes_tmc_atr_xover TO bdit_humans WITH GRANT OPTION;
