CREATE TABLE miovision_api.volumes_tmc_atr_xover
(
  volume_15min_tmc_uid integer,
  volume_15min_uid integer,
  CONSTRAINT volumes_tmc_atr_xover_volume_15min_tmc_uid_fkey FOREIGN KEY (volume_15min_tmc_uid)
      REFERENCES miovision_api.volumes_15min_tmc (volume_15min_tmc_uid) MATCH SIMPLE
      ON UPDATE RESTRICT ON DELETE CASCADE,
  CONSTRAINT volumes_tmc_atr_xover_volume_15min_uid_fkey FOREIGN KEY (volume_15min_uid)
      REFERENCES miovision_api.volumes_15min (volume_15min_uid) MATCH SIMPLE
      ON UPDATE RESTRICT ON DELETE CASCADE
)
WITH (
  OIDS=FALSE
);