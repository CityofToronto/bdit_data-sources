SET schema 'miovision_api';
CREATE TABLE volumes_tmc_zeroes
(
  volume_uid integer,
  volume_15min_tmc_uid integer,
  CONSTRAINT volumes_tmc_zeroes_volume_15min_tmc_uid_fkey FOREIGN KEY (volume_15min_tmc_uid)
      REFERENCES miovision_api.volumes_15min_tmc (volume_15min_tmc_uid) MATCH SIMPLE
      ON UPDATE NO ACTION ON DELETE CASCADE
      --This Foreign Key relationship doesn't work if `miovision_api.volumes`
      --is partitioned
      
      -- ,  CONSTRAINT volumes_tmc_zeroes_volume_uid_fkey FOREIGN KEY (volume_uid)
      -- REFERENCES miovision_api.volumes (volume_uid) MATCH SIMPLE
      -- ON UPDATE NO ACTION ON DELETE CASCADE
)
WITH (
  OIDS=FALSE
);
REVOKE INSERT, UPDATE ON TABLE volumes_tmc_zeroes FROM bdit_humans;

