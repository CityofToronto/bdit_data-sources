SET schema miovision;

CREATE TABLE volumes_tmc_zeroes (
	volume_uid INT ,
	volume_15min_tmc_uid INT,
	 FOREIGN KEY (volume_uid)
        REFERENCES rdumas.volumes (volume_uid) MATCH SIMPLE
        ON DELETE CASCADE,
	 FOREIGN KEY (volume_15min_tmc_uid)
        REFERENCES rdumas.volumes_15min_tmc (volume_15min_tmc_uid) MATCH SIMPLE
        ON DELETE CASCADE
)