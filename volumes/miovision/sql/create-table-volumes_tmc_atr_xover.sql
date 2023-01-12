
CREATE TABLE miovision_api.volumes_mvt_atr_xover
(
    volume_15min_mvt_uid integer,
    volume_15min_uid integer,
    CONSTRAINT atr_mvt_uid_volume_15min_mvt_uid_fkey FOREIGN KEY (volume_15min_mvt_uid)
        REFERENCES miovision_api.volumes_15min_mvt (volume_15min_mvt_uid) MATCH SIMPLE
        ON UPDATE RESTRICT
        ON DELETE CASCADE,
    CONSTRAINT atr_mvt_uid_volume_15min_uid_fkey FOREIGN KEY (volume_15min_uid)
        REFERENCES miovision_api.volumes_15min (volume_15min_uid) MATCH SIMPLE
        ON UPDATE RESTRICT
        ON DELETE CASCADE
)
WITH (
    OIDS = FALSE
)
TABLESPACE pg_default;

ALTER TABLE miovision_api.volumes_mvt_atr_xover
    OWNER to miovision_admins;

GRANT ALL ON TABLE miovision_api.volumes_mvt_atr_xover TO bdit_bots;

GRANT SELECT, REFERENCES, TRIGGER ON TABLE miovision_api.volumes_mvt_atr_xover TO bdit_humans WITH GRANT OPTION;

GRANT ALL ON TABLE miovision_api.volumes_mvt_atr_xover TO dbadmin;

GRANT ALL ON TABLE miovision_api.volumes_mvt_atr_xover TO miovision_admins;

GRANT ALL ON TABLE miovision_api.volumes_mvt_atr_xover TO rds_superuser WITH GRANT OPTION;
-- Index: volumes_mvt_atr_xover_volume_15min_mvt_uid_idx

-- DROP INDEX miovision_api.volumes_mvt_atr_xover_volume_15min_mvt_uid_idx;

CREATE INDEX volumes_mvt_atr_xover_volume_15min_mvt_uid_idx
    ON miovision_api.volumes_mvt_atr_xover USING btree
    (volume_15min_mvt_uid ASC NULLS LAST)
    TABLESPACE pg_default;
-- Index: volumes_mvt_atr_xover_volume_15min_mvt_uid_idx1

-- DROP INDEX miovision_api.volumes_mvt_atr_xover_volume_15min_mvt_uid_idx1;

CREATE INDEX volumes_mvt_atr_xover_volume_15min_mvt_uid_idx1
    ON miovision_api.volumes_mvt_atr_xover USING btree
    (volume_15min_mvt_uid ASC NULLS LAST)
    TABLESPACE pg_default;
-- Index: volumes_mvt_atr_xover_volume_15min_uid_idx

-- DROP INDEX miovision_api.volumes_mvt_atr_xover_volume_15min_uid_idx;

CREATE INDEX volumes_mvt_atr_xover_volume_15min_uid_idx
    ON miovision_api.volumes_mvt_atr_xover USING btree
    (volume_15min_uid ASC NULLS LAST)
    TABLESPACE pg_default;