-- Table: miovision_api.anomaly_investigation_levels

-- DROP TABLE IF EXISTS miovision_api.anomaly_investigation_levels;

CREATE TABLE IF NOT EXISTS miovision_api.anomaly_investigation_levels
(
    uid text COLLATE pg_catalog."default" NOT NULL,
    description text COLLATE pg_catalog."default" NOT NULL,
    CONSTRAINT qa_levels_pkey PRIMARY KEY (uid)
)

TABLESPACE pg_default;

ALTER TABLE IF EXISTS miovision_api.anomaly_investigation_levels
OWNER TO miovision_admins;

REVOKE ALL ON TABLE miovision_api.anomaly_investigation_levels FROM bdit_humans;
REVOKE ALL ON TABLE miovision_api.anomaly_investigation_levels FROM miovision_data_detectives;

GRANT SELECT ON TABLE miovision_api.anomaly_investigation_levels TO bdit_humans;

GRANT ALL ON TABLE miovision_api.anomaly_investigation_levels TO miovision_admins;

GRANT REFERENCES ON TABLE miovision_api.anomaly_investigation_levels TO miovision_data_detectives;

GRANT ALL ON TABLE miovision_api.anomaly_investigation_levels TO scannon;

COMMENT ON TABLE miovision_api.anomaly_investigation_levels
IS 'Indicates the furthest degree to which the movision QA issue has been investigated. Is this only a suspicion? Or has the issue been fully confirmed/resolved?';