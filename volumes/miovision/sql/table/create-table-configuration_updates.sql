-- Table: miovision_api.configuration_updates

-- DROP TABLE IF EXISTS miovision_api.configuration_updates;

CREATE TABLE IF NOT EXISTS miovision_api.configuration_updates
(
    intersection_uid integer NOT NULL,
    updated_time timestamp without time zone NOT NULL,
    CONSTRAINT mio_configuration_updates_pkey PRIMARY KEY (
        intersection_uid, updated_time
    )
)

TABLESPACE pg_default;

ALTER TABLE IF EXISTS miovision_api.configuration_updates
OWNER TO miovision_admins;

REVOKE ALL ON TABLE miovision_api.configuration_updates FROM bdit_humans;

GRANT SELECT ON TABLE miovision_api.configuration_updates TO bdit_humans;

GRANT ALL ON TABLE miovision_api.configuration_updates TO miovision_admins;

GRANT INSERT, SELECT ON TABLE miovision_api.configuration_updates TO miovision_api_bot;

COMMENT ON TABLE miovision_api.configuration_updates IS
'Stores Miovision camera configuration update timestamps. '
'Updated daily. Only populated since 2024-12-05. ';