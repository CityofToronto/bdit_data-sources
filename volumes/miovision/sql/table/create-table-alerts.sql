-- Table: miovision_api.alerts

-- DROP TABLE IF EXISTS miovision_api.alerts;

CREATE TABLE IF NOT EXISTS miovision_api.alerts
(
    intersection_id text COLLATE pg_catalog."default" NOT NULL,
    alert text COLLATE pg_catalog."default" NOT NULL,
    start_time timestamp without time zone NOT NULL,
    end_time timestamp without time zone,
    CONSTRAINT miovision_alerts_pkey PRIMARY KEY (intersection_id, alert, start_time)
)

TABLESPACE pg_default;

ALTER TABLE IF EXISTS miovision_api.alerts
OWNER to miovision_admins;

REVOKE ALL ON TABLE miovision_api.alerts FROM bdit_humans;
REVOKE ALL ON TABLE miovision_api.alerts FROM miovision_api_bot;

GRANT SELECT ON TABLE miovision_api.alerts TO bdit_humans;

GRANT ALL ON TABLE miovision_api.alerts TO miovision_admins;

GRANT INSERT, SELECT, DELETE, UPDATE ON TABLE miovision_api.alerts TO miovision_api_bot;

COMMENT ON TABLE miovision_api.alerts
IS 'This table contains Miovision alerts to 5 minute accuracy,
with maximum interval of 1 day. Pulled by a daily Airflow DAG `miovision_alerts`. 
Note: a more detailed description is available on Miovision One.';

COMMENT ON COLUMN miovision_api.alerts.intersection_id
IS 'The intersection id, corresponding to intersections.intersection_id column';

COMMENT ON COLUMN miovision_api.alerts.alert
IS 'Short text description of the alert.
Longer forms are available in the Miovision One UI';

COMMENT ON COLUMN miovision_api.alerts.start_time
IS 'First 5 minute interval at which the alert appeared.
Subtract 5 minutes to get earliest possible start time.';

COMMENT ON COLUMN miovision_api.alerts.end_time
IS 'Final 5 minute interval at which the alert appeared.
Add 5 minutes to get latest possible end time. Note if end
time is midnight, this could be extended on the following day.';
