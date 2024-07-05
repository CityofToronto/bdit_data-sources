-- Table: miovision_api.alerts_new

-- DROP TABLE IF EXISTS miovision_api.alerts_new;

CREATE TABLE IF NOT EXISTS miovision_api.alerts_new
(
    alert_id text COLLATE pg_catalog."default" NOT NULL,
    intersection_id text COLLATE pg_catalog."default" NOT NULL,
    alert text COLLATE pg_catalog."default" NOT NULL,
    start_time timestamp without time zone NOT NULL,
    end_time  timestamp without time zone,
    intersection_uid integer,
    CONSTRAINT miovision_alerts_pkey_new PRIMARY KEY (alert_id),
    CONSTRAINT miov_alert_intersection_fkey_new FOREIGN KEY (intersection_uid)
    REFERENCES miovision_api.intersections (intersection_uid) MATCH FULL
    ON UPDATE NO ACTION
    ON DELETE NO ACTION
)

TABLESPACE pg_default;

ALTER TABLE IF EXISTS miovision_api.alerts_new
OWNER TO miovision_admins;

REVOKE ALL ON TABLE miovision_api.alerts_new FROM bdit_humans;
REVOKE ALL ON TABLE miovision_api.alerts_new FROM miovision_api_bot;

GRANT SELECT ON TABLE miovision_api.alerts_new TO bdit_humans;

GRANT ALL ON TABLE miovision_api.alerts_new TO miovision_admins;

GRANT INSERT, SELECT, DELETE, UPDATE ON TABLE miovision_api.alerts_new TO miovision_api_bot;

COMMENT ON TABLE miovision_api.alerts_new IS E''
'This table contains Miovision alerts pulled by a daily Airflow DAG `miovision_pull`, `pull_alerts` task. '
'Note: a more detailed description is available on Miovision One.';

COMMENT ON COLUMN miovision_api.alerts_new.intersection_id
IS 'The intersection id, corresponding to intersections.intersection_id column';

COMMENT ON COLUMN miovision_api.alerts_new.alert IS E''
'Short text description of the alert. More detail on the different alerts can be found here:
https://help.miovision.com/s/article/Alert-and-Notification-Types';