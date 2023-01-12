CREATE TABLE miovision_api.report_dates
(
    intersection_uid integer,
    class_type text COLLATE pg_catalog."default",
    period_type text COLLATE pg_catalog."default",
    dt date,
    CONSTRAINT intersection_uid_class_period_dt_unique_key UNIQUE (intersection_uid, class_type, period_type, dt)
);

ALTER TABLE miovision_api.report_dates
    OWNER to miovision_admins;

GRANT SELECT, TRIGGER, REFERENCES ON TABLE miovision_api.report_dates TO jchew;

GRANT ALL ON TABLE miovision_api.report_dates TO bdit_bots;

GRANT TRIGGER, REFERENCES, SELECT ON TABLE miovision_api.report_dates TO bdit_humans WITH GRANT OPTION;

GRANT ALL ON TABLE miovision_api.report_dates TO dbadmin;

GRANT ALL ON TABLE miovision_api.report_dates TO miovision_admins;

GRANT ALL ON TABLE miovision_api.report_dates TO rds_superuser WITH GRANT OPTION;
-- Index: report_dates_class_type_idx

-- DROP INDEX miovision_api.report_dates_class_type_idx;

CREATE INDEX report_dates_class_type_idx
    ON miovision_api.report_dates USING btree
    (class_type COLLATE pg_catalog."default" ASC NULLS LAST)
    TABLESPACE pg_default;
-- Index: report_dates_dt_idx

-- DROP INDEX miovision_api.report_dates_dt_idx;

CREATE INDEX report_dates_dt_idx
    ON miovision_api.report_dates USING btree
    (dt ASC NULLS LAST)
    TABLESPACE pg_default;
-- Index: report_dates_intersection_idx

-- DROP INDEX miovision_api.report_dates_intersection_idx;

CREATE INDEX report_dates_intersection_idx
    ON miovision_api.report_dates USING btree
    (intersection_uid ASC NULLS LAST)
    TABLESPACE pg_default;