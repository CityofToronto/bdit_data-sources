CREATE TABLE miovision_api.report_dates
(
    intersection_uid integer,
    class_type text COLLATE pg_catalog."default",
    period_type text COLLATE pg_catalog."default",
    dt date
    CONSTRAINT intersection_uid_class_period_dt_unique_key UNIQUE (intersection_uid, class_type, period_type, dt)
)