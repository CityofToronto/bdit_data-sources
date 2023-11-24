CREATE TABLE miovision_api.unacceptable_gaps
(
    intersection_uid integer,
    gap_start timestamp without time zone,
    gap_end timestamp without time zone,
    gap_start_floor_15 timestamp without time zone,
    gap_end_ceil_15 timestamp without time zone,
    gap_minute integer,
    allowed_gap integer,
    accept boolean,
    CONSTRAINT intersection_uid_gap_start_gap_end_key UNIQUE (intersection_uid, gap_start, gap_end)
)

ALTER TABLE miovision_api.unacceptable_gaps
    OWNER to miovision_admins;

GRANT SELECT, REFERENCES, TRIGGER ON TABLE miovision_api.unacceptable_gaps TO bdit_humans WITH GRANT OPTION;

GRANT ALL ON TABLE miovision_api.unacceptable_gaps TO miovision_admins;

GRANT ALL ON TABLE miovision_api.unacceptable_gaps TO miovision_api_bot;