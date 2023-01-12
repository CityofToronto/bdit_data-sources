CREATE TABLE miovision_csv.unacceptable_gaps_2020
(
    intersection_uid integer,
    gap_start timestamp without time zone,
    gap_end timestamp without time zone,
    gap_minute integer,
    allowed_gap integer,
    accept boolean,
    CONSTRAINT intersection_uid_gap_start_gap_end_key UNIQUE (intersection_uid, gap_start, gap_end)
)