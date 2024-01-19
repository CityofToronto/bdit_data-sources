CREATE TABLE miovision_api.unacceptable_gaps
(
    dt date,
    intersection_uid integer,
    gap_start timestamp without time zone,
    gap_end timestamp without time zone,
    gap_minutes_total integer,
    allowable_total_gap_threshold integer,
    datetime_bin timestamp without time zone,
    gap_minutes_15min integer,
    CONSTRAINT intersection_uid_datetime_bin_unique UNIQUE (intersection_uid, datetime_bin)
);

ALTER TABLE miovision_api.unacceptable_gaps OWNER to miovision_admins;

GRANT SELECT, REFERENCES, TRIGGER ON TABLE miovision_api.unacceptable_gaps TO bdit_humans WITH GRANT OPTION;

GRANT ALL ON TABLE miovision_api.unacceptable_gaps TO miovision_admins;

GRANT ALL ON TABLE miovision_api.unacceptable_gaps TO miovision_api_bot;

COMMENT ON TABLE miovision_api.unacceptable_gaps IS 'This table stores time ranges
containing no data for a certain camera with a minimum duration of at least
5-20 minutes. The minimum duration threshold depends on historical avg volumes
in that hour, stored in miovision_api.gapsize_lookup.';

COMMENT ON COLUMN miovision_api.unacceptable_gaps.dt
IS 'The date for which the function `miovision_api.find_gaps` was run
to insert this row.';

COMMENT ON COLUMN miovision_api.unacceptable_gaps.allowable_total_gap_threshold
IS 'The minimum duration of zero volume to be considered an unacceptable gap
for this intersection, hour, and day type (weekend/weekday) based on a 60 day
lookback and calculated in miovision_api.gapsize_lookup_insert.';

COMMENT ON COLUMN miovision_api.unacceptable_gaps.datetime_bin IS 'A 15 datetime_bin
which falls within the gap, to be used for joining to volumes_15min* tables.';

COMMENT ON COLUMN miovision_api.unacceptable_gaps.gap_minutes_15min
IS 'The portion of the total gap which falls within the 15 minute bin
starting with datetime_bin.';