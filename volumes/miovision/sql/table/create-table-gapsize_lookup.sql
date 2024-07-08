CREATE TABLE miovision_api.gapsize_lookup
(
    dt date NOT NULL,
    intersection_uid integer NOT NULL,
    classification_uid integer,
    hour_bin smallint NOT NULL,
    weekend boolean NOT NULL,
    avg_hour_vol numeric,
    gap_tolerance smallint,
    CONSTRAINT gapsize_lookup_unique
    UNIQUE NULLS NOT DISTINCT (dt, intersection_uid, hour_bin, classification_uid)
);

CREATE INDEX ON miovision_api.gapsize_lookup USING brin (dt);

ALTER TABLE miovision_api.gapsize_lookup
OWNER TO miovision_admins;

GRANT SELECT, REFERENCES, TRIGGER ON TABLE miovision_api.gapsize_lookup
TO bdit_humans WITH GRANT OPTION;

GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE miovision_api.gapsize_lookup
TO miovision_api_bot;

COMMENT ON TABLE miovision_api.gapsize_lookup
IS 'This table stores the minimum gap duration to be considered an unacceptable_gap,
used in the `miovision_api.find_gaps` function. It also stores the average volumes
for each classification during the same avg historical hour, to be used in the 
volumes_daily table.';

COMMENT ON COLUMN miovision_api.gapsize_lookup.dt IS
'The date for which `miovision_api.gapsize_lookup_insert` was run to generate this row.
The lookback period used to is the 60 days preceeding this date, matching the same 
day type (weekday/weekend)';

COMMENT ON COLUMN miovision_api.gapsize_lookup.classification_uid IS
'A null classification_uid refers to the total volume for that intersection
which is used to determine the gap_tolerance.';

COMMENT ON COLUMN miovision_api.gapsize_lookup.hour_bin IS
'Hour of the day from 0-23.';

COMMENT ON COLUMN miovision_api.gapsize_lookup.weekend IS
'True if Saturday/Sunday or holiday (based on ref.holiday table).';

COMMENT ON COLUMN miovision_api.gapsize_lookup.avg_hour_vol IS
'The average volume for this hour/intersection/weekend combination
based on a 60 day lookback.';

COMMENT ON COLUMN miovision_api.gapsize_lookup.gap_tolerance IS
'The minimum gap duration to be considered an unacceptable_gap. Only valid
for the overall intersection volume (classification_uid IS NULL).';