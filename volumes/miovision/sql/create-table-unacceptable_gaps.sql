CREATE TABLE miovision_api.unacceptable_gaps
(
    intersection_uid integer,
    gap_start timestamp without time zone,
    gap_end timestamp without time zone,
    gap_minutes_total integer,
    allowable_total_gap_threshold integer, 
    datetime_bin timestamp without time zone,
    gap_minutes_15min integer,
    avg_historical_total_vol integer,
    avg_historical_veh_vol integer,
    CONSTRAINT intersection_uid_gap_start_gap_end_key UNIQUE (intersection_uid, gap_start, gap_end)
);

ALTER TABLE miovision_api.unacceptable_gaps OWNER to miovision_admins;

GRANT SELECT, REFERENCES, TRIGGER ON TABLE miovision_api.unacceptable_gaps TO bdit_humans WITH GRANT OPTION;

GRANT ALL ON TABLE miovision_api.unacceptable_gaps TO miovision_admins;

GRANT ALL ON TABLE miovision_api.unacceptable_gaps TO miovision_api_bot;

COMMENT ON COLUMN miovision_api.unacceptable_gaps.avg_historical_total_vol
IS '''The average historical volume from all classifications during the portion
    of the gap within the 15 minute bin starting with datetime_bin.''';

COMMENT ON COLUMN miovision_api.unacceptable_gaps.avg_historical_veh_vol;
IS '''The average historical volume where classtype = ''Vehicles'' during the
    portion of the gap within the 15 minute bin starting with datetime_bin.''';

COMMENT ON COLUMN miovision_api.unacceptable_gaps.gap_minutes_15min
IS '''The portion of the gap which falls within the 15 minute bin starting with
    datetime_bin.''';
