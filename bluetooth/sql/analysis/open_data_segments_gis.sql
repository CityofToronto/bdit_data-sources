DROP VIEW open_data.bluetooth_segments;

CREATE OR REPLACE VIEW open_data.bluetooth_segments AS
SELECT
    segments.segment_name AS "resultId",
    segments.street AS start_street,
    segments.direction,
    segments.start_crossstreet AS start_cross_street,
    segments.end_street,
    segments.end_crossstreet AS end_cross_street,
    report_active_dates.start_date,
    report_active_dates.end_date,
    segments.length,
    segments.geom
FROM bluetooth.segments
JOIN bluetooth.report_active_dates USING (analysis_id)
WHERE NOT segments.duplicate;

ALTER TABLE open_data.bluetooth_segments
OWNER TO rdumas;

GRANT SELECT ON TABLE open_data.bluetooth_segments TO od_extract_svc;
GRANT ALL ON TABLE open_data.bluetooth_segments TO rdumas;
GRANT SELECT ON TABLE open_data.bluetooth_segments TO bdit_humans;
