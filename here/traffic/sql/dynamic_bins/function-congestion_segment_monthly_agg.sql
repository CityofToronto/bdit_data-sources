-- FUNCTION: here_agg.segment_monthly_agg(date)

-- DROP FUNCTION IF EXISTS here_agg.segment_monthly_agg(date);

CREATE OR REPLACE FUNCTION here_agg.segment_monthly_agg(
    mon date
)
RETURNS void
LANGUAGE SQL
COST 100
VOLATILE PARALLEL UNSAFE
AS $BODY$

INSERT INTO here_agg.segments_monthy_summary (
    segment_id, mnth, is_wkdy, hr, avg_tt, stdev, percentile_05, percentile_15,
    percentile_50, percentile_85, percentile_95, num_quasi_obs
)
SELECT
    segment_id,
    congestion_segment_monthly_agg.mon AS mnth,
    date_part('isodow', dt) <= 5 AS is_wkdy,
    hr,
    AVG(tt) AS avg_tt,
    stddev(tt) AS stdev,
    PERCENTILE_CONT(0.05) WITHIN GROUP (ORDER BY tt) AS percentile_05,
    PERCENTILE_CONT(0.15) WITHIN GROUP (ORDER BY tt) AS percentile_15,
    PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY tt) AS percentile_50,
    PERCENTILE_CONT(0.85) WITHIN GROUP (ORDER BY tt) AS percentile_85,
    PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY tt) AS percentile_95,
    COUNT(*) AS num_quasi_obs
FROM here_agg.raw_segments
LEFT JOIN ref.holiday USING (dt)
WHERE
    dt >= congestion_segment_monthly_agg.mon
    AND dt < congestion_segment_monthly_agg.mon + interval '1 month'
    AND holiday.holiday IS NULL
GROUP BY
    segment_id,
    hr,
    is_wkdy;

$BODY$;

ALTER FUNCTION here_agg.segment_monthly_agg(date)
OWNER TO here_admins;

GRANT EXECUTE ON FUNCTION here_agg.segment_monthly_agg(date) TO public;

GRANT EXECUTE ON FUNCTION here_agg.segment_monthly_agg(date) TO congestion_bot;

