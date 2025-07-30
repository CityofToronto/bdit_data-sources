-- FUNCTION: gwolofs.congestion_segment_monthly_agg(date)

-- DROP FUNCTION IF EXISTS gwolofs.congestion_segment_monthly_agg(date);

CREATE OR REPLACE FUNCTION gwolofs.congestion_segment_monthly_agg(
	mon date)
    RETURNS void
    LANGUAGE 'sql'
    COST 100
    VOLATILE PARALLEL UNSAFE
AS $BODY$

INSERT INTO gwolofs.congestion_segments_monthy_summary (
    segment_id, mnth, is_wkdy, hr, avg_tt, stdev, percentile_05, percentile_15,
    percentile_50, percentile_85, percentile_95, num_quasi_obs
)
SELECT
    segment_id,
    congestion_segment_monthly_agg.mon AS mnth,
    date_part('isodow', dt) <= 5 AS is_wkdy,
    date_part('hour', hr) AS hr,
    ROUND(AVG(tt), 2) AS avg_tt,
    ROUND(stddev(tt), 2) AS stdev,
    ROUND(PERCENTILE_CONT(0.05) WITHIN GROUP (ORDER BY tt)::numeric, 2) AS percentile_05,
    ROUND(PERCENTILE_CONT(0.15) WITHIN GROUP (ORDER BY tt)::numeric, 2) AS percentile_15,
    ROUND(PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY tt)::numeric, 2) AS percentile_50,
    ROUND(PERCENTILE_CONT(0.85) WITHIN GROUP (ORDER BY tt)::numeric, 2) AS percentile_85,
    ROUND(PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY tt)::numeric, 2) AS percentile_95,
    COUNT(*) AS num_quasi_obs
FROM gwolofs.congestion_raw_segments
LEFT JOIN ref.holiday USING (dt)
WHERE
    dt >= congestion_segment_monthly_agg.mon
    AND dt < congestion_segment_monthly_agg.mon + interval '1 month'
    AND holiday.holiday IS NULL
GROUP BY
    segment_id,
    date_part('hour', hr),
    is_wkdy;

$BODY$;

ALTER FUNCTION gwolofs.congestion_segment_monthly_agg(date)
    OWNER TO gwolofs;

GRANT EXECUTE ON FUNCTION gwolofs.congestion_segment_monthly_agg(date) TO PUBLIC;

GRANT EXECUTE ON FUNCTION gwolofs.congestion_segment_monthly_agg(date) TO congestion_bot;

GRANT EXECUTE ON FUNCTION gwolofs.congestion_segment_monthly_agg(date) TO gwolofs;

