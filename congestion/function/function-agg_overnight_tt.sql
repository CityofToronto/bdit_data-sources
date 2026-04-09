CREATE OR REPLACE FUNCTION here_agg.agg_overnight_tt(
    mnth date
)
RETURNS void
SECURITY DEFINER
AS $$

    WITH windowed AS (
        SELECT DISTINCT
            segment_id,
            date_part('isodow', dt) <= 5 AS is_wkdy,
            dt,
            AVG(tt) OVER (PARTITION BY segment_id, date_part('isodow', dt) <= 5 ORDER BY dt RANGE BETWEEN '6 months' PRECEDING AND '1 day' PRECEDING) AS overnight_avg_tt,
            --alternative: weighted avg
            --SUM(tt * num_obs) / SUM(num_obs) OVER (PARTITION BY segment_id, date_part('isodow', dt) <= 5 ORDER BY dt RANGE BETWEEN '6 months' PRECEDING AND '1 day' PRECEDING) AS overnight_avg_tt
            COUNT(*) OVER (PARTITION BY segment_id, date_part('isodow', dt) <= 5 ORDER BY dt RANGE BETWEEN '6 months' PRECEDING AND '1 day' PRECEDING) AS rolling_6month_quasi_obs
        FROM here_agg.raw_segments
        WHERE
            dt >= agg_overnight_tt.mnth - interval '6 months'
            AND dt < agg_overnight_tt.mnth + interval '1 month'
            AND (
                (
                    dt < '2024-01-01'::date
                    AND hr BETWEEN 0 AND 3
                ) OR (
                    dt >= '2024-01-01'::date
                    AND hr BETWEEN 1 AND 4
                )
            )
        ORDER BY
            is_wkdy DESC,
            segment_id,
            dt
    )

    INSERT INTO here_agg.segment_overnight_tts (
        segment_id, dt, is_wkdy, overnight_avg_tt, rolling_6month_quasi_obs
    )
    SELECT
        segment_id,
        dt,
        is_wkdy,
        overnight_avg_tt,
        rolling_6month_quasi_obs
    FROM windowed
    WHERE
        dt >= agg_overnight_tt.mnth
    ON CONFLICT ON CONSTRAINT segment_overnight_tts_pkey
    DO UPDATE
    SET
        overnight_avg_tt = EXCLUDED.overnight_avg_tt,
        rolling_6month_quasi_obs = EXCLUDED.rolling_6month_quasi_obs;
       
$$ LANGUAGE sql;

COMMENT ON FUNCTION here_agg.agg_overnight_tt IS 'Aggregate a month worth of rolling 6 month overnight travel times. Uses an ON CONFLICT DO UPDATE clause - can be re-run when input data changes. Takes around 1-2 minutes per month.';

ALTER FUNCTION here_agg.agg_overnight_tt OWNER TO here_admins;

REVOKE EXECUTE ON FUNCTION here_agg.agg_overnight_tt FROM public;

GRANT EXECUTE ON FUNCTION here_agg.agg_overnight_tt TO congestion_bot;
