CREATE OR REPLACE FUNCTION here_agg.agg_overnight_tt(
    mnth date
)
RETURNS void
SECURITY DEFINER
AS $$

    INSERT INTO here_agg.segment_overnight_tts (
        segment_id, dt, is_wkdy, overnight_avg_tt, overnight_avg_tt_weighted, rolling_6month_quasi_obs
    )
    SELECT
        rs.segment_id,
        gs.dt::date AS dt,
        date_part('isodow', gs.dt) <= 5 AS is_wkdy,
        AVG(tt) AS overnight_avg_tt,
        --alternative: weighted avg
        SUM(tt * num_obs) / SUM(num_obs) AS overnight_avg_tt_weighted,
        COUNT(*) AS rolling_6month_quasi_obs
    FROM generate_series(
        agg_overnight_tt.mnth,
        agg_overnight_tt.mnth::date + interval '1 month' - interval '1 day',
        interval '1 day'
    ) AS gs(dt)
    LEFT JOIN here_agg.raw_segments AS rs ON
        --range between
        rs.dt >= gs.dt - interval '6 months' --6 months before
        AND rs.dt < gs.dt --1 day preceding
        AND (date_part('isodow', gs.dt) <= 5) = (date_part('isodow', rs.dt) <= 5 ) --is_wkdy = is_wkdy
        AND (
            (
                rs.dt < '2024-01-01'::date
                AND rs.hr BETWEEN 0 AND 3
            ) OR (
                rs.dt >= '2024-01-01'::date
                AND rs.hr BETWEEN 1 AND 4
            )
        )
    WHERE
        --partition selection
        rs.dt >= agg_overnight_tt.mnth::date - interval '6 months'
        AND rs.dt < agg_overnight_tt.mnth::date + interval '1 month'
    GROUP BY
        rs.segment_id,
        date_part('isodow', gs.dt) <= 5,
        gs.dt
    ORDER BY
        gs.dt,
        is_wkdy DESC,
        rs.segment_id
    ON CONFLICT ON CONSTRAINT segment_overnight_tts_pkey
    DO UPDATE
    SET
        overnight_avg_tt = EXCLUDED.overnight_avg_tt,
        overnight_avg_tt_weighted = EXCLUDED.overnight_avg_tt_weighted,
        rolling_6month_quasi_obs = EXCLUDED.rolling_6month_quasi_obs;
       
$$ LANGUAGE sql;

COMMENT ON FUNCTION here_agg.agg_overnight_tt IS 'Aggregate a month worth of rolling 6 month overnight travel times. Uses an ON CONFLICT DO UPDATE clause - can be re-run when input data changes. Takes around 1-2 minutes per month.';

ALTER FUNCTION here_agg.agg_overnight_tt OWNER TO here_admins;

REVOKE EXECUTE ON FUNCTION here_agg.agg_overnight_tt FROM public;

GRANT EXECUTE ON FUNCTION here_agg.agg_overnight_tt TO congestion_bot;
