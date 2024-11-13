CREATE VIEW ecocounter.calibration_factors AS
WITH dates AS (
    SELECT
        UNNEST(vr.flow_ids) AS flow_id,
        vr.count_date,
        vr.ecocounter_day_corr_factor,
        sh.setting,
        sh.date_range AS sensitivity_date_range,
        LAG(vr.count_date) OVER w AS last_count_date,
        LEAD(vr.count_date) OVER w AS next_count_date
    FROM ecocounter.sensitivity_history AS sh
    LEFT JOIN ecocounter.validation_results AS vr
        ON
        sh.flow_id = ANY(vr.flow_ids)
        AND vr.count_date <@ sh.date_range
    WINDOW w AS (PARTITION BY sh.flow_id, sh.date_range ORDER BY vr.count_date)
    ORDER BY
        sh.flow_id,
        vr.count_date
)

SELECT
    flow_id,
    count_date,
    ecocounter_day_corr_factor,
    setting,
    sensitivity_date_range,
    daterange(
        --if there's a previous study, let that one take precedent for days before count_date.
        CASE
            WHEN last_count_date IS NULL THEN LOWER(sensitivity_date_range)
            ELSE count_date
        END,
        --if there's a future study, let that one take precedent.
        COALESCE(next_count_date, UPPER(sensitivity_date_range))
    ) AS factor_range
FROM dates
ORDER BY
    flow_id,
    count_date;

COMMENT ON VIEW ecocounter.calibration_factors IS
'(in development) - table of validation results and dates to apply '
'calibration factors based on other validation studies and sensitivity history.';

ALTER VIEW ecocounter.calibration_factors OWNER TO ecocounter_admins;