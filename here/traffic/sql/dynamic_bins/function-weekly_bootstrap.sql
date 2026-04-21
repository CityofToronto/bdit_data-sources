CREATE OR REPLACE FUNCTION here_agg.weekly_bootstrap(
    week_start date,
    segments bigint []
)
RETURNS void
LANGUAGE sql
SECURITY DEFINER
AS $$

    WITH agg_grps AS (
        SELECT
            weekly_bootstrap.week_start,
            dow_group,
            isodows,
            include_holidays,
            hrs.hr_start,
            hrs.hr_end,
            ARRAY_AGG(holiday.dt) FILTER (WHERE (wkdy_grps.dow_group = 'Weekend/Holiday' OR date_part('isodow', holiday.dt) = ANY(wkdy_grps.isodows)) AND holiday.holiday IS NOT NULL) AS holiday_exceptions
        FROM (VALUES
            ('Weekend/Holiday', ARRAY[6, 7], True),
            ('Tue-Thu', ARRAY[2, 3, 4], False),
            ('Mon-Fri', ARRAY[1, 2, 3, 4, 5], False)
        ) AS wkdy_grps(dow_group, isodows, include_holidays)
        LEFT JOIN ref.holiday ON date_trunc('week', dt)::date - 2 = weekly_bootstrap.week_start,
        UNNEST(
            array[0,7,10,15,19]::smallint[],
            array[7,10,15,19,24]::smallint[]
        ) AS hrs(hr_start, hr_end)
        GROUP BY
            dow_group,
            isodows,
            include_holidays,
            hrs.hr_start,
            hrs.hr_end
        ORDER BY
            dow_group,
            hrs.hr_start
    )
    
    INSERT INTO here_agg.segments_bootstrap_weekly (
        segment_id, dow_group, week_start, holiday_exceptions, hr_start, hr_end, avg_tt, avg_ci_lower,
        avg_ci_upper, q1_tt, q1_ci_lower, q1_ci_upper, median_tt, median_ci_lower, median_ci_upper,
        q3_tt, q3_ci_lower, q3_ci_upper, tti, n, n_resample
    )
    SELECT
        unnested.segment_id,
        agg_grps.dow_group,
        agg_grps.week_start,
        agg_grps.holiday_exceptions,
        agg_grps.hr_start,
        agg_grps.hr_end,
        lat.avg_tt,
        lat.avg_ci_lower,
        lat.avg_ci_upper,
        lat.q1_tt,
        lat.q1_ci_lower,
        lat.q1_ci_upper,
        lat.median_tt,
        lat.median_ci_lower,
        lat.median_ci_upper,
        lat.q3_tt,
        lat.q3_ci_lower,
        lat.q3_ci_upper,
        lat.avg_tt / overn.overnight_avg_tt AS tti,
        lat.n,
        300 AS n_resample
    FROM UNNEST(weekly_bootstrap.segments) AS unnested(segment_id)
    CROSS JOIN agg_grps
    LEFT JOIN LATERAL here_agg.segment_bootstrap(
        start_date := agg_grps.week_start,
        end_date := agg_grps.week_start + 7,
        segment_id := segment_id,
        n_resamples := 300,
        isodows := agg_grps.isodows::smallint[],
        include_holidays := agg_grps.include_holidays,
        hr_starts := agg_grps.hr_start::smallint,
        hr_ends := agg_grps.hr_end::smallint
    ) AS lat ON true --return rows even when no result from segment_bootstrap
    LEFT JOIN here_agg.segment_overnight_tts AS overn ON
        overn.segment_id = unnested.segment_id
        AND overn.mnth = date_trunc('month', agg_grps.week_start);

$$;

ALTER FUNCTION here_agg.weekly_bootstrap OWNER TO here_admins;
GRANT EXECUTE ON FUNCTION here_agg.weekly_bootstrap TO here_bot;
REVOKE EXECUTE ON FUNCTION here_agg.weekly_bootstrap FROM bdit_humans;
