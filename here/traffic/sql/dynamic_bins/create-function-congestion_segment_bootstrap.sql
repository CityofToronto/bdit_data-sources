--DROP FUNCTION here_agg.segment_bootstrap;

CREATE OR REPLACE FUNCTION here_agg.segment_bootstrap(
    start_date date,
    end_date date,
    segment_id bigint,
    n_resamples int,
    isodows smallint[],
    include_holidays boolean,
    hr_starts smallint,
    hr_ends smallint
)
RETURNS TABLE (
    segment_id bigint,
    start_date date,
    end_date date,
    hr_start smallint,
    hr_end smallint,
    avg_tt real,
    avg_ci_lower real,
    avg_ci_upper real,
    q1_tt real,
    q1_ci_lower real,
    q1_ci_upper real,
    median_tt real,
    median_ci_lower real,
    median_ci_upper real,
    q3_tt real,
    q3_ci_lower real,
    q3_ci_upper real,
    n int,
    n_resample int,
    length numeric
)
LANGUAGE sql
COST 100
VOLATILE PARALLEL SAFE
AS $BODY$
    
    SELECT setseed(('0.'||segment_id::text)::numeric);

    WITH raw_obs AS (
        SELECT
            --segment_id and mnth don't need to be in group by until end
            ARRAY_AGG(tt::real) AS tt_array,
            AVG(tt::real) AS avg_tt,
            percentile_cont(0.25) WITHIN GROUP (ORDER BY tt::real)::real AS q1_tt,
            percentile_cont(0.50) WITHIN GROUP (ORDER BY tt::real)::real AS q2_tt,
            percentile_cont(0.75) WITHIN GROUP (ORDER BY tt::real)::real AS q3_tt,
            COUNT(*) AS n
        FROM here_agg.raw_segments AS rs
        WHERE -- same params as the above aggregation
            dt >= segment_bootstrap.start_date
            AND dt < segment_bootstrap.end_date
            AND rs.hr >= segment_bootstrap.hr_starts
            AND rs.hr < segment_bootstrap.hr_ends
            AND segment_id = segment_bootstrap.segment_id
            AND EXTRACT('isodow' FROM dt) = ANY(segment_bootstrap.isodows)
            AND NOT (
                segment_bootstrap.include_holidays = False
                AND EXISTS (
                    SELECT 1 FROM ref.holiday WHERE rs.dt = holiday.dt
                )
            )
    ),
    
    random_selections AS (
        SELECT
            raw_obs.avg_tt,
            raw_obs.q1_tt,
            raw_obs.q2_tt,
            raw_obs.q3_tt,
            raw_obs.n,
            sample_group.group_id,
            --get a random observation from the array of tts
            AVG(raw_obs.tt_array[ceiling(random() * raw_obs.n)]) AS rnd_avg_tt,
            percentile_cont(0.25) WITHIN GROUP (ORDER BY raw_obs.tt_array[ceiling(random() * raw_obs.n)])::real AS rnd_q1_tt,
            percentile_cont(0.5) WITHIN GROUP (ORDER BY raw_obs.tt_array[ceiling(random() * raw_obs.n)])::real AS rnd_q2_tt,
            percentile_cont(0.75) WITHIN GROUP (ORDER BY raw_obs.tt_array[ceiling(random() * raw_obs.n)])::real AS rnd_q3_tt
        FROM raw_obs
        CROSS JOIN generate_series(1, n)
        -- 200 resamples (could be any number)
        CROSS JOIN generate_series(1, segment_bootstrap.n_resamples) AS sample_group(group_id)
        GROUP BY
            raw_obs.avg_tt,
            raw_obs.q1_tt,
            raw_obs.q2_tt,
            raw_obs.q3_tt,
            raw_obs.n,
            sample_group.group_id
    )
    
    SELECT
        segment_bootstrap.segment_id,
        segment_bootstrap.start_date,
        segment_bootstrap.end_date,
        segment_bootstrap.hr_starts,
        segment_bootstrap.hr_ends,
        avg_tt::real AS avg_tt,
        percentile_cont(0.025) WITHIN GROUP (ORDER BY rnd_avg_tt)::real AS avg_ci_lower,
        percentile_cont(0.975) WITHIN GROUP (ORDER BY rnd_avg_tt)::real AS avg_ci_upper,
        q1_tt,
        percentile_cont(0.025) WITHIN GROUP (ORDER BY rnd_q1_tt)::real AS q1_ci_lower,
        percentile_cont(0.975) WITHIN GROUP (ORDER BY rnd_q1_tt)::real AS q1_ci_upper,
        q2_tt AS median_tt,
        percentile_cont(0.025) WITHIN GROUP (ORDER BY rnd_q2_tt)::real AS median_ci_lower,
        percentile_cont(0.975) WITHIN GROUP (ORDER BY rnd_q2_tt)::real AS median_ci_upper,
        q3_tt,
        percentile_cont(0.025) WITHIN GROUP (ORDER BY rnd_q3_tt)::real AS q3_ci_lower,
        percentile_cont(0.975) WITHIN GROUP (ORDER BY rnd_q3_tt)::real AS q3_ci_upper,
        n,
        n_resamples,
        cs.total_length
    FROM random_selections
    JOIN congestion.congestion_segments AS cs
        ON cs.segment_id = segment_bootstrap.segment_id
        AND cs.ver_id = here_agg.select_map_version(start_date, start_date + 1, 'path_hm')
    GROUP BY
        hr_starts,
        hr_ends,
        avg_tt,
        q1_tt,
        q2_tt,
        q3_tt,
        n,
        cs.total_length;

    $BODY$;

GRANT EXECUTE ON FUNCTION here_agg.segment_bootstrap(
    date, date, bigint, int, smallint[], boolean, smallint, smallint
) TO congestion_bot;


/*Usage example: (works best one segment at a time with Lateral)
--16s 8 segments, 1 week
SELECT wkdy_grps.*, lat.*
FROM UNNEST('{2,3,4,5,6,7,8,9}'::bigint[]) AS unnested(segment_id),
(VALUES
    ('Weekend/Holiday', ARRAY[6, 7], True),
    ('Tue-Thu', ARRAY[2, 3, 4], False),
    ('Mon-Fri', ARRAY[1, 2, 3, 4, 5], False)
) AS wkdy_grps(dow_group, isodows, include_holidays),
LATERAL (
    SELECT * FROM here_agg.segment_bootstrap(
                    start_date := '2025-12-01'::date,
                    end_date := '2025-12-07'::date,
                    segment_id := segment_id::bigint,
                    n_resamples := 300::int,
                    hr_starts := 0::smallint,
                    hr_ends := 7::smallint,
                    isodows := wkdy_grps.isodows::smallint[],
                    include_holidays := wkdy_grps.include_holidays
            )
) AS lat
*/