--DROP FUNCTION here_agg.segment_bootstrap(date, date, bigint, int, smallint[], smallint[]);


CREATE OR REPLACE FUNCTION here_agg.segment_bootstrap(
    start_date date,
    end_date date,
    segment_id bigint,
    n_resamples int,
    hr_starts smallint[] default array[0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23],
    hr_ends smallint[] default array[1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24]
)
RETURNS TABLE (
    segment_id bigint,
    start_date date,
    end_date date,
    is_wkdy boolean,
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
    n_resample int
)
LANGUAGE SQL
COST 100
VOLATILE PARALLEL SAFE
AS $BODY$

    SELECT setseed(('0.'||segment_id::text)::numeric);

    WITH hours AS (
        SELECT
            unnested.hr_start,
            unnested.hr_end
        FROM UNNEST(
            segment_bootstrap.hr_starts,
            segment_bootstrap.hr_ends
        ) AS unnested(hr_start, hr_end)
    ),
    
    raw_obs AS (
        SELECT
            --segment_id and mnth don't need to be in group by until end
            EXTRACT('isodow' FROM dt) IN (1, 2, 3, 4, 5) AS is_wkdy,
            hours.hr_start,
            hours.hr_end,
            ARRAY_AGG(tt::real) AS tt_array,
            AVG(tt::real) AS avg_tt,
            percentile_disc(0.25) WITHIN GROUP (ORDER BY tt::real)::real AS q1_tt,
            percentile_disc(0.50) WITHIN GROUP (ORDER BY tt::real)::real AS q2_tt,
            percentile_disc(0.75) WITHIN GROUP (ORDER BY tt::real)::real AS q3_tt,
            COUNT(*) AS n
        FROM here_agg.raw_segments AS rs
        JOIN hours ON
            rs.hr >= hours.hr_start
            AND rs.hr < hours.hr_end
        WHERE -- same params as the above aggregation
            dt >= segment_bootstrap.start_date
            AND dt < segment_bootstrap.end_date
            AND segment_id = segment_bootstrap.segment_id
        GROUP BY
            segment_id,
            is_wkdy,
            hours.hr_start,
            hours.hr_end
    ),
    
    random_selections AS (
        SELECT
            raw_obs.is_wkdy,
            raw_obs.hr_start,
            raw_obs.hr_end,
            raw_obs.avg_tt,
            raw_obs.q1_tt,
            raw_obs.q2_tt,
            raw_obs.q3_tt,
            raw_obs.n,
            sample_group.group_id,
            --get a random observation from the array of tts
            AVG(raw_obs.tt_array[ceiling(random() * raw_obs.n)]) AS rnd_avg_tt,
            percentile_disc(0.25) WITHIN GROUP (ORDER BY raw_obs.tt_array[ceiling(random() * raw_obs.n)])::real AS rnd_q1_tt,
            percentile_disc(0.5) WITHIN GROUP (ORDER BY raw_obs.tt_array[ceiling(random() * raw_obs.n)])::real AS rnd_q2_tt,
            percentile_disc(0.75) WITHIN GROUP (ORDER BY raw_obs.tt_array[ceiling(random() * raw_obs.n)])::real AS rnd_q3_tt
        FROM raw_obs
        CROSS JOIN generate_series(1, n)
        -- 200 resamples (could be any number)
        CROSS JOIN generate_series(1, segment_bootstrap.n_resamples) AS sample_group(group_id)
        GROUP BY
            raw_obs.is_wkdy,
            raw_obs.hr_start,
            raw_obs.hr_end,
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
        is_wkdy,
        hr_start,
        hr_end,
        avg_tt::real AS avg_tt,
        percentile_disc(0.025) WITHIN GROUP (ORDER BY rnd_avg_tt)::real AS avg_ci_lower,
        percentile_disc(0.975) WITHIN GROUP (ORDER BY rnd_avg_tt)::real AS avg_ci_upper,
        q1_tt,
        percentile_disc(0.025) WITHIN GROUP (ORDER BY rnd_q1_tt)::real AS q1_ci_lower,
        percentile_disc(0.975) WITHIN GROUP (ORDER BY rnd_q1_tt)::real AS q1_ci_upper,
        q2_tt AS median_tt,
        percentile_disc(0.025) WITHIN GROUP (ORDER BY rnd_q2_tt)::real AS median_ci_lower,
        percentile_disc(0.975) WITHIN GROUP (ORDER BY rnd_q2_tt)::real AS median_ci_upper,
        q3_tt,
        percentile_disc(0.025) WITHIN GROUP (ORDER BY rnd_q3_tt)::real AS q3_ci_lower,
        percentile_disc(0.975) WITHIN GROUP (ORDER BY rnd_q3_tt)::real AS q3_ci_upper,
        n,
        n_resamples
    FROM random_selections
    GROUP BY
        is_wkdy,
        hr_start,
        hr_end,
        avg_tt,
        q1_tt,
        q2_tt,
        q3_tt,
        n;


    $BODY$;

GRANT EXECUTE ON FUNCTION here_agg.segment_bootstrap(
    date, date, bigint, int, smallint[], smallint[]
) TO congestion_bot;

/*Usage example: (works best one segment at a time with Lateral)
SELECT lat.*
FROM UNNEST('{1,2,3,4,5,6,7,8,9}'::bigint[]) AS unnested(segment_id),
LATERAL (
    SELECT * FROM here_agg.segment_bootstrap(
                    start_date := '2025-12-01'::date,
                    end_date := '2025-12-06'::date,
                    segment_id := segment_id,
                    n_resamples := 300,
                    hr_starts := array[0,7,10,15,19]::smallint[],
                    hr_ends := array[7,10,15,19,24]::smallint[]
            )
) AS lat
*/
