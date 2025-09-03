--DROP FUNCTION gwolofs.congestion_segment_bootstrap(date,bigint,integer);

CREATE OR REPLACE FUNCTION gwolofs.congestion_segment_bootstrap(
    mnth date,
    segment_id bigint,
    n_resamples int
    )
    RETURNS VOID
    LANGUAGE SQL
    COST 100
    VOLATILE PARALLEL SAFE 
AS $BODY$

    SELECT setseed(('0.'||replace(mnth::text, '-', ''))::numeric);
    
    WITH raw_obs AS (
        SELECT
            --segment_id and mnth don't need to be in group by until end
            EXTRACT('isodow' FROM dt) IN (1, 2, 3, 4, 5) AS is_wkdy,
            EXTRACT('hour' FROM hr) AS hr,
            ARRAY_AGG(tt::real) AS tt_array,
            AVG(tt::real) AS avg_tt,
            COUNT(*) AS n
        FROM gwolofs.congestion_raw_segments
        WHERE -- same params as the above aggregation
            dt >= congestion_segment_bootstrap.mnth
            AND dt < congestion_segment_bootstrap.mnth + interval '1 month'
            AND segment_id = congestion_segment_bootstrap.segment_id
        GROUP BY
            segment_id,
            is_wkdy,
            EXTRACT('hour' FROM hr)
    ),
    
    random_selections AS (
        SELECT
            raw_obs.is_wkdy,
            raw_obs.hr,
            raw_obs.avg_tt,
            raw_obs.n,
            sample_group.group_id,
            --get a random observation from the array of tts
            AVG(raw_obs.tt_array[ceiling(random() * raw_obs.n)]) AS rnd_avg_tt
        FROM raw_obs
        CROSS JOIN generate_series(1, n)
        -- 200 resamples (could be any number)
        CROSS JOIN generate_series(1, congestion_segment_bootstrap.n_resamples) AS sample_group(group_id)
        GROUP BY
            raw_obs.is_wkdy,
            raw_obs.hr,
            raw_obs.avg_tt,
            raw_obs.n,
            sample_group.group_id
    )
    
    INSERT INTO gwolofs.congestion_segments_monthly_bootstrap (
        segment_id, mnth, is_wkdy, hr, avg_tt, n, n_resamples, ci_lower, ci_upper
    )
    SELECT
        congestion_segment_bootstrap.segment_id,
        congestion_segment_bootstrap.mnth,
        is_wkdy,
        hr,
        avg_tt::real,
        n,
        n_resamples,
        percentile_disc(0.025) WITHIN GROUP (ORDER BY rnd_avg_tt)::real AS ci_lower,
        percentile_disc(0.975) WITHIN GROUP (ORDER BY rnd_avg_tt)::real AS ci_upper
    FROM random_selections
    GROUP BY
        is_wkdy,
        hr,
        avg_tt,
        n;

    $BODY$;

GRANT EXECUTE ON FUNCTION gwolofs.congestion_segment_bootstrap(date,bigint,integer) TO congestion_bot;

/*Usage example: (works best one segment at a time with Lateral)
SELECT *
FROM UNNEST('{1,2,3,4,5,6,7,8,9}'::bigint[]) AS unnested(segment_id)
LATERAL (
    SELECT gwolofs.congestion_segment_bootstrap(
                    mnth := '2025-06-01'::date,
                    segment_ids := segment_id,
                    n_resamples := 300)
)
*/