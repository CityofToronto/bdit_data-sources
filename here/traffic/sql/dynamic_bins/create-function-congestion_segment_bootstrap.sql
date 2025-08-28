/*
--test
SELECT tt_array[ceiling(random() * 3)]
FROM (VALUES(ARRAY[1,2,3])) AS val(tt_array)
CROSS JOIN generate_series(1,100,1)
*/


CREATE OR REPLACE FUNCTION gwolofs.congestion_segment_bootstrap(
	mnth date,
	segment_ids bigint[],
    n_resamples int)
    RETURNS TABLE(
        segment_id integer, mnth date, is_wkdy boolean, hr numeric, avg_tt real, n bigint, ci_lower real, ci_upper real
    ) 
    LANGUAGE SQL
    COST 100
    VOLATILE PARALLEL SAFE 
AS $BODY$

WITH raw_obs AS (
    SELECT
        segment_id,
        date_trunc('month', dt)::date AS mnth,
        EXTRACT('isodow' FROM dt) IN (1, 2, 3, 4, 5) AS is_wkdy,
        EXTRACT('hour' FROM hr) AS hr,
        ARRAY_AGG(tt::real) AS tt_array,
        AVG(tt::real) AS avg_tt,
        COUNT(*) AS n
    FROM gwolofs.congestion_raw_segments
    WHERE -- same params as the above aggregation
        dt >= congestion_segment_bootstrap.mnth
        AND dt < congestion_segment_bootstrap.mnth + interval '1 month'
        AND segment_id = ANY(congestion_segment_bootstrap.segment_ids)
    GROUP BY
        segment_id,
        mnth,
        is_wkdy,
        EXTRACT('hour' FROM hr)
),

random_selections AS (
    SELECT
        raw_obs.segment_id,
        raw_obs.mnth,
        raw_obs.is_wkdy,
        raw_obs.hr,
        sample_group.group_id,
        raw_obs.avg_tt,
        raw_obs.n,
        --get a random observation from the array of tts
        raw_obs.tt_array[ceiling(random() * raw_obs.n)] AS rnd_tt
    FROM raw_obs
    CROSS JOIN generate_series(1, n)
    -- 200 resamples (could be any number)
    CROSS JOIN generate_series(1, congestion_segment_bootstrap.n_resamples) AS sample_group(group_id)
),

resampled_averages AS (
    SELECT
        segment_id,
        mnth,
        is_wkdy,
        hr,
        group_id,
        AVG(rnd_tt) AS rnd_avg_tt
    FROM random_selections
    GROUP BY
        segment_id,
        mnth,
        is_wkdy,
        hr,
        group_id
)

SELECT
    ra.segment_id,
    ra.mnth,
    ra.is_wkdy,
    ra.hr,
    raw_obs.avg_tt::real,
    raw_obs.n,
    percentile_disc(0.025) WITHIN GROUP (ORDER BY ra.rnd_avg_tt)::real AS ci_lower,
    percentile_disc(0.975) WITHIN GROUP (ORDER BY ra.rnd_avg_tt)::real AS ci_upper
FROM resampled_averages AS ra
JOIN raw_obs USING (segment_id, mnth, is_wkdy, hr)
GROUP BY
    ra.segment_id,
    ra.mnth,
    ra.is_wkdy,
    ra.hr,
    raw_obs.avg_tt,
    raw_obs.n;

    $BODY$;

--6:52 for 100
/*example
SELECT congestion_segment_bootstrap.*
FROM gwolofs.congestion_segment_bootstrap(
    mnth := '2025-05-01'::date,
    segment_ids := (SELECT ARRAY(SELECT segment_id FROM generate_series(1,100) AS a(segment_id))),
    n_resamples := 300
)
*/

--6.5 hours estimate for all
--SELECT COUNT(DISTINCT segment_id) / 100.0 * 352 / 60 / 60 FROM congestion.network_segments_24_4 ORDER BY 1
