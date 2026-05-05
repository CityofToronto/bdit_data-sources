CREATE OR REPLACE FUNCTION here_agg.agg_overnight_tt(
    mnth date,
    segments bigint [] DEFAULT NULL -- NULL = "all segments"
)
RETURNS void
SECURITY DEFINER
LANGUAGE plpgsql
AS $BODY$

DECLARE
    map_version text := here_agg.select_map_version(
        start_date := agg_overnight_tt.mnth,
        end_date := agg_overnight_tt.mnth + 1,
        agg_type := 'path_hm'
    );

BEGIN
    EXECUTE format($sql$
    WITH all_tts AS (
        --No need to filter map versions here: if segment hasn't changed ids between versions, OK to use.
        SELECT
            cs.segment_id,
            cs.ver_id,
            rs.tt
        FROM congestion.congestion_segments AS cs
        LEFT JOIN here_agg.raw_segments AS rs ON
            cs.segment_id = rs.segment_id
            AND cs.ver_id = rs.ver_id
            AND rs.dt >= %1$L::date - interval '6 months'
            AND rs.dt < %1$L::date
            AND (
                (
                    rs.dt < '2024-01-01'::date
                    AND rs.hr BETWEEN 0 AND 3
                ) OR (
                    rs.dt >= '2024-01-01'::date
                    AND rs.hr BETWEEN 1 AND 4
                )
            )
        --for testing
        /*WHERE
            cs.segment_id IN (7808, 7809) --these are new in 25_1
            AND cs.segment_id = 2 --this one is in both 24_4, 25_1*/

        UNION ALL

        --tt from retired segments
        SELECT
            --columns for QC
            /*
            retired.old_segment_id,
            retired.old_ver,
            cs_old.highway,
            rs.tt AS retired_tt,
            cs_old.total_length / rs.tt AS retired_metres_per_second,
            cs_new.total_length,
            */
            unnested.new_segment_id,
            cs_new.ver_id,
            cs_new.total_length / cs_old.total_length * rs.tt AS tt --new tt, using old speed, new length
        FROM congestion.congestion_retired_segments AS retired
        JOIN congestion.congestion_segments AS cs_old
            ON retired.old_ver = cs_old.ver_id
            AND cs_old.segment_id = retired.old_segment_id
        JOIN here_agg.raw_segments AS rs ON
            rs.ver_id = cs_old.ver_id
            AND rs.segment_id = retired.old_segment_id
            AND rs.dt >= %1$L::date - interval '6 months'
            AND rs.dt < %1$L::date
            AND (
                (
                    rs.dt < '2024-01-01'::date
                    AND rs.hr BETWEEN 0 AND 3
                ) OR (
                    rs.dt >= '2024-01-01'::date
                    AND rs.hr BETWEEN 1 AND 4
                )
            ),
        LATERAL UNNEST(retired.new_segment_ids) AS unnested(new_segment_id)
        LEFT JOIN congestion.congestion_segments AS cs_new ON
            unnested.new_segment_id = cs_new.segment_id
            AND cs_new.ver_id = %2$L
        WHERE retired.new_ver = %2$L
            --for testing
            --AND cs_new.segment_id IN (7808, 7809)
            --AND cs_new.segment_id = 2
    )

    INSERT INTO here_agg.segment_overnight_tts (
        segment_id, ver_id, mnth, overnight_avg_tt, rolling_6month_quasi_obs
    )
    SELECT
        segment_id,
        MAX(ver_id) AS ver_id,
        %1$L::date AS mnth,
        AVG(tt) AS overnight_avg_tt,
        COUNT(*) AS rolling_6month_quasi_obs
    FROM all_tts
    GROUP BY segment_id, ver_id, mnth
    HAVING MAX(ver_id) = %2$L
    ON CONFLICT ON CONSTRAINT segment_overnight_tts_pkey
    DO UPDATE
    SET
        overnight_avg_tt = EXCLUDED.overnight_avg_tt,
        rolling_6month_quasi_obs = EXCLUDED.rolling_6month_quasi_obs;
    $sql$, agg_overnight_tt.mnth, map_version);
    
END;
$BODY$;

COMMENT ON FUNCTION here_agg.agg_overnight_tt
IS 'Aggregate a month worth of rolling 6 month overnight travel times.
Uses an ON CONFLICT DO UPDATE clause - can be re-run when input data changes.
Takes around 1-2 minutes per month.';

ALTER FUNCTION here_agg.agg_overnight_tt OWNER TO here_admins;

REVOKE EXECUTE ON FUNCTION here_agg.agg_overnight_tt FROM public;

GRANT EXECUTE ON FUNCTION here_agg.agg_overnight_tt TO congestion_bot;

--SELECT here_agg.agg_overnight_tt('2026-01-01')
