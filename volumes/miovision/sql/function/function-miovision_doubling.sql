CREATE OR REPLACE FUNCTION gwolofs.miovision_doubling_summary(end_date date)
RETURNS TABLE (
    _check boolean,
    summ text,
    summary text []
) AS
$$

    WITH doublings AS (
        SELECT
            dt,
            intersection_uid,
            classification_uid,
            this_week_volume,
            previous_week_volume,
            CASE WHEN dt = 1 + lag(dt, 1) OVER (
                PARTITION BY intersection_uid, classification_uid ORDER BY dt
            ) THEN 0 ELSE 1 END AS run_bool
        FROM gwolofs.miovision_doublings
    ),

    runs AS (
        SELECT
            intersection_uid,
            classification_uid,
            dt,
            this_week_volume,
            previous_week_volume,
            SUM(run_bool) OVER (
                PARTITION BY intersection_uid, classification_uid ORDER BY dt
            ) AS run_id
        FROM doublings
        ORDER BY intersection_uid, classification_uid, dt
    ),

    runs_ranked AS (
        SELECT
            intersection_uid,
            classification_uid,
            dt,
            this_week_volume,
            previous_week_volume,
            rank() OVER (
                PARTITION BY intersection_uid, classification_uid, run_id ORDER BY dt
            ) AS run_rank
        FROM runs
    ),

    warnings AS (
        SELECT
            i.intersection_name || ' (' || rr.intersection_uid || ')' AS intersection,
            CASE
                WHEN rr.classification_uid = 2 THEN 'Bicycle TMC'
                WHEN rr.classification_uid = 10 THEN 'Bicycle Approach'
                ELSE c.classification
            END AS classification,
            dt,
            this_week_volume,
            previous_week_volume
        FROM runs_ranked AS rr
        JOIN miovision_api.intersections AS i USING (intersection_uid)
        JOIN miovision_api.classifications AS c USING (classification_uid)
        WHERE
            --notify on every 7th day
            mod(run_rank-1, 7) = 0
            --alerts in last 7 days
            AND dt >= end_date - interval '7 days'

    )

    SELECT
        NOT(COUNT(*) > 0) AS _check,
        'Volumes have doubled week over week in the following ' || COUNT(*)
        || CASE WHEN COUNT(*) = 1 THEN ' cases:' ELSE ' case:' END AS summ,
        array_agg(
            '`' || intersection || '`, `'
            || classification
            || '`, volume: `' || to_char(this_week_volume, 'FM9,999,999')
            || ' (last week: ' || to_char(previous_week_volume, 'FM9,999,999') || ')`'
        ) AS summary
    FROM warnings;

$$
LANGUAGE SQL;

ALTER FUNCTION gwolofs.miovision_doubling_summary(date) OWNER TO miovision_admins;
GRANT EXECUTE ON FUNCTION gwolofs.miovision_doubling_summary(date) TO miovision_api_bot;

COMMENT ON FUNCTION gwolofs.miovision_doubling_summary(date)
IS '(in development) Function to identify when Miovision volumes have doubled week over week.';