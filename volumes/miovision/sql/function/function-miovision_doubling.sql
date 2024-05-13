CREATE OR REPLACE FUNCTION gwolofs.miovision_doubling(end_date date)
RETURNS TABLE (
    _check boolean,
    summ text,
    summary text []
) AS
$$

    WITH doublings AS (
        SELECT
            i.intersection_name || ' (' || vd.intersection_uid || ')' AS intersection,
            CASE
                WHEN vd.classification_uid = 2 THEN 'Bicycle TMC'
                WHEN vd.classification_uid = 10 THEN 'Bicycle Approach'
                ELSE c.classification
            END,
            SUM(vd.daily_volume) FILTER (WHERE w.this_week) AS this_week_volume,
            SUM(vd.daily_volume) FILTER (WHERE NOT w.this_week) AS previous_week_volume,
            COUNT(DISTINCT vd.dt::date) FILTER (WHERE w.this_week) AS this_week_days,
            COUNT(DISTINCT vd.dt::date) FILTER (WHERE NOT w.this_week) AS previous_week_days
        FROM miovision_api.volumes_daily_unfiltered AS vd
        JOIN miovision_api.intersections AS i USING (intersection_uid)
        JOIN miovision_api.classifications AS c USING (classification_uid),
        LATERAL(
            SELECT dt >= end_date::date + interval '1 day' - interval '7 days' AS this_week
        ) AS w
        WHERE
            vd.classification_uid IN (1,2,6,10)
            AND vd.dt >= end_date::date + interval '1 day' - interval '14 days'
            AND vd.dt < end_date::date + interval '1 day'
        GROUP BY
            vd.intersection_uid,
            i.intersection_name,
            vd.classification_uid,
            c.classification
        HAVING
            --doubling of volume week over week
            SUM(vd.daily_volume) FILTER (WHERE w.this_week) >= 2 * SUM(vd.daily_volume) FILTER (WHERE NOT w.this_week)
            --last week (denominator) has all 7 days of data
            --this week having less than 7 days is OK (=worse)
            AND COUNT(DISTINCT vd.dt::date) FILTER (WHERE NOT w.this_week) = 7
    )
    
    SELECT
        NOT(COUNT(*) > 0) AS _check,
        'Volumes have doubled week over week in the following ' || COUNT(*)
        || CASE WHEN COUNT(*) = 1 THEN ' cases:' ELSE ' case:' END AS summ,
        array_agg(
            '`' || d.intersection || '`, `'
            || d.classification
            || '`, volume: `' || to_char(d.this_week_volume, 'FM9,999,999')
            || ' (last week: ' || to_char(d.previous_week_volume, 'FM9,999,999') || ')`'
        ) AS summary
    FROM doublings AS d;
$$
LANGUAGE SQL;