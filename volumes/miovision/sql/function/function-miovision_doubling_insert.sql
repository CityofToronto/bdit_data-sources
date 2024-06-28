CREATE OR REPLACE FUNCTION gwolofs.miovision_doubling_insert(end_date date)
RETURNS VOID
AS $$

    INSERT INTO gwolofs.miovision_doublings (
        intersection_uid, classification_uid, dt, this_week_volume,
        previous_week_volume, this_week_days, previous_week_days
    )
    SELECT
        vd.intersection_uid,
        vd.classification_uid,
        end_date AS dt,
        SUM(vd.daily_volume) FILTER (WHERE w.this_week) AS this_week_volume,
        SUM(vd.daily_volume) FILTER (WHERE NOT w.this_week) AS previous_week_volume,
        COUNT(DISTINCT vd.dt::date) FILTER (WHERE w.this_week) AS this_week_days,
        COUNT(DISTINCT vd.dt::date) FILTER (WHERE NOT w.this_week) AS previous_week_days
    FROM miovision_api.volumes_daily_unfiltered AS vd,
    LATERAL(
        SELECT dt >= end_date + interval '1 day' - interval '7 days' AS this_week
    ) AS w
    WHERE
        vd.classification_uid IN (1,2,6,10)
        AND vd.dt >= end_date + interval '1 day' - interval '14 days'
        AND vd.dt < end_date + interval '1 day'
    GROUP BY
        vd.intersection_uid,
        vd.classification_uid
    HAVING
        --doubling of volume week over week
        SUM(vd.daily_volume) FILTER (WHERE w.this_week)
        >= 2 * SUM(vd.daily_volume) FILTER (WHERE NOT w.this_week)
        --last week (denominator) has all 7 days of data
        --this week having less than 7 days is OK (=worse)
        AND COUNT(DISTINCT vd.dt::date) FILTER (WHERE NOT w.this_week) = 7
        --last week volume greater than 1000
        AND SUM(vd.daily_volume) FILTER (WHERE NOT w.this_week) > 1000
    ON CONFLICT (intersection_uid, classification_uid, dt)
    DO UPDATE SET
        this_week_volume = EXCLUDED.this_week_volume,
        previous_week_volume = EXCLUDED.previous_week_volume,
        this_week_days = EXCLUDED.this_week_days,
        previous_week_days = EXCLUDED.previous_week_days;

$$
LANGUAGE SQL;

ALTER FUNCTION gwolofs.miovision_doubling_insert(date) OWNER TO miovision_admins;
GRANT EXECUTE ON FUNCTION gwolofs.miovision_doubling_insert(date) TO miovision_api_bot;

COMMENT ON FUNCTION gwolofs.miovision_doubling_insert(date)
IS '(in development) Function to identify when Miovision volumes have doubled week over week.'