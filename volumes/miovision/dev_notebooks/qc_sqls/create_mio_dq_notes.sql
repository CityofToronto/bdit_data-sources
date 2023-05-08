-- make a table that contains ranges of no vs low car volume so that the latter classification can be investigated
CREATE TABLE scannon.mio_dq_notes AS (

    -- grab weeks with weekly volumes sums that = 0
    WITH zero_weeks AS (
        SELECT 
            v.intersection_uid, 
            i.intersection_name, 
            date_trunc('week', v.datetime_bin)::date AS mon, 
            SUM(v.volume) AS car_ct
        FROM miovision_api.volumes_15min AS v
        LEFT JOIN miovision_api.intersections AS i USING (intersection_uid)
        WHERE 
            v.classification_uid = 1
        GROUP BY 
            v.intersection_uid, 
            i.intersection_name, 
            date_trunc('week', v.datetime_bin)::date
        HAVING SUM(v.volume) = 0
    ),

    -- make a categorical field for reasons of identifying no cars vs low cars purposes
    car_count AS (
        SELECT 
            bw.intersection_uid, 
            bw.intersection_name,
            bw.bad_week,
            z.car_ct,
            CASE
                WHEN z.car_ct = 0 THEN 'car count = 0'
                ELSE 'low car count - investigate'
            END AS notes
        FROM scannon.miovision_bad_weeks AS bw
        LEFT JOIN zero_weeks AS z
            ON
                bw.bad_week = z.mon
                AND bw.intersection_uid = z.intersection_uid
    ),

    -- find consecutive weeks with the same low + no car classifications for the same intersection_uid
    cnsc_weeks AS (
        SELECT 
            cc.intersection_uid,
            cc.intersection_name,
            cc.bad_week,
            cc.notes,
            ROW_NUMBER() OVER (ORDER BY cc.intersection_uid, cc.notes, cc.bad_week) AS row_num,
            CASE
                WHEN (cc.bad_week - lag(cc.bad_week, 1) OVER (PARTITION BY cc.intersection_uid ORDER BY cc.intersection_uid, cc.bad_week)) = 7
                    AND cc.intersection_uid = lag(cc.intersection_uid) OVER (PARTITION BY cc.intersection_uid ORDER BY cc.intersection_uid, cc.bad_week)
                    AND cc.notes = lag(cc.notes) OVER (PARTITION BY cc.intersection_uid ORDER BY cc.intersection_uid, cc.bad_week)
                    THEN 0
                ELSE 1
            END AS cnsc_wk
        FROM car_count AS cc
    ),

    -- some intersection_uids have two non-continuous periods that suck - group the consecutive weeks
    weekly_groups AS (
        SELECT 
            cw.row_num,
            cw.intersection_uid,
            cw.intersection_name,
            cw.bad_week,
            cw.notes,
            cw.cnsc_wk,
            SUM(
                CASE
                    WHEN cw.cnsc_wk = 1 THEN 1
                    ELSE 0
                END) OVER (ORDER BY cw.row_num) AS week_group
        FROM cnsc_weeks AS cw
    )

    -- final table showing ranges of weeks with bad data by intersection_uid
    SELECT
        wg.intersection_uid,
        wg.intersection_name,
        wg.notes,
        MIN(wg.bad_week)::timestamp AS excl_start,
        (MAX(wg.bad_week) + interval '7 days')::timestamp AS excl_end, -- since the ranges are by week I need to add 7 days to represent all the days in that week
        tsrange(MIN(wg.bad_week), (MAX(wg.bad_week) + interval '7 days')::date, '[)') AS excl_range
    FROM weekly_groups AS wg
    GROUP BY
        wg.intersection_uid,
        wg.intersection_name,
        wg.week_group,
        wg.notes
);