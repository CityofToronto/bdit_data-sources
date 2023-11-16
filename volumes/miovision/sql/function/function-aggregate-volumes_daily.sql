CREATE OR REPLACE FUNCTION miovision_api.aggregate_volumes_daily(
    start_date date,
    end_date date
)
RETURNS void
LANGUAGE 'plpgsql'

COST 100
VOLATILE
AS $BODY$

BEGIN
    --delete existing data for the date range
    DELETE FROM miovision_api.volumes_daily
    WHERE
        period_start >= start_date - interval '1 hour'
        AND period_end <= end_date - interval '1 hour';

    INSERT INTO miovision_api.volumes_daily (
        intersection_uid, dt, period_start, period_end, volume_1, volume_2, volume_3, volume_4, volume_5, volume_6, volume_7, volume_8, volume_9, volume_10, volume_total
    )
    SELECT
        i.intersection_uid,
        d.dt,
        d.dt - interval '1 hour' AS period_start,
        d.dt - interval '1 hour' + interval '1 day' AS period_end,
        SUM(v.volume) FILTER (WHERE v.classification_uid = 1) AS volume_1,
        SUM(v.volume) FILTER (WHERE v.classification_uid = 2) AS volume_2,
        SUM(v.volume) FILTER (WHERE v.classification_uid = 3) AS volume_3,
        SUM(v.volume) FILTER (WHERE v.classification_uid = 4) AS volume_4,
        SUM(v.volume) FILTER (WHERE v.classification_uid = 5) AS volume_5,
        SUM(v.volume) FILTER (WHERE v.classification_uid = 6) AS volume_6,
        SUM(v.volume) FILTER (WHERE v.classification_uid = 7) AS volume_7,
        SUM(v.volume) FILTER (WHERE v.classification_uid = 8) AS volume_8,
        SUM(v.volume) FILTER (WHERE v.classification_uid = 9) AS volume_9,
        SUM(v.volume) FILTER (WHERE v.classification_uid = 10) AS volume_10,
        SUM(v.volume) AS volume_total
    --add entries for intersections with no outages
    FROM miovision_api.intersections AS i
    LEFT JOIN miovision_api.volumes_15min_mvt AS v ON
        i.intersection_uid = v.intersection_uid
        AND v.datetime_bin > i.date_installed + INTERVAL '1 day'
        AND (
            i.date_decommissioned IS NULL
            OR datetime_bin < i.date_decommissioned - INTERVAL '1 day'
        ),
        LATERAL (
            --day beginning and ending at 11pm
            SELECT date_trunc('day', v.datetime_bin + interval '1 hour') AS dt
        ) d
    WHERE
        v.datetime_bin >= start_date - interval '1 hour'
        AND v.datetime_bin < end_date - interval '1 hour'
    GROUP BY
        i.intersection_uid,
        d.dt
    ORDER BY
        i.intersection_uid,
        d.dt; 

END;
$BODY$;

ALTER FUNCTION miovision_api.aggregate_volumes_daily(date, date) OWNER TO miovision_admins;
GRANT EXECUTE ON FUNCTION miovision_api.aggregate_volumes_daily(date, date) TO dbadmin WITH GRANT OPTION;
GRANT EXECUTE ON FUNCTION miovision_api.aggregate_volumes_daily(date, date) TO miovision_api_bot;