CREATE OR REPLACE FUNCTION miovision_api.insert_volumes_daily(
    start_date date,
    end_date date
)
RETURNS void
LANGUAGE 'plpgsql'

COST 100
VOLATILE 
AS $BODY$
BEGIN

    DELETE FROM miovision_api.volumes_daily
    WHERE 
        dt >= start_date
        AND dt < end_date;

    --identify duration of unacceptable gaps per day, intersection
    WITH unacceptable_gaps_summarized AS (
        SELECT
            un.intersection_uid,
            avg_vols.classification_uid,
            un.datetime_bin::date AS gap_date,
            SUM(un.gap_minutes_15min) AS unacceptable_gap_minutes,
            SUM(un.gap_minutes_15min * avg_vols.avg_hour_vol)/60 AS avg_historical_gap_vol
        FROM miovision_api.unacceptable_gaps AS un
        --find average historical volume by classification during those gaps
        LEFT JOIN miovision_api.gapsize_lookup AS avg_vols ON
            avg_vols.intersection_uid = un.intersection_uid
            AND avg_vols.hour_bin = date_part('hour', un.datetime_bin)
            AND avg_vols.dt = un.dt
            AND avg_vols.classification_uid IS NOT NULL
        WHERE
            un.datetime_bin::date >= start_date
            AND un.datetime_bin::date < end_date
        GROUP BY
            un.intersection_uid,
            un.datetime_bin::date,
            avg_vols.classification_uid
    )

    INSERT INTO miovision_api.volumes_daily (
        intersection_uid, dt, classification_uid, daily_volume, isodow,
        holiday, unacceptable_gap_minutes, datetime_bins_missing, avg_historical_gap_vol
    )
    SELECT
        v.intersection_uid,
        v.datetime_bin::date AS dt,
        v.classification_uid,
        SUM(v.volume) AS daily_volume,
        date_part('isodow', v.datetime_bin::date) AS isodow,
        hol.holiday IS NOT NULL AS holiday,
        COALESCE(un.unacceptable_gap_minutes, 0) AS unacceptable_gap_minutes,
        60 * 24 - COUNT(DISTINCT datetime_bin) AS datetime_bins_missing,
        un.avg_historical_gap_vol::int
    --raw data
    FROM miovision_api.volumes AS v
    LEFT JOIN ref.holiday AS hol ON hol.dt = v.datetime_bin::date
    --identify duration of unacceptable gaps (extended zero periods)
    --and avg historical volumes by classification for those gaps
    LEFT JOIN unacceptable_gaps_summarized AS un ON
        un.intersection_uid = v.intersection_uid
        AND v.datetime_bin::date = un.gap_date
        AND un.classification_uid = v.classification_uid
    --anti join anomalous_ranges table
    LEFT JOIN miovision_api.anomalous_ranges AS ar ON
        (
            ar.intersection_uid = v.intersection_uid
            OR ar.intersection_uid IS NULL
        ) AND (
            ar.classification_uid = v.classification_uid
            OR ar.classification_uid IS NULL
        )
        AND v.datetime_bin >= LOWER(ar.time_range)
        AND v.datetime_bin < UPPER(ar.time_range)
        AND ar.problem_level IN ('do-not-use', 'questionable')
    WHERE
        ar.time_range IS NULL --anti join anomalous ranges
        AND v.datetime_bin >= start_date
        AND v.datetime_bin < end_date
    GROUP BY
        v.intersection_uid,
        v.classification_uid,
        v.datetime_bin::date,
        hol.holiday,
        un.unacceptable_gap_minutes,
        un.avg_historical_gap_vol
    HAVING SUM(v.volume) > 0
    ORDER BY
        v.datetime_bin::date,
        v.intersection_uid,
        v.classification_uid;

END;
$BODY$;

COMMENT ON FUNCTION miovision_api.insert_volumes_daily IS
'Function for inserting daily volumes into miovision_api.volumes_daily';

ALTER FUNCTION miovision_api.insert_volumes_daily OWNER TO miovision_admins;
GRANT EXECUTE ON FUNCTION miovision_api.insert_volumes_daily TO miovision_api_bot;