--DROP FUNCTION miovision_api.update_open_issues;
CREATE OR REPLACE FUNCTION miovision_api.update_open_issues()
RETURNS void
LANGUAGE PLPGSQL

AS $BODY$

BEGIN

    WITH alerts AS (
        SELECT
            ar.uid,
            string_agg(DISTINCT alerts.alert, '; '::text) AS alerts
        FROM miovision_api.anomalous_ranges AS ar
        JOIN miovision_api.alerts
            ON alerts.intersection_uid = ar.intersection_uid
            AND tsrange(alerts.start_time, alerts.end_time)
            && tsrange(ar.range_start, ar.range_end)
        GROUP BY ar.uid
    ),

    open_issues AS (
        SELECT
            ar.uid,
            ar.intersection_uid,
            i.id AS intersection_id,
            i.api_name AS intersection_name,
            ar.classification_uid,
            CASE
                WHEN ar.classification_uid = 2 THEN 'Bicycle TMC'
                WHEN ar.classification_uid = 10 THEN 'Bicycle Approach'
                WHEN ar.classification_uid IS NULL THEN 'All modes'
                ELSE c.classification
            END,
            ar.leg,
            ar.range_start::date,
            (current_timestamp AT TIME ZONE 'EST5EDT')::date - ar.range_start::date AS num_days,
            ar.notes,
            SUM(v.volume) AS volume,
            alerts.alerts
        FROM miovision_api.anomalous_ranges AS ar
        --keep rows with null classification_uid
        LEFT JOIN miovision_api.classifications AS c USING (classification_uid)
        --omit null intersection_uids. These will go under discontinuities. 
        JOIN miovision_api.intersections AS i USING (intersection_uid)
        --find last week volume
        LEFT JOIN miovision_api.volumes AS v
            ON ar.intersection_uid = v.intersection_uid
            --volume within the last 7 days and after AR started
            AND v.datetime_bin >= ar.range_start
            --prune the partitions
            AND v.datetime_bin >= current_date - interval '7 days'
            AND (
                ar.classification_uid = v.classification_uid
                OR ar.classification_uid IS NULL
            )
            AND (
                ar.leg = v.leg
                OR ar.leg IS NULL
            )
        LEFT JOIN alerts ON alerts.uid = ar.uid
        WHERE
            ar.problem_level <> 'valid-caveat'
            --currently active
            AND (
                ar.range_end IS NULL
                OR (
                    ar.notes LIKE '%identified by a daily airflow process%'
                    AND ar.range_end = (current_timestamp AT TIME ZONE 'EST5EDT')::date --today
                )
            )
        GROUP BY
            ar.uid,
            ar.intersection_uid,
            i.id,
            i.api_name,
            ar.classification_uid,
            c.classification,
            ar.range_start,
            ar.notes,
            alerts.alerts
        ORDER BY
            ar.intersection_uid,
            ar.range_start,
            ar.classification_uid
    ),

    closed AS (
        DELETE FROM miovision_api.open_issues
        WHERE uid NOT IN (SELECT uid FROM open_issues)
    )

    MERGE INTO miovision_api.open_issues AS oir
    USING open_issues AS oi 
    ON oir.uid = oi.uid
    WHEN MATCHED THEN
    UPDATE SET
        intersection_uid = oi.intersection_uid,
        intersection_id = oi.intersection_id,
        intersection_name = oi.intersection_name,
        classification_uid = oi.classification_uid,
        classification = oi.classification,
        leg = oi.leg,
        range_start = oi.range_start,
        num_days = oi.num_days,
        notes = oi.notes,
        volume = oi.volume,
        alerts = oi.alerts
    WHEN NOT MATCHED THEN
        INSERT (
            uid, intersection_uid, intersection_id, intersection_name, classification_uid,
            classification, leg, range_start, num_days, notes, volume, alerts
        ) VALUES (
            oi.uid, oi.intersection_uid, oi.intersection_id, oi.intersection_name,
            oi.classification_uid, oi.classification, oi.leg, oi.range_start, oi.num_days, oi.notes,
            oi.volume, oi.alerts
        );

    EXECUTE FORMAT(
    'COMMENT ON TABLE miovision_api.open_issues IS %L', 
    'Last updated ' || to_char(now() AT TIME ZONE 'EST5EDT', 'yyyy-mm-dd HH24:MI')
    || ' using `SELECT miovision_api.update_open_issues();`.'
    || ' This is performed automatically once per day by `miovision_pull` DAG.'
    );
    
END
$BODY$;

COMMENT ON FUNCTION miovision_api.update_open_issues() IS
'A function to update miovision_api.open_issues. '
'Run daily via miovision_pull Airflow DAG.';

ALTER FUNCTION miovision_api.update_open_issues() OWNER TO miovision_admins;

GRANT EXECUTE ON FUNCTION miovision_api.update_open_issues() TO MIOVISION_API_BOT;
GRANT EXECUTE ON FUNCTION miovision_api.update_open_issues() TO MIOVISION_DATA_DETECTIVES;

