WITH ongoing_outages AS (
    SELECT
        i.intersection_name || ' (uid: ' || i.intersection_uid || ') - data last received: '
            || MAX(v.datetime_bin::date) || ' (' || '{{ data_interval_end }} 00:00:00'::timestamp - MAX(v.datetime_bin::date) || ')' AS description
    FROM miovision_api.volumes AS v
    JOIN miovision_api.intersections AS i USING (intersection_uid)
    WHERE
        v.datetime_bin >= '{{ data_interval_end }} 00:00:00'::timestamp - interval '{{ params.lookback }}'
        AND v.datetime_bin < '{{ data_interval_end }} 00:00:00'::timestamp + interval '1 day'
    GROUP BY
        i.intersection_uid,
        i.intersection_name
    HAVING MAX(v.datetime_bin::date) < '{{ data_interval_end }} 00:00:00'::timestamp - interval '{{ params.min_duration }}'
)

SELECT
    COUNT(ongoing_outages.*) < 1 AS check,
    CASE WHEN COUNT(ongoing_outages.*) = 1 THEN 'There is ' ELSE 'There are ' END ||
        COALESCE(COUNT(ongoing_outages.*), 0) ||
        CASE WHEN COUNT(ongoing_outages.*) = 1 THEN ' ongoing outage.' ELSE ' ongoing outages.'
    END AS summ, --gap_threshold
    array_agg(ongoing_outages.description || chr(10)) AS gaps
FROM ongoing_outages