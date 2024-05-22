WITH ongoing_outages AS (
    SELECT
        i.intersection_name || ' (uid: ' || i.intersection_uid || ') - data last received: '
        || MAX(v.datetime_bin::date) || ' (' || '{{ ds_add(ds, 7) }}'::date --noqa: TMP, LT05
        - MAX(v.datetime_bin::date) || ' days)' AS descrip
    FROM miovision_api.volumes AS v
    JOIN miovision_api.intersections AS i USING (intersection_uid)
    WHERE
        v.datetime_bin >= '{{ ds_add(ds, 7) }}'::date - interval '{{ params.lookback }}' --noqa: TMP, LT05
        AND v.datetime_bin < '{{ ds_add(ds, 7) }}'::date + interval '1 day' --noqa: TMP, LT05
    GROUP BY
        i.intersection_uid,
        i.intersection_name
    HAVING
        MAX(v.datetime_bin::date)
        < '{{ ds_add(ds, 7) }}'::date - interval '{{ params.min_duration }}' --noqa: TMP
)

SELECT
    COUNT(ongoing_outages.*) < 1 AS _check,
    CASE WHEN COUNT(ongoing_outages.*) = 1 THEN 'There is ' ELSE 'There are ' END
    || COALESCE(COUNT(ongoing_outages.*), 0)
    || CASE WHEN COUNT(ongoing_outages.*) = 1 THEN ' ongoing outage.' ELSE ' ongoing outages.'
    END AS summ, --gap_threshold
    array_agg(ongoing_outages.descrip || chr(10)) AS gaps
FROM ongoing_outages