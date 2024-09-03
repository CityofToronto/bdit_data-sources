WITH ongoing_outages AS (
    SELECT
        i.intersection_name || ' (id: `' || i.id || '`) - data last received: `'
        || MAX(v.dt::date) || '` (' || '{{ macros.ds_add(ds, 6) }}'::date --noqa: TMP, LT05
        - MAX(v.dt::date) || ' days)' AS descrip
    FROM miovision_api.volumes_daily_unfiltered AS v
    JOIN miovision_api.intersections AS i USING (intersection_uid)
    WHERE i.date_decommissioned IS NULL
    GROUP BY
        i.intersection_uid,
        i.intersection_name
    HAVING
        MAX(v.dt)
        < '{{ macros.ds_add(ds, 6) }}'::date --noqa: TMP
        - interval '{{ params.min_duration }}' --noqa: TMP
)

SELECT
    COUNT(ongoing_outages.*) < 1 AS _check,
    CASE WHEN COUNT(ongoing_outages.*) = 1 THEN 'There is ' ELSE 'There are ' END
    || COALESCE(COUNT(ongoing_outages.*), 0)
    || CASE WHEN COUNT(ongoing_outages.*) = 1 THEN ' ongoing outage.' ELSE ' ongoing outages.'
    END AS summ, --gap_threshold
    array_agg(ongoing_outages.descrip) AS gaps
FROM ongoing_outages
