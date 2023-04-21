DROP VIEW open_data.ksp_agg_travel_time;
CREATE OR REPLACE VIEW open_data.ksp_agg_travel_time AS
WITH valid_corridors AS (
    SELECT
        x.dt,
        x.day_type,
        x.category,
        x.period_name,
        x.period_id,
        x.tt,
        CASE
            WHEN t.excluded_datetime IS NULL THEN y.corridor_id
            ELSE t.corridor_id_new
        END AS corridor_id
    FROM (SELECT
        a.bt_id,
        b.dt,
        b.day_type,
        b.category,
        c.period_id,
        c.period_name,
        avg(a.tt) AS tt
    FROM king_pilot.tt_30min AS a
    JOIN king_pilot.date_lookup AS b USING (dt)
    JOIN king_pilot.periods AS c USING (day_type)
    WHERE a.time_bin <@ c.period_range
    GROUP BY a.bt_id, b.dt, b.day_type, b.category, c.period_id, c.period_name
    ORDER BY a.bt_id, b.dt, b.day_type, b.category, c.period_id) AS x
    JOIN king_pilot.bt_corridor_segments AS y USING (bt_id)
    LEFT JOIN
        king_pilot.bt_corridors_exceptions AS t
        ON t.corridor_id_old = y.corridor_id AND x.dt <@ t.excluded_datetime
    WHERE y.corridor_id <> ALL(ARRAY[7, 8, 9])
    ORDER BY x.period_id, x.day_type, x.dt
),

pilot AS (
    SELECT
        z.street_name AS street,
        z.direction,
        z.from_intersection,
        z.to_intersection,
        z.corridor_id,
        u.dt,
        u.day_type,
        u.category,
        u.period_name AS period,
        sum(u.tt) / 60.0 AS tt
    FROM valid_corridors AS u
    JOIN king_pilot.bt_corridors AS z USING (corridor_id)
    GROUP BY
        z.corridor_id,
        z.street_name,
        z.direction,
        u.dt,
        u.day_type,
        u.category,
        u.period_name,
        u.period_id,
        z.segments,
        z.from_intersection,
        z.to_intersection
    HAVING count(u . *) = z.segments
    ORDER BY z.corridor_id, z.direction, u.period_id, u.day_type, u.dt
),

baseline AS (
    SELECT
        z.street_name AS street,
        z.direction,
        z.from_intersection,
        z.to_intersection,
        x.day_type,
        z.corridor_id,
        x.period_name AS period,
        (
            (('('::text || to_char(lower(x.period_range)::interval, 'HH24:MM'::text)) || '-'::text)
            || to_char(upper(x.period_range)::interval, 'HH24:MM'::text)
        )
        || ')'::text AS period_range,
        sum(x.tt) / 60.0 AS tt
    FROM (SELECT
        a.bt_id,
        a.day_type,
        b.period_name,
        b.period_range,
        avg(a.tt) AS tt
    FROM king_pilot.baselines AS a
    JOIN king_pilot.periods AS b USING (day_type)
    JOIN king_pilot.bt_segments USING (bt_id)
    WHERE a.time_bin <@ b.period_range
    GROUP BY a.bt_id, a.day_type, b.period_name, b.period_range) AS x
    JOIN king_pilot.bt_corridor_segments USING (bt_id)
    JOIN king_pilot.bt_corridors AS z USING (corridor_id)
    WHERE z.corridor_id <> ALL(ARRAY[8, 9])
    GROUP BY
        z.corridor_id,
        z.corridor_name,
        x.day_type,
        x.period_name,
        x.period_range,
        z.segments,
        z.street_name,
        z.direction,
        z.from_intersection,
        z.to_intersection
    HAVING count(x . *) = z.segments
    ORDER BY z.corridor_id, x.day_type, x.period_range
)

SELECT
    to_char(
        date_trunc('month'::text, pilot.dt::timestamp with time zone), 'Mon ''YY'::text
    ) AS month,
    pilot.street,
    pilot.direction,
    baseline.from_intersection,
    baseline.to_intersection,
    pilot.day_type,
    (pilot.period || ' '::text) || baseline.period_range AS time_period,
    round(baseline.tt, 1) AS baseline_travel_time,
    round(avg(pilot.tt), 1) AS average_travel_time
FROM pilot
JOIN baseline USING (corridor_id, direction, day_type, period)
WHERE
    pilot.category = 'Pilot'::text
    AND pilot.dt
    < date_trunc('month'::text, 'now'::text::date::timestamp with time zone) - interval '1 month'
GROUP BY
    (to_char(date_trunc('month'::text, pilot.dt::timestamp with time zone), 'Mon ''YY'::text)),
    (date_trunc('month'::text, pilot.dt::timestamp with time zone)),
    pilot.street,
    baseline.from_intersection,
    baseline.to_intersection,
    pilot.direction,
    pilot.day_type,
    ((pilot.period || ' '::text) || baseline.period_range),
    baseline.tt
ORDER BY (date_trunc('month'::text, pilot.dt::timestamp with time zone));
GRANT SELECT ON TABLE open_data.ksp_agg_travel_time TO od_extract_svc;
GRANT SELECT ON TABLE open_data.ksp_agg_travel_time TO bdit_humans;