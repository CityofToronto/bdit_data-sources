-- FUNCTION: gwolofs.here_dynamic_bin_avg(date, date, time without time zone, time without time zone, integer[], text[], boolean)

-- DROP FUNCTION IF EXISTS gwolofs.here_dynamic_bin_avg(date, date, time without time zone, time without time zone, integer[], text[], boolean);

CREATE OR REPLACE FUNCTION gwolofs.here_dynamic_bin_avg(
	start_date date,
	end_date date,
	start_tod time without time zone,
	end_tod time without time zone,
	dow_list integer[],
	link_dirs text[],
	holidays boolean)
    RETURNS numeric
    LANGUAGE 'sql'
    COST 100
    VOLATILE PARALLEL UNSAFE
AS $BODY$

CALL gwolofs.cache_tt_segment(here_dynamic_bin_avg.link_dirs);

CALL gwolofs.cache_tt_results(
    start_date := here_dynamic_bin_avg.start_date,
    end_date := here_dynamic_bin_avg.end_date,
    start_tod := here_dynamic_bin_avg.start_tod,
    end_tod := here_dynamic_bin_avg.end_tod,
    dow_list := here_dynamic_bin_avg.dow_list,
    link_dirs := here_dynamic_bin_avg.link_dirs,
    holidays := here_dynamic_bin_avg.holidays
);

WITH time_grps AS (
    SELECT tsrange(
        (days.dt + here_dynamic_bin_avg.start_tod)::timestamp,
        (days.dt + here_dynamic_bin_avg.end_tod)::timestamp, '[)') AS time_grp
    FROM generate_series(
        here_dynamic_bin_avg.start_date::date,
        here_dynamic_bin_avg.end_date::date - '1 day'::interval, '1 day'::interval) AS days(dt)
    WHERE date_part('isodow', dt) = ANY(here_dynamic_bin_avg.dow_list)
)

SELECT AVG(tt)
FROM gwolofs.dynamic_binning_results AS res
JOIN time_grps USING (time_grp)
JOIN gwolofs.tt_segments AS segs ON res.segment_uid = segs.uid
WHERE segs.link_dirs = here_dynamic_bin_avg.link_dirs

$BODY$;

ALTER FUNCTION gwolofs.here_dynamic_bin_avg(
    date, date, time without time zone, time without time zone, integer[], text[], boolean
)
OWNER TO gwolofs;


/*example of use: 

SELECT
    start_date,
    end_date,
    start_tod,
    end_tod,
    dow_list,
    link_dirs,
    gwolofs.here_dynamic_bin_avg(
        start_date := l.start_date,
        end_date := l.end_date,
        start_tod := l.start_tod,
        end_tod := l.end_tod,
        dow_list := l.dow_list,
        link_dirs := l.link_dirs,
        holidays := TRUE
    )
FROM
(VALUES
('2025-01-02'::date, '2025-01-10'::date, '07:00'::time, '10:00'::time, '{1,2,3,4,5}'::int[], '{1258924853F,1258924867F,1258924868F,1258924894F}'::text[]),
('2025-01-02'::date, '2025-01-10'::date, '11:00'::time, '15:00'::time, '{1,2,3,4,5}'::int[], '{1258924852F,1258924867F,1258924868F,1258924894F}'::text[]),
('2025-01-02'::date, '2025-01-10'::date, '07:00'::time, '10:00'::time, '{1,3,5}'::int[], '{1258924852F,1258924853F,1258924868F,1258924894F}'::text[]),
('2024-01-02'::date, '2025-01-10'::date, '07:00'::time, '10:00'::time, '{1,2,3,4,5}'::int[], '{1258924852F,1258924853F,1258924867F,1258924894F}'::text[])
) AS l(start_date, end_date, start_tod, end_tod, dow_list, link_dirs);

*/