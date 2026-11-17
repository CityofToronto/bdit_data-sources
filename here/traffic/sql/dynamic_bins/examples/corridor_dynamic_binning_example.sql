--identify an example with a 30 minute interval for testing:
SELECT * FROM here_agg.raw_corridors
WHERE upper(bin_range) = lower(bin_range) + '30 minutes'::interval
LIMIT 1;

--"corridor_id"	"time_grp"	"bin_range"	"tt"	"num_obs"	"uri_string"	"dt"	"hr"
--1	"empty"	"[""2026-02-15 05:05:00"",""2026-02-15 05:35:00"")"	85.730286	442		"2026-02-15"	5

--get the data to manually verify.
WITH links AS MATERIALIZED (
    SELECT
        link_dir,
        lat.len,
        lat.len / SUM(lat.len) OVER () AS proportion_of_length
    --edit the map version here if neccessary based on the corridor selected
    FROM here.routing_streets_25_1 AS streets,
    LATERAL (
        SELECT st_length(st_transform(streets.geom, 2952)) AS len
    ) AS lat
    WHERE
        link_dir = ANY(
            SELECT UNNEST(link_dirs)
            FROM here_agg.corridors WHERE corridor_id = 1
        )
)

SELECT
    ta.tx,
    SUM(l.proportion_of_length) AS proportion_of_length,
    SUM(l.len / ta.harmonic_mean * 3.6) / SUM(l.proportion_of_length) AS adjusted_tt,
    'link_dir, proportion_of_length, tt' || chr(10) ||
    string_agg(
        ta.link_dir || ',' ||
        l.proportion_of_length || ',' ||
        l.len / ta.harmonic_mean * 3.6, --tt
        chr(10)
    ) AS ta_data
FROM here.ta_path_hm AS ta
JOIN links AS l USING (link_dir)
WHERE
    tx >= '2026-02-15 05:00:00'
    AND tx < '2026-02-15 06:00:00'
GROUP BY tx
ORDER BY tx;

--you can work out the tt by finding the average travel time for each link from a range of bins, then SUM(link_avg_tt) / SUM(proportion_of_length) = TT.
--You should find that bins assembled with >=80% (unique) length match those below. 

WITH expected_results (dt, corridor_id, bin_range, tt) AS (
    VALUES
    ('2026-02-15'::date, 1::integer, '[''2026-02-15 05:00:00'',''2026-02-15 05:05:00'')'::tsrange, 98.784515::numeric),
    ('2026-02-15'::date, 1::integer, '[''2026-02-15 05:05:00'',''2026-02-15 05:35:00'')'::tsrange, 85.730286::numeric),
    ('2026-02-15'::date, 1::integer, '[''2026-02-15 05:35:00'',''2026-02-15 05:45:00'')'::tsrange, 109.09694::numeric),
    ('2026-02-15'::date, 1::integer, '[''2026-02-15 05:55:00'',''2026-02-15 06:00:00'')'::tsrange, 102.10823::numeric)
),

results AS (
    SELECT dt, corridor_id, bin_range, tt
    FROM here_agg.return_dynamic_bins(
        start_date := '2026-02-15'::date,
        end_date := '2026-02-16'::date,
        start_tod := '05:00:00'::time without time zone,
        end_tod := '06:00:00'::time without time zone,
        dow_list := '{1,2,3,4,5,6,7}'::integer [],
        node_start := (SELECT node_start FROM here_agg.corridors WHERE corridor_id = 1)::bigint,
        node_end := (SELECT node_end FROM here_agg.corridors WHERE corridor_id = 1)::bigint,
        holidays := True::boolean
    )
)

-- Rows in A but not in B, and vice versa
-- Should return no rows if perfect match.
(SELECT * FROM expected_results EXCEPT SELECT * FROM results)
UNION ALL
(SELECT * FROM results EXCEPT SELECT * FROM expected_results);


--travel time app gives result of 99 vs 98.92 for harmonic mean above. Check if this is just a mean vs harmonic mean discrepency.
--time_mean = 99 - (98.5, 85.5, 109.6, 102.2) https://trans-bdit.intra.prod-toronto.ca/tt-request-backend/aggregate-travel-times/30420278/30419014/5/6/2026-02-15/2026-02-16/true/1234567
--time_mean = 98.98 - below (98.49, 85.53, 109.65, 102.24)
--matches!

WITH links AS MATERIALIZED (
    SELECT
        link_dir,
        lat.len,
        lat.len / SUM(lat.len) OVER () AS proportion_of_length
    --edit the map version here if neccessary based on the corridor selected
    FROM here.routing_streets_24_4 AS streets,
    LATERAL (
        SELECT st_length(st_transform(streets.geom, 2952)) AS len
    ) AS lat
    WHERE
        link_dir = ANY(
            SELECT UNNEST(link_dirs)
            FROM here_agg.corridors WHERE corridor_id = 1
        )
)

SELECT
    ta.tx,
    SUM(l.proportion_of_length) AS proportion_of_length,
    SUM(l.len / ta.mean * 3.6) / SUM(l.proportion_of_length) AS adjusted_tt,
    'link_dir, proportion_of_length, tt' || chr(10) ||
    string_agg(
        ta.link_dir || ',' ||
        l.proportion_of_length || ',' ||
        l.len / ta.mean * 3.6, --tt
        chr(10)
    ) AS ta_data
FROM here.ta_path AS ta
JOIN links AS l USING (link_dir)
WHERE
    tx >= '2026-02-15 05:00:00'
    AND tx < '2026-02-15 06:00:00'
GROUP BY tx
ORDER BY tx;
