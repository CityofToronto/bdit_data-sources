--identify an example with a 30 minute interval for testing:
SELECT * FROM here_agg.raw_segments
WHERE upper(bin_range) = lower(bin_range) + '15 minutes'::interval
LIMIT 1;

--"segment_id"	"dt"	"bin_start"	"bin_range"	"tt"	"num_obs"	"hr"	"ver_id"
--2	"2023-12-13"	"2023-12-13 07:45:00"	"[""2023-12-13 07:45:00"",""2023-12-13 08:00:00"")"	99.71924	0.8446231	7	"24_4"

--get the data to manually verify.
WITH links AS MATERIALIZED (
    SELECT
        cl.link_dir,
        cl.length AS len,
        cl.length / SUM(cl.length) OVER () AS proportion_of_length
    --edit the map version here if neccessary based on the corridor selected
    FROM congestion.congestion_links AS cl
    WHERE ver_id = '24_4' AND segment_id = 2
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
    tx >= '2023-12-13 07:00:00'
    AND tx < '2023-12-13 08:00:00'
GROUP BY tx
ORDER BY tx;

--Delete and reaggregate:
DELETE FROM here_agg.raw_segments WHERE segment_id = 2 AND bin_start >= '2023-12-13 07:00' AND bin_start < '2023-12-13 08:00';
SELECT here_agg.network_segment_agg(
    start_date:='2023-12-13'::date,
    segment_ids:='{2}'::bigint[]
);

--you can work out the tt by finding the average travel time for each link from a range of bins, then SUM(link_avg_tt) / SUM(proportion_of_length) = TT.
--You should find that bins assembled with >=80% (unique) length match those below. 
WITH expected_results (dt, segment_id, bin_range, tt) AS (
    VALUES
    ('2023-12-13'::date, 2::integer, '[''2023-12-13 07:10:00'',''2023-12-13 07:15:00'')'::tsrange, 59.832609::real),
    ('2023-12-13'::date, 2::integer, '[''2023-12-13 07:15:00'',''2023-12-13 07:20:00'')'::tsrange, 43.280372::real),
    ('2023-12-13'::date, 2::integer, '[''2023-12-13 07:30:00'',''2023-12-13 07:35:00'')'::tsrange, 58.480389::real),
    ('2023-12-13'::date, 2::integer, '[''2023-12-13 07:45:00'',''2023-12-13 08:00:00'')'::tsrange, 99.719237::real)
),

results AS (
    SELECT dt, segment_id, bin_range, tt
    FROM here_agg.raw_segments
    WHERE
        segment_id = 2
        AND bin_start >= '2023-12-13 07:00'
        AND bin_start < '2023-12-13 08:00'
)

-- Rows in A but not in B, and vice versa
(SELECT * FROM expected_results EXCEPT SELECT * FROM results)
UNION ALL
(SELECT * FROM results EXCEPT SELECT * FROM expected_results);


--travel time app gives result of 58.7 vs 65.328 for harmonic mean above. Check if this is just a mean vs harmonic mean discrepency.
--time_mean = 58.7 - https://trans-bdit.intra.prod-toronto.ca/tt-request-backend/aggregate-travel-times/30334262/30334233/7/8/2023-12-13/2023-12-14/true/1234567
--time_mean = 58.73 - below (59.83260903, 43.28037237, 32.10521177, 99.7192378)
--matches!

--get the data to manually verify.
WITH links AS MATERIALIZED (
    SELECT
        cl.link_dir,
        cl.length AS len,
        cl.length / SUM(cl.length) OVER () AS proportion_of_length
    --edit the map version here if neccessary based on the corridor selected
    FROM congestion.congestion_links AS cl
    WHERE ver_id = '24_4' AND segment_id = 2
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
    tx >= '2023-12-13 07:00:00'
    AND tx < '2023-12-13 08:00:00'
GROUP BY tx
ORDER BY tx;
