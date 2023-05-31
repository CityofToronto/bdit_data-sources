DROP MATERIALIZED VIEW gwolofs.rescu_individual_outages;

CREATE MATERIALIZED VIEW gwolofs.rescu_individual_outages
AS

--make a table of outages by rescu detector.
    --excludes network wide outages. 
--runs in 2 minutes for all of rescu.volumes15min
--doesn't include outages that end after last data point (by detector)

WITH bin_gaps AS (
    SELECT
        detector_id,
        datetime_bin,
        volume_15min,
        LAG(
            datetime_bin, 1
        ) OVER (
            PARTITION BY detector_id ORDER BY datetime_bin
        ) + INTERVAL '15 minutes' AS gap_start,
        datetime_bin - INTERVAL '15 minutes' AS gap_end,
        CASE
            WHEN
                datetime_bin - LAG(
                    datetime_bin, 1
                ) OVER (PARTITION BY detector_id ORDER BY datetime_bin) = '00:15:00' THEN 0
            ELSE 1
        END AS bin_break,
        datetime_bin - LAG(
            datetime_bin, 1
        ) OVER (PARTITION BY detector_id ORDER BY datetime_bin) AS bin_gap
    FROM (
        SELECT
            detector_id,
            datetime_bin,
            volume_15min
        FROM rescu.volumes_15min
        WHERE COALESCE(volume_15min, 0) > 0
    ) AS v15_remove_zeros
--WHERE datetime_bin >= '2023-05-01'
),

--individual outages joined with network wide outages. 
joined_outages AS (
    SELECT
        bt.detector_id,
        bt.gap_start AS time_start,
        bt.gap_end AS time_end,
        ro.time_start AS nwout_start,
        ro.time_end AS nwout_end,
        tsrange(bt.gap_start, bt.gap_end, '[]') AS time_range,
        (bt.gap_start)::DATE AS date_start,
        (bt.gap_end)::DATE AS date_end,
        daterange((bt.gap_start)::DATE, (bt.gap_end)::DATE, '[]') AS date_range,
        EXTRACT(
            epoch FROM (bt.gap_end + INTERVAL '15 MINUTES') - bt.gap_start
        ) / 86400 AS duration_days
    FROM bin_gaps AS bt
    LEFT JOIN gwolofs.network_outages AS ro ON
        --left join to network wide outages, include WHERE ro.time_range is null
        tsrange(bt.gap_start, bt.gap_end, '[]') @> ro.time_range
    WHERE
        bt.bin_break = 1
        AND bt.bin_gap IS NOT NULL
        --Start and end are inclusive so interval 15 minutes implies a gap of 2 bins or 30 minutes.  
        AND bt.gap_end - bt.gap_start >= INTERVAL '15 MINUTES'
    ORDER BY bt.gap_start
)

--detector outages that did not overlap with network wide outages 
SELECT
    detector_id,
    time_start,
    time_end,
    time_range,
    date_start,
    date_end,
    date_range,
    duration_days
--null::tsrange as nwout_range
FROM joined_outages
WHERE nwout_start IS NULL

UNION

--portion of detector outages intersecting with network wide outages that occured before nwout
SELECT
    detector_id,
    time_start,
    nwout_start - INTERVAL '15 MINUTES' AS time_end,
    tsrange(time_start, nwout_start - INTERVAL '15 MINUTES', '[]') AS time_range,
    date_start,
    (nwout_start - INTERVAL '15 MINUTES')::DATE AS date_end,
    daterange(date_start, (nwout_start - INTERVAL '15 MINUTES')::DATE, '[]') AS date_range,
    EXTRACT(epoch FROM nwout_start - time_start) / 86400 AS duration_days
--tsrange(nwout_start, nwout_end, '[]') as nwout_range --for testing
FROM joined_outages
WHERE time_start < nwout_start - INTERVAL '15 MINUTES' --more than one time bin in excess of nwout

UNION

--portion of detector outages intersecting with network wide outages that occured after nwout
SELECT
    detector_id,
    nwout_end + INTERVAL '15 MINUTES' AS time_start,
    time_end,
    tsrange(nwout_end + INTERVAL '15 MINUTES', time_end, '[]') AS time_range,
    (nwout_end + INTERVAL '15 MINUTES')::DATE AS date_start,
    date_end,
    daterange((nwout_end + INTERVAL '15 MINUTES')::DATE, date_end, '[]') AS date_range,
    EXTRACT(epoch FROM time_end - nwout_end) / 86400 AS duration_days
--tsrange(nwout_start, nwout_end, '[]') as nwout_range --for testing 
FROM joined_outages
WHERE time_end > nwout_end + INTERVAL '15 MINUTES' --more than one time bin in excess of nwout
ORDER BY
    detector_id,
    time_start