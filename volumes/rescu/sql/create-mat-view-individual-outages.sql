DROP MATERIALIZED VIEW gwolofs.rescu_individual_outages;

CREATE MATERIALIZED VIEW gwolofs.rescu_individual_outages
AS

--make a table of outages by rescu detector.
    --Outages includes network wide outages. Couldn't resolve overlapping individual and network wide outages using this method (no longer cross joining timebins and detectors).
    --could be possible using future range operator: anymultirange - anymultirange → anymultirange ("Computes the difference of the multiranges.")
--runs in 2 minutes for all of rescu.volumes15min
--doesn't include outages that end after last data point (by detector)

WITH non_zero_bins AS (
    SELECT
        detector_id,
        datetime_bin,
        volume_15min
    FROM rescu.volumes_15min
    WHERE COALESCE(volume_15min, 0) > 0
),

bin_gaps AS (
    SELECT
        detector_id,
        datetime_bin,
        volume_15min,
        LAG(
            datetime_bin, 1
        ) OVER (
            PARTITION BY detector_id ORDER BY datetime_bin
        ) + interval '15 MINUTES' AS gap_start,
        datetime_bin - interval '15 MINUTES' AS gap_end,
        CASE
            WHEN
                datetime_bin - LAG(
                    datetime_bin, 1
                ) OVER (PARTITION BY detector_id ORDER BY datetime_bin) = '00:15:00' THEN 0
            ELSE 1
        END AS bin_break, --identify non-consecutive bins
        datetime_bin - LAG(
            datetime_bin, 1
        ) OVER (PARTITION BY detector_id ORDER BY datetime_bin) AS bin_gap --duration of gap
    FROM non_zero_bins
)

SELECT
    bt.detector_id,
    bt.gap_start AS time_start,
    bt.gap_end AS time_end,
    (bt.gap_start)::date AS date_start,
    (bt.gap_end)::date AS date_end,
    tsrange(bt.gap_start, bt.gap_end, '[]') AS time_range,
    daterange((bt.gap_start)::date, (bt.gap_end)::date, '[]') AS date_range,
    EXTRACT(
        EPOCH FROM (bt.gap_end + interval '15 MINUTES') - bt.gap_start
    ) / 86400 AS duration_days
FROM bin_gaps AS bt
WHERE
    bt.bin_break = 1
    AND bt.bin_gap IS NOT NULL
    --Start and end are inclusive so interval 15 minutes implies a gap of 2 bins or 30 minutes
    AND bt.gap_end - bt.gap_start >= interval '15 MINUTES'
ORDER BY
    bt.detector_id,
    bt.gap_start
