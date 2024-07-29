CREATE OR REPLACE VIEW vds.individual_outages
AS

-- make a table of outages by rescu detector.
-- Outages includes network wide outages. Couldn't resolve overlapping individual and network
-- wide outages using this method (no longer cross joining timebins and detectors).
-- could be possible using future range operator: anymultirange - anymultirange → anymultirange
-- runs in 2 minutes for all of rescu.volumes15min
-- doesn't include outages that end after last data point (by detector)

WITH bin_gaps AS (
    SELECT
        vdsconfig_uid,
        entity_location_uid,
        division_id,
        datetime_15min,
        count_15min,
        LAG(datetime_15min, 1) OVER sensor + interval '15 minutes' AS gap_start,
        datetime_15min - interval '15 minutes' AS gap_end,
        CASE datetime_15min - LAG(datetime_15min, 1) OVER sensor
            WHEN '00:15:00' THEN 0
            ELSE 1
        END AS bin_break, --identify non-consecutive bins
        datetime_15min - LAG(datetime_15min, 1) OVER sensor AS bin_gap --duration of gap
    FROM vds.counts_15min_div2
    WHERE COALESCE(count_15min, 0) > 0
    WINDOW sensor AS (
        PARTITION BY vdsconfig_uid, entity_location_uid, division_id ORDER BY datetime_15min
    )
)

SELECT
    bt.vdsconfig_uid,
    bt.entity_location_uid,
    bt.division_id,
    bt.gap_start AS time_start,
    bt.gap_end AS time_end,
    (bt.gap_start)::date AS date_start,
    (bt.gap_end)::date AS date_end,
    tsrange(bt.gap_start, bt.gap_end, '[]') AS time_range,
    daterange((bt.gap_start)::date, (bt.gap_end)::date, '[]') AS date_range,
    EXTRACT(
        EPOCH FROM (bt.gap_end + interval '15 minutes') - bt.gap_start
    ) / 86400 AS duration_days
FROM bin_gaps AS bt
WHERE
    bt.bin_break = 1
    AND bt.bin_gap IS NOT NULL
    --Start and end are inclusive so interval 15 minutes implies a gap of 2 bins or 30 minutes
    AND bt.gap_end - bt.gap_start >= interval '15 minutes'
ORDER BY
    bt.vdsconfig_uid,
    bt.entity_location_uid,
    bt.division_id,
    bt.gap_start;

GRANT SELECT ON TABLE vds.individual_outages TO bdit_humans;

COMMENT ON VIEW vds.individual_outages IS ''
'A view which identifies individual sensor outages in the VDS network. '
'Runs in around 1:30 for entire dataset or include a WHERE filter on vdsconfig_uid for speed.';