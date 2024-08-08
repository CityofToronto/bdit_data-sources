--network outages using partition, lag (sare's method).
--much faster, runs in 1s for all of vds. 
CREATE OR REPLACE VIEW vds.network_outages AS

WITH rescu_summary AS (
    SELECT
        datetime_15min AS datetime_bin,
        SUM(count_15min) AS volume
    FROM vds.counts_15min_div2
    GROUP BY datetime_15min
    HAVING COALESCE(SUM(count_15min), 0) > 0

    UNION

    SELECT --quick way to include ongoing outage
        now()::date AS datetime_bin,
        1 AS volume
    ORDER BY datetime_bin
),

bin_gaps AS (
    SELECT
        datetime_bin,
        volume,
        datetime_bin - interval '15 minutes' AS gap_end,
        CASE datetime_bin - LAG(datetime_bin, 1) OVER (ORDER BY datetime_bin)
            WHEN '00:15:00' THEN 0
            ELSE 1
        END AS bin_break,
        datetime_bin - LAG(datetime_bin, 1) OVER (ORDER BY datetime_bin) AS bin_gap,
        LAG(datetime_bin, 1) OVER (ORDER BY datetime_bin) + interval '15 minutes' AS gap_start
    FROM rescu_summary
)

-- calculate the start and end times of gaps that are longer than 15 minutes
SELECT
    gap_start AS time_start,
    gap_end AS time_end,
    gap_start::date AS date_start,
    gap_end::date AS date_end,
    tsrange(gap_start, gap_end, '[]') AS time_range,
    daterange(gap_start::date, gap_end::date, '[]') AS date_range,
    --no minimum duration for a network wide outage
    EXTRACT(EPOCH FROM gap_end - gap_start) / 86400 AS duration_days
FROM bin_gaps
WHERE
    bin_break = 1
    AND bin_gap IS NOT NULL
ORDER BY gap_start;

ALTER TABLE vds.network_outages OWNER TO vds_admins;

GRANT SELECT ON TABLE vds.network_outages TO bdit_humans;

COMMENT ON VIEW vds.network_outages
IS 'A view which identifies network wide outages in the VDS network.';