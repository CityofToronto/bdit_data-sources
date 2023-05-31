DROP TABLE gwolofs.rescu_individual_outages;

CREATE TABLE gwolofs.rescu_individual_outages(
    detector_id text,
    time_start TIMESTAMP,
    time_end TIMESTAMP,
    time_range tsrange,
    date_start DATE,
    date_end DATE,
    date_range daterange,
    duration_days numeric
);

INSERT INTO gwolofs.rescu_individual_outages (
    detector_id,
    time_start,
    time_end,
    time_range,
    date_start,
    date_end,
    date_range,
    duration_days
)

--outages by sensor
--1 month takes 12 minutes
WITH time_bins AS (
    SELECT generate_series(
        '2023-05-01 00:00'::TIMESTAMP,
        '2023-05-28 23:45',
        '15 minutes') AS time_bin
),

detectors AS (
    SELECT DISTINCT detector_id
    FROM rescu.volumes_15min AS v15
    JOIN time_bins AS tb ON v15.datetime_bin = tb.time_bin
--WHERE detector_id IN ('DW0161DWG', 'DW0161DEG', 'DN0140DND', 'DN0140DSD') --for testing
),

individual_detector_bins AS (
    SELECT
        d.detector_id,
        tb.time_bin,
        COUNT(v15.*) FILTER (WHERE v15.volume_15min > 0) AS detector_active
    FROM detectors AS d
    CROSS JOIN time_bins AS tb
    LEFT JOIN gwolofs.rescu_outages AS ro ON ro.time_range @> tb.time_bin
    LEFT JOIN rescu.volumes_15min AS v15 ON
        v15.datetime_bin = tb.time_bin
        AND v15.detector_id = d.detector_id
    WHERE ro.time_range IS NULL
    GROUP BY 1, 2
    ORDER BY 1, 2
),

run_groups AS (
    SELECT
        time_bin,
        detector_id,
        detector_active,
        (
            SELECT COUNT(*) AS count
            FROM individual_detector_bins AS g
            WHERE g.detector_active <> gr.detector_active
                AND g.time_bin <= gr.time_bin
                AND g.detector_id = gr.detector_id
        ) AS run_group
    FROM individual_detector_bins
)

SELECT
    detector_id,
    MIN(time_bin) AS time_start,
    MAX(time_bin) AS time_end,
    tsrange(MIN(time_bin), MAX(time_bin), '[]') AS time_range,
    MIN(time_bin)::DATE AS date_start,
    MAX(time_bin)::DATE AS date_end,
    daterange(MIN(time_bin)::DATE, MAX(time_bin)::DATE, '[]') AS date_range,
    COUNT(*) / (60.0 / 15 * 24) AS duration_days
FROM run_groups
WHERE detector_active = 0 --only count length of outages
GROUP BY
    detector_id,
    run_group
HAVING COUNT(*) > 1 --don't include single bin outages
ORDER BY
    detector_id,
    MIN(time_bin)