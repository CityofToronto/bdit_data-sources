--- new anomalous ranges prior to 11-2026 open-data publication

--- The new ranges

INSERT INTO ecocounter.anomalous_ranges (
    site_id, time_range, notes, investigation_level, problem_level
)
VALUES
(
    300044518,
    tsrange('2026-10-01'::date, NULL, '[)'),
    'suspiciously low volumes in only one direction. Possibly due to construction',
    'suspect',
    'do-not-use'
),
(
    300067807,
    tsrange('2026-10-14'::date, '2026-10-20'::date, '[]'),
    'Zeros-value counts, site was vandalized',
    'confirmed',
    'do-not-use'
),
(
    300067807,
    tsrange('2026-10-31'::date, '2026-10-31'::date, '[]'),
    'Zeros-value counts, site was vandalized',
    'confirmed',
    'do-not-use'
),
(
    300067807,
    tsrange('2026-11-02'::date, '2026-11-06'::date, '[]'),
    'Zeros-value counts, site was vandalized',
    'confirmed',
    'do-not-use'
),
(
    300028433,
    tsrange('2026-09-30'::date, '2026-10-14'::date, '[]'),
    'Strange volumes likely caused by dying or dead battery',
    'confirmed',
    'do-not-use'
);

--- Deletions
DELETE FROM ecocounter.open_data_daily_counts AS od
USING (
    SELECT * FROM ecocounter.anomalous_ranges
    WHERE
        site_id IN (300044518, 300067807, 300028433)
        AND problem_level = 'do-not-use'
) AS ar
WHERE
    od.site_id = ar.site_id
    AND od.dt::timestamp <@ ar.time_range;

DELETE FROM ecocounter.open_data_15min_counts AS od
USING (
    SELECT * FROM ecocounter.anomalous_ranges
    WHERE
        site_id IN (300044518, 300067807, 300028433)
        AND problem_level = 'do-not-use'
) AS ar
WHERE
    od.site_id = ar.site_id
    AND od.datetime_bin <@ ar.time_range;

-- Checks
SELECT * FROM ecocounter.anomalous_ranges
WHERE site_id IN (300044518, 300067807, 300028433);

WITH ar AS (
    SELECT * FROM ecocounter.anomalous_ranges
    WHERE
        site_id IN (300044518, 300067807, 300028433)
        AND problem_level = 'do-not-use'
)

SELECT * FROM ecocounter.open_data_daily_counts AS od
JOIN ar
    ON od.site_id = ar.site_id
    AND od.dt::timestamp <@ ar.time_range;

WITH ar AS (
    SELECT * FROM ecocounter.anomalous_ranges
    WHERE
        site_id IN (300044518, 300067807, 300028433)
        AND problem_level = 'do-not-use'
)

SELECT * FROM ecocounter.open_data_15min_counts AS od
JOIN ar
    ON od.site_id = ar.site_id
    AND od.datetime_bin::timestamp <@ ar.time_range;
