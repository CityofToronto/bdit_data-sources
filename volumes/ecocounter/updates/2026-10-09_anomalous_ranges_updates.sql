--- implementing changes as identified in issue # 1510

INSERT INTO ecocounter.anomalous_ranges (
    flow_id, site_id, time_range, notes, investigation_level, problem_level
)
VALUES
(
    NULL,
    300028589,
    tsrange('2026-08-07', '2026-09-17', '[]'),
    'Removing partial days caused my new battery installation',
    'confirmed',
    'do-not-use'
),
(
    NULL,
    300062143,
    tsrange('2026-08-22', NULL, '[)'),
    'site is no longer working, unclear why',
    'suspect',
    'do-not-use'
),
(
    NULL,
    300028595,
    tsrange('2026-07-04', NULL, '[)'),
    'cellular transmission issue',
    'confirmed',
    'do-not-use'
);

