-- Apply changes to ALL flows on a "counter" at sites with both sides of the road on one counter.
-- 2 cases. This was confirmed by changes seen in the factors on both sides of the 
-- road following a sensitivity change requested for only one side in May 2025.

-- Sheppard Ave W, west of Sentinel Rd (multi-use path)
UPDATE ecocounter.sensitivity_history
SET 
    date_range = daterange(lower(date_range), '2025-05-14', '[)')
WHERE flow_id IN (353324722, 353324720) AND date_range = '[2022-07-25,)';

INSERT INTO ecocounter.sensitivity_history (
VALUES 
    (353324722, daterange('2025-05-14', NULL, '[)'), '2 (01C0)'),
    (353324720, daterange('2025-05-14', NULL, '[)'), '2 (01C0)')
);

-- Yonge St, north of St Clair Ave
UPDATE ecocounter.sensitivity_history
SET 
    date_range = daterange(lower(date_range), '2025-05-14', '[)')
WHERE flow_id IN (353341348) AND date_range = '[2024-09-01,)';

INSERT INTO ecocounter.sensitivity_history (
VALUES 
    (353341348, daterange('2025-05-14', NULL, '[)'), '2')
);

-- 