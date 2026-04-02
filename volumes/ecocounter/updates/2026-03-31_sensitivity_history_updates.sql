-- Apply changes to ALL flows on a "counter" at sites with both sides of the road on one counter.
-- 2 cases. This was confirmed by changes seen in the factors on both sides of the 
-- road following a sensitivity change requested for only one side in May 2025.

-- Sheppard Ave W, west of Sentinel Rd (multi-use path)
UPDATE ecocounter.sensitivity_history
SET 
    date_range = daterange(lower(date_range), '2025-05-14', '[)')
WHERE flow_id IN (353324722, 353324720) AND date_range = '[2022-07-25,)';

INSERT INTO ecocounter.sensitivity_history(flow_id, date_range, setting) (
VALUES 
    (353324722, daterange('2025-05-14', NULL, '[)'), '2 (01C0)'),
    (353324720, daterange('2025-05-14', NULL, '[)'), '2 (01C0)')
);

-- Yonge St, north of St Clair Ave
UPDATE ecocounter.sensitivity_history
SET 
    date_range = daterange(lower(date_range), '2025-05-14', '[)')
WHERE flow_id IN (353341348) AND date_range = '[2024-09-01,)';

INSERT INTO ecocounter.sensitivity_history(flow_id, date_range, setting) (
VALUES 
    (353341348, daterange('2025-05-14', NULL, '[)'), '2')
);

-- Newly repaired or reconfigured sensors

-- Bloor St E, west of Castle Frank Rd
-- re-installed sensors started producing good data on 2025-02-27. after ~2 yr gap.
INSERT INTO ecocounter.sensitivity_history(flow_id, date_range, setting) (
VALUES 
    (353517542, daterange('2025-02-27', NULL, '[)'), 'initial config after repair'),
    (353354428, daterange('2025-02-27', NULL, '[)'), 'initial config after repair')
);

-- Bloor St W, west of Huron St (site_id = 300028396) - 
-- reconfigured sensors (8 in total for bike,scooter,main-flow,contra-flow,2 sides-of-the-road)
-- started producing good data on 2025-06-27.
INSERT INTO ecocounter.sensitivity_history(flow_id, date_range, setting) (
VALUES 
    (353341333, daterange('2025-06-27', NULL, '[)'), 'initial config after reinstallation'),
    (353554896, daterange('2025-06-27', NULL, '[)'), 'initial config after install'),
    (353554897, daterange('2025-06-27', NULL, '[)'), 'initial config after install'),
    (353554898, daterange('2025-06-27', NULL, '[)'), 'initial config after install'),
    (353554899, daterange('2025-06-27', NULL, '[)'), 'initial config after install'),
    (353554900, daterange('2025-06-27', NULL, '[)'), 'initial config after install'),
    (353554901, daterange('2025-06-27', NULL, '[)'), 'initial config after install'),
    (353341334, daterange('2025-06-27', NULL, '[)'), 'initial config after reinstallation')
);
