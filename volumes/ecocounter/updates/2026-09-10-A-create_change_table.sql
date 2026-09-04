-- Drop if exists
DROP TABLE IF EXISTS temp_ecocounter_changes;


--- Initiate table and populate with new sites ----

CREATE TEMP TABLE temp_ecocounter_changes (
    site_id bigint,
    direction_main travel_directions,
    change text,
    PRIMARY KEY (site_id, direction_main)
);


INSERT INTO temp_ecocounter_changes
VALUES
(300066507, 'Eastbound', 'new_site'),
(300066342, 'Northbound', 'new_site'),
(300066343, 'Southbound', 'new_site'),
(300066428, 'Eastbound', 'new_site'),
(300066427, 'Westbound', 'new_site'),
(300066611, 'Eastbound', 'new_site'),
(300066611, 'Westbound', 'new_site'),
(300066447, 'Eastbound', 'new_site'),
(300066447, 'Westbound', 'new_site'),
(300067807, 'Westbound', 'new_site'),
(300067807, 'Eastbound', 'new_site'),
(300067687, 'Westbound', 'new_site'),
(300066346, 'Eastbound', 'new_site'),
(300066351, 'Westbound', 'new_site'),
(300060828, 'Eastbound', 'new_site'),
(300062143, 'Westbound', 'new_site');

INSERT INTO temp_ecocounter_changes
VALUES
(300024488, 'Northbound', 'anomalous_range'),
(300024488, 'Southbound', 'anomalous_range'),
(300024652, 'Northbound', 'anomalous_range'),
(300024652, 'Southbound', 'anomalous_range'),
(300026120, 'Northbound', 'anomalous_range'),
(300026120, 'Southbound', 'anomalous_range'),
(300026125, 'Westbound', 'anomalous_range'),
(300026125, 'Eastbound', 'anomalous_range');

ALTER TABLE temp_ecocounter_changes
ADD COLUMN count_date date;

--- UPDATE count_date ----

UPDATE temp_ecocounter_changes e
SET count_date = m.count_date
FROM (
    SELECT *
    FROM ecocounter.manual_counts_info
    WHERE count_date >= '2026-01-01'
) AS m
WHERE m.ecocounter_site_id = e.site_id;

--- OUTER leftJOIN FOR flow_id ----

-- Drop if exists
DROP TABLE IF EXISTS joined_flow;

CREATE TABLE temp_joined_flow AS
WITH f AS (
    SELECT
        flow_id,
        site_id,
        direction_main,
        mode_counted
    FROM ecocounter.flows_unfiltered
    WHERE date_decommissioned IS NULL
)

SELECT
    e.*,
    f.flow_id,
    f.mode_counted
FROM temp_ecocounter_changes AS e
LEFT JOIN f
    ON e.site_id = f.site_id
    AND e.direction_main = f.direction_main;

DROP TABLE temp_ecocounter_changes;

ALTER TABLE temp_joined_flow RENAME TO temp_ecocounter_changes;