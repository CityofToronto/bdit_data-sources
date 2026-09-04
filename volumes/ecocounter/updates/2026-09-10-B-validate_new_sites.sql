---- Commit changes for new counters ----
-----------------------------------------

DROP TABLE IF EXISTS temp_validated_sites;

CREATE TABLE temp_validated_sites AS
SELECT * FROM temp_ecocounter_changes
WHERE change = 'new_site';

---- sites unfiltered ----
UPDATE ecocounter.sites_unfiltered AS a
SET validated = TRUE
FROM temp_validated_sites AS v
WHERE a.site_id = v.site_id;

---- flows unfiltered ----
UPDATE ecocounter.flows_unfiltered AS a
SET validated = TRUE
FROM temp_validated_sites AS v
WHERE a.flow_id = v.flow_id;

---- anomolous ranges --- (Theres only one and the date_range is closed)
--SELECT * FROM ecocounter.anomalous_ranges
--WHERE site_id IN (
--    SELECT site_id
--    FROM temp_validated_sites
--);


---- New Anomolous ranges ----
------------------------------

---- There is only one that needs to be added, one which needs to be marked do-not-use
DROP TABLE IF EXISTS temp_anomalous_sites;

CREATE TABLE temp_anomalous_sites AS
SELECT DISTINCT
    site_id,
    count_date
FROM temp_ecocounter_changes
WHERE
    change = 'anomalous_range'
    AND site_id NOT IN (
        SELECT site_id
        FROM ecocounter.anomalous_ranges
        WHERE upper(time_range) IS NULL
    ); -- did not realise there was only one until I made this table


ALTER TABLE temp_anomalous_sites
ADD COLUMN uid smallint DEFAULT 110,
ADD COLUMN time_range tsrange,
ADD COLUMN flow_id numeric,
ADD COLUMN notes text DEFAULT 'unreasonable over/under counts',
ADD COLUMN investigation_level text DEFAULT 'confirmed',
ADD COLUMN problem_level text DEFAULT 'do-not-use';

-- use date_count for daterange
UPDATE temp_anomalous_sites
SET time_range = tsrange(count_date, NULL, '[)');

--- Drop unnecessary date column
ALTER TABLE temp_anomalous_sites
DROP count_date;

-- Add to table -- 
INSERT INTO ecocounter.anomalous_ranges
SELECT
    uid,
    flow_id,
    site_id,
    time_range,
    notes,
    investigation_level,
    problem_level
FROM temp_anomalous_sites;

--- This one was open ended but did not have the correct problem-level
UPDATE ecocounter.anomalous_ranges
SET
    problem_level = 'do-not-use',
    notes = 'unreasonable over/under counts'
WHERE site_id = 300024652 AND upper(time_range) IS NULL;

--- All done!



