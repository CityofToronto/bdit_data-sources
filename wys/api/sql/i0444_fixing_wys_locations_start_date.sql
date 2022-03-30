/* 
Updating the table wys.locations so that the earliest start_date for each api_id 
is the first date on which we have data #444 
*/
WITH t AS (
    SELECT speed.api_id, speed.start_date, loc.id
    FROM (
        SELECT api_id, DATE(MIN(datetime_bin)) AS start_date 
        FROM wys.speed_counts_agg_5kph
        Group BY api_id) speed
    JOIN (
        SELECT DISTINCT ON (api_id) * 
        FROM wys.locations
        ORDER BY api_id, start_date) loc
    ON speed.api_id = loc.api_id)

UPDATE wys.locations
SET start_date = t.start_date
FROM t
WHERE wys.locations.id = t.id;