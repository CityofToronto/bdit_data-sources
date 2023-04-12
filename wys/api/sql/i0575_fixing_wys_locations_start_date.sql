/*
Updating the table wys.locations so that the earliest start_date for each api_id 
is the first date on which we have data
*/

UPDATE wys.locations
SET start_date = speed.start_date
FROM (
    SELECT
        api_id,
        DATE(MIN(datetime_bin)) AS start_date
    FROM wys.speed_counts_agg_5kph
    GROUP BY api_id
) AS speed
WHERE
    locations.api_id = speed.api_id
    AND locations.id IN (
        SELECT DISTINCT ON (api_id) id
        FROM wys.locations
        ORDER BY api_id, start_date
    );