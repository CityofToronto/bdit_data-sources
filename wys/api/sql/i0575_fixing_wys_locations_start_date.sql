/*
1. Updating the table wys.locations so that the earliest start_date for each api_id 
is the first date on which we have data
2. refresh the two materialized views feeding into Open Data
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

REFRESH MATERIALIZED VIEW CONCURRENTLY wys.stationary_signs WITH DATA;

REFRESH MATERIALIZED VIEW CONCURRENTLY wys.mobile_api_id WITH DATA;