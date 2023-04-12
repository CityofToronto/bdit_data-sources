/* Delete the extra (duplicated) records in wys.locations that might have been
added since we updated how to add new locations in wys_api.py on April 12, 2022
in this commit e2fc008
*/

CREATE TEMP TABLE distinct_locations AS (
    SELECT DISTINCT ON (api_id, address, sign_name, dir, start_date, loc, geom) *
    FROM wys.locations
);

TRUNCATE wys.wys_locations;

INSERT INTO wys.wys_locations
SELECT * FROM distinct_locations;