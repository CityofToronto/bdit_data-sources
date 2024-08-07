-- most recent record of each sign
WITH locations AS (
    SELECT DISTINCT ON (api_id)
        api_id,
        address,
        sign_name,
        dir,
        loc,
        start_date,
        geom,
        id
    FROM wys.locations 
    ORDER BY
        api_id,
        start_date DESC
),

-- New and moved signs
differences AS (
    SELECT
        di.api_id,
        di.address,
        di.sign_name,
        di.dir,
        di.start_date,
        di.loc,
        di.geom 
    FROM daily_intersections AS di
    LEFT JOIN locations AS loc USING (api_id)
    WHERE
        st_distance(di.geom, loc.geom) > 100 -- moved more than 100m
        OR di.dir <> loc.dir -- changed direction
        OR di.api_id NOT IN (SELECT api_id FROM locations) -- new sign
),

-- Insert new & moved signs into wys.locations
new_signs AS (
    INSERT INTO wys.locations (
        api_id, address, sign_name, dir, start_date, loc, geom
    )
    SELECT
        api_id,
        address,
        sign_name,
        dir,
        start_date,
        loc,
        geom 
    FROM differences
),

-- Signs with new name and/or address
updated_signs AS (
    SELECT
        di.api_id,
        di.address,
        di.sign_name,
        di.dir,
        di.loc,
        di.start_date,
        di.geom,
        loc.id 
    FROM daily_intersections AS di
    LEFT JOIN locations AS loc USING (api_id, dir)
    WHERE
        st_distance(di.geom, loc.geom) < 100
        AND (
            di.sign_name <> loc.sign_name
            OR di.address <> loc.address
        )
)

-- Update name and/or address
UPDATE wys.locations AS loc
SET
    api_id = upd.api_id,
    address = upd.address,
    sign_name = upd.sign_name,
    dir = upd.dir,
    start_date = upd.start_date,
    loc = upd.loc,
    id = upd.id,
    geom = upd.geom
FROM updated_signs AS upd
WHERE loc.id = upd.id;