DROP TABLE IF EXISTS daily_intersections;

CREATE TEMP TABLE daily_intersections (
    api_id integer NOT NULL,
    address text,
    sign_name text,
    dir text,
    start_date date,
    loc text,
    geom geometry
);