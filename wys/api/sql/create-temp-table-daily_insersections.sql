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

INSERT INTO daily_intersections (
    api_id, address, sign_name, dir, start_date, loc
) VALUES %s;

UPDATE daily_intersections
SET geom = ST_Transform(
    ST_SetSRID(
        ST_MakePoint(
            split_part(regexp_replace(loc, '[()]'::text, ''::text, 'g'::text), ','::text, 2)::double precision,
            split_part(regexp_replace(loc, '[()]'::text, ''::text, 'g'::text), ','::text, 1)::double precision
        ),
        4326),
    2952
);