-- This table was created to store weeks that were identified as having low or no car volumes via the "graph weekly data and note the dips" process
CREATE TABLE scannon.miovision_bad_weeks (
    intersection_uid smallint,
    intersection_name text,
    bad_week date,
    UNIQUE ( intersection_uid, bad_week )
);

-- I used a psql /copy command to populate the table.