-- This table was created to store weeks that were identified as having low or no car volumes via the "graph weekly data and note the dips" process
CREATE TABLE scannon.miovision_bad_weeks (
    intersection_uid smallint,
    intersection_name text,
    bad_week date,
    UNIQUE (intersection_uid, bad_week)
);

COMMENT ON TABLE IS 
'Contains weeks where the volume of lights (aka classification_uid = 1; cars and other light vehicles) is zero or significantly lower than historical volumes. Data recorded from January 2019 until the end of March 2023 were evaluated. Volumes beyond March 2023 have not been evaluated.' 

-- I used a psql /copy command to populate the table.