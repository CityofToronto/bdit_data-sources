CREATE VIEW gwolofs.congestion_time_grps AS

SELECT
    start_tod,
    end_tod,
    1 AS table_order
FROM (
    VALUES
        ('00:00:00'::time, '06:00:00'::time),
        ('06:00:00'::time, '10:00:00'::time),
        ('10:00:00'::time, '15:00:00'::time),
        ('15:00:00'::time, '19:00:00'::time),
        ('19:00:00'::time, '24:00:00'::time)
) AS times(start_tod, end_tod)
UNION
SELECT
    (start_hour || ':00')::time AS start_tod,
    (start_hour + 1 || ':00')::time AS end_tod,
    2 AS table_order
FROM generate_series(0, 23, 1) AS start_hour
ORDER BY
    table_order,
    start_tod,
    end_tod;
    
COMMENT ON VIEW gwolofs.congestion_time_grps
IS 'Hours and time periods for congestion aggregation.';

ALTER VIEW gwolofs.congestion_time_grps OWNER TO gwolofs;

GRANT SELECT ON TABLE gwolofs.congestion_time_grps TO bdit_humans;
GRANT ALL ON TABLE gwolofs.congestion_time_grps TO gwolofs;
