

DROP TABLE IF EXISTS inrix.agg_extract_hour_ymd; 
CREATE TABLE inrix.agg_extract_hour_ymd ( 
    tmc char(9) NOT NULL,
    time_15_continuous smallint NOT NULL,
    day_continuous smallint NOT NULL,
    count smallint NOT NULL,
    avg_speed numeric(5,2) NOT NULL,
    month smallint NOT NULL,
    year smallint NOT NULL,
    weekday smallint
);

COMMENT ON TABLE inrix.agg_extract_hour_ymd
  IS 'Added year month day columns to test aggregation speeds.';

DROP TABLE IF EXISTS inrix.agg_extract_hour;
CREATE TABLE inrix.agg_extract_hour ( 
    tmc char(9) NOT NULL,
    time_15_continuous smallint NOT NULL,
    dt DATE NOT NULL, 
    count smallint NOT NULL,
    avg_speed numeric(5,2) NOT NULL
);