CREATE TABLE inrix.agg_wkday_mnth (
    tmc char(9) NOT NULL,
    time_15_continuous numeric(3) NOT NULL,
    yyyy numeric(4) NOT NULL,
    mm numeric(2) NOT NULL,
    dow numeric(1) NOT NULL,
    cnt smallint NOT NULL,
    avg_speed numeric(5, 2) NOT NULL
)