/*
Purpose:
A monthly summary of the mobile WYS sign data.

Dependent Objects:
    Type    |Name
    View    |open_data.wys_mobile_summary
*/
-- DROP TABLE IF EXISTS wys.mobile_summary;

CREATE TABLE IF NOT EXISTS wys.mobile_summary ( -- noqa: PRS
    location_id integer,
    ward_no integer,
    location text COLLATE pg_catalog."default",
    from_street text COLLATE pg_catalog."default",
    to_street text COLLATE pg_catalog."default",
    direction text COLLATE pg_catalog."default",
    installation_date date,
    removal_date date,
    days_with_data integer,
    max_date date,
    schedule text COLLATE pg_catalog."default",
    min_speed integer,
    pct_05 integer,
    pct_10 integer,
    pct_15 integer,
    pct_20 integer,
    pct_25 integer,
    pct_30 integer,
    pct_35 integer,
    pct_40 integer,
    pct_45 integer,
    pct_50 integer,
    pct_55 integer,
    pct_60 integer,
    pct_65 integer,
    pct_70 integer,
    pct_75 integer,
    pct_80 integer,
    pct_85 integer,
    pct_90 integer,
    pct_95 integer,
    spd_00 integer,
    spd_05 integer,
    spd_10 integer,
    spd_15 integer,
    spd_20 integer,
    spd_25 integer,
    spd_30 integer,
    spd_35 integer,
    spd_40 integer,
    spd_45 integer,
    spd_50 integer,
    spd_55 integer,
    spd_60 integer,
    spd_65 integer,
    spd_70 integer,
    spd_75 integer,
    spd_80 integer,
    spd_85 integer,
    spd_90 integer,
    spd_95 integer,
    spd_100_and_above integer,
    volume integer,
    CONSTRAINT mobile_summary_ward_no_location_from_street_to_street_direc_key UNIQUE (ward_no, location, from_street, to_street, direction, installation_date)
)
WITH (
    OIDS = FALSE
)
TABLESPACE pg_default;

ALTER TABLE IF EXISTS wys.mobile_summary OWNER TO wys_admins;

REVOKE ALL ON TABLE wys.mobile_summary FROM wys_bot;

GRANT SELECT ON TABLE wys.mobile_summary TO bdit_humans;

GRANT ALL ON TABLE wys.mobile_summary TO wys_admins;

GRANT DELETE, INSERT, SELECT ON TABLE wys.mobile_summary TO wys_bot;