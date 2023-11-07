/*
Purpose:
A monthly summary of the stationary WYS sign data.

Dependent Objects:
    Type    |Name
    View    |open_data.wys_stationary_summary
*/
-- DROP TABLE IF EXISTS wys.stationary_summary;

CREATE TABLE IF NOT EXISTS wys.stationary_summary (
    sign_id integer,
    mon date,
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
    api_id integer,
    CONSTRAINT stationary_summary_sign_id_mon_key UNIQUE (sign_id, mon)
)
WITH (
    OIDS = FALSE
)
TABLESPACE pg_default;

ALTER TABLE IF EXISTS wys.stationary_summary OWNER TO wys_admins;

REVOKE ALL ON TABLE wys.stationary_summary FROM wys_bot;

GRANT SELECT ON TABLE wys.stationary_summary TO bdit_humans;

GRANT ALL ON TABLE wys.stationary_summary TO wys_admins;

GRANT DELETE, INSERT, SELECT ON TABLE wys.stationary_summary TO wys_bot;