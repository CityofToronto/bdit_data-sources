--DROP VIEW open_data.wys_mobile_summary CASCADE; --wys_mobile_detailed depends on this
CREATE OR REPLACE VIEW open_data.wys_mobile_summary AS

SELECT
    location_id,
    ward_no,
    location,
    from_street,
    to_street,
    direction,
    installation_date,
    removal_date,
    days_with_data,
    max_date,
    schedule, 
    min_speed,
    pct_05,
    pct_10,
    pct_15,
    pct_20,
    pct_25,
    pct_30,
    pct_35,
    pct_40,
    pct_45,
    pct_50,
    pct_55,
    pct_60,
    pct_65,
    pct_70,
    pct_75,
    pct_80,
    pct_85,
    pct_90,
    pct_95,
    spd_00,
    spd_05,
    spd_10,
    spd_15,
    spd_20,
    spd_25,
    spd_30,
    spd_35,
    spd_40,
    spd_45,
    spd_50,
    spd_55,
    spd_60,
    spd_65,
    spd_70,
    spd_75,
    spd_80,
    spd_85,
    spd_90,
    spd_95,
    spd_100_and_above,
    volume
FROM wys.mobile_summary
WHERE (
        --avoid adding signs with less than one day of data to Open Data.
        removal_date > installation_date + interval '1 day'
        OR removal_date IS NULL
    )
    AND (
        --avoid adding very new signs with little data to Open Data.
        installation_date < date_trunc('month', now()) - interval '2 weeks'
        --however, do include these signs if they are complete.
        OR removal_date < date_trunc('month', now())
    )
ORDER BY installation_date DESC;
ALTER TABLE open_data.wys_mobile_summary
    OWNER TO wys_admins;

GRANT SELECT ON TABLE open_data.wys_mobile_summary TO bdit_humans;
GRANT SELECT ON TABLE open_data.wys_mobile_summary TO od_extract_svc;
COMMENT ON VIEW open_data.wys_mobile_summary IS 'Summary data for each mobile sign installation from the wys.mobile_summary table. Long-running "permanent" signs will have their data included even if they haven''t been removed yet.';