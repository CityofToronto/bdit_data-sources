-- DROP TABLE gwolofs.miovision_open_data_monthly_summary;

CREATE TABLE gwolofs.miovision_open_data_monthly_summary (
    intersection_uid integer,
    intersection_long_name text,
    mnth date,
    avg_daily_vol_auto numeric,
    avg_daily_vol_truckbus numeric,
    avg_daily_vol_ped numeric,
    avg_daily_vol_bike numeric,
    avg_am_peak_hour_vol_auto numeric,
    avg_am_peak_hour_vol_truckbus numeric,
    avg_am_peak_hour_vol_ped numeric,
    avg_am_peak_hour_vol_bike numeric,
    avg_pm_peak_hour_vol_auto numeric,
    avg_pm_peak_hour_vol_truckbus numeric,
    avg_pm_peak_hour_vol_ped numeric,
    avg_pm_peak_hour_vol_bike numeric,
    CONSTRAINT miovision_open_data_monthly_summary_pkey PRIMARY KEY (
        intersection_uid, intersection_long_name, mnth
    )
)

TABLESPACE pg_default;

CREATE INDEX miovision_monthly_od_dt_idx ON
gwolofs.miovision_open_data_monthly_summary USING brin (mnth);

ALTER TABLE IF EXISTS gwolofs.miovision_open_data_monthly_summary
OWNER TO gwolofs;

REVOKE ALL ON TABLE gwolofs.miovision_open_data_monthly_summary FROM bdit_humans;

GRANT SELECT ON TABLE gwolofs.miovision_open_data_monthly_summary TO bdit_humans;

GRANT ALL ON TABLE gwolofs.miovision_open_data_monthly_summary TO gwolofs;

GRANT ALL ON TABLE gwolofs.miovision_open_data_monthly_summary TO miovision_admins;

GRANT SELECT, INSERT, DELETE ON TABLE gwolofs.miovision_open_data_monthly_summary
TO miovision_api_bot;

COMMENT ON TABLE gwolofs.miovision_open_data_monthly_summary
IS 'Table to store Miovision monthly summary open data. 
Contains an approachable monthly-intersection summary of 
avg daily and avg peak AM/PM hour volumes by mode.';