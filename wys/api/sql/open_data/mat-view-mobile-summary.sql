-- View: open_data.wys_mobile_summary

DROP MATERIALIZED VIEW open_data.wys_mobile_summary;

CREATE MATERIALIZED VIEW open_data.wys_mobile_summary
TABLESPACE pg_default
AS
 SELECT loc.ward_no,
    loc.location,
    loc.from_street,
    loc.to_street,
    loc.direction,
    loc.installation_date,
    loc.removal_date,
    schedule, 
    min_speed, 
    speed_limit, 
    flash_speed, 
    strobe_speed,
    percentile_cont(0.05) WITHIN GROUP (ORDER BY (raw.speed))::INT AS pct_05,
    percentile_cont(0.10) WITHIN GROUP (ORDER BY (raw.speed))::INT AS pct_10,
    percentile_cont(0.15) WITHIN GROUP (ORDER BY (raw.speed))::INT AS pct_15,
    percentile_cont(0.20) WITHIN GROUP (ORDER BY (raw.speed))::INT AS pct_20,
    percentile_cont(0.25) WITHIN GROUP (ORDER BY (raw.speed))::INT AS pct_25,
    percentile_cont(0.30) WITHIN GROUP (ORDER BY (raw.speed))::INT AS pct_30,
    percentile_cont(0.35) WITHIN GROUP (ORDER BY (raw.speed))::INT AS pct_35,
    percentile_cont(0.40) WITHIN GROUP (ORDER BY (raw.speed))::INT AS pct_40,
    percentile_cont(0.45) WITHIN GROUP (ORDER BY (raw.speed))::INT AS pct_45,
    percentile_cont(0.50) WITHIN GROUP (ORDER BY (raw.speed))::INT AS pct_50,
    percentile_cont(0.55) WITHIN GROUP (ORDER BY (raw.speed))::INT AS pct_55,
    percentile_cont(0.60) WITHIN GROUP (ORDER BY (raw.speed))::INT AS pct_60,
    percentile_cont(0.65) WITHIN GROUP (ORDER BY (raw.speed))::INT AS pct_65,
    percentile_cont(0.70) WITHIN GROUP (ORDER BY (raw.speed))::INT AS pct_70,
    percentile_cont(0.75) WITHIN GROUP (ORDER BY (raw.speed))::INT AS pct_75,
    percentile_cont(0.80) WITHIN GROUP (ORDER BY (raw.speed))::INT AS pct_80,
    percentile_cont(0.85) WITHIN GROUP (ORDER BY (raw.speed))::INT AS pct_85,
    percentile_cont(0.90) WITHIN GROUP (ORDER BY (raw.speed))::INT AS pct_90,
    percentile_cont(0.95) WITHIN GROUP (ORDER BY (raw.speed))::INT AS pct_95,
    count(1) AS count
   FROM wys.mobile_api_id loc
     JOIN wys.sign_schedules_list USING (api_id)
     JOIN wys.sign_schedules_clean USING (schedule_name)
	 JOIN wys.raw_data raw ON loc.api_id = raw.api_id 
                         AND loc.removal_date < date_trunc('month', now())
                         AND raw.datetime_bin >= loc.installation_date 
                         AND raw.datetime_bin < loc.removal_date
  GROUP BY loc.ward_no, loc.location, loc.from_street, loc.to_street, loc.direction, loc.installation_date, loc.removal_date,schedule, 
    min_speed, speed_limit, flash_speed, strobe_speed
WITH DATA;

ALTER TABLE open_data.wys_mobile_summary
    OWNER TO rdumas;

GRANT SELECT ON TABLE open_data.wys_mobile_summary TO od_extract_svc;
GRANT ALL ON TABLE open_data.wys_mobile_summary TO rdumas;
GRANT SELECT ON TABLE open_data.wys_mobile_summary TO bdit_humans;