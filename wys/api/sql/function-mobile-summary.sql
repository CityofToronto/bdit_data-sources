CREATE OR REPLACE FUNCTION wys.mobile_summary_for_month (_mon DATE)
RETURNS void
    LANGUAGE 'sql'

    COST 100
    VOLATILE SECURITY DEFINER 
AS $BODY$

INSERT INTO wys.mobile_summary
SELECT 
    loc.location_id,
    loc.ward_no,
    loc.location,
    loc.from_street,
    loc.to_street,
    loc.direction,
    loc.installation_date,
    loc.removal_date,
    ssc.schedule,
    ssc.min_speed,
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
    SUM(raw.count) FILTER (WHERE sb.speed_id = 1) AS spd_00,
    SUM(raw.count) FILTER (WHERE sb.speed_id = 2) AS spd_05,
    SUM(raw.count) FILTER (WHERE sb.speed_id = 3) AS spd_10,
    SUM(raw.count) FILTER (WHERE sb.speed_id = 4) AS spd_15,
    SUM(raw.count) FILTER (WHERE sb.speed_id = 5) AS spd_20,
    SUM(raw.count) FILTER (WHERE sb.speed_id = 6) AS spd_25,
    SUM(raw.count) FILTER (WHERE sb.speed_id = 7) AS spd_30,
    SUM(raw.count) FILTER (WHERE sb.speed_id = 8) AS spd_35,
    SUM(raw.count) FILTER (WHERE sb.speed_id = 9) AS spd_40,
    SUM(raw.count) FILTER (WHERE sb.speed_id = 10) AS spd_45,
    SUM(raw.count) FILTER (WHERE sb.speed_id = 11) AS spd_50,
    SUM(raw.count) FILTER (WHERE sb.speed_id = 12) AS spd_55,
    SUM(raw.count) FILTER (WHERE sb.speed_id = 13) AS spd_60,
    SUM(raw.count) FILTER (WHERE sb.speed_id = 14) AS spd_65,
    SUM(raw.count) FILTER (WHERE sb.speed_id = 15) AS spd_70,
    SUM(raw.count) FILTER (WHERE sb.speed_id = 16) AS spd_75,
    SUM(raw.count) FILTER (WHERE sb.speed_id = 17) AS spd_80,
    SUM(raw.count) FILTER (WHERE sb.speed_id = 18) AS spd_85,
    SUM(raw.count) FILTER (WHERE sb.speed_id = 19) AS spd_90,
    SUM(raw.count) FILTER (WHERE sb.speed_id = 20) AS spd_95,
    SUM(raw.count) FILTER (WHERE sb.speed_id = 21) AS spd_100_and_above,
    SUM(raw.count) AS COUNT
FROM wys.mobile_api_id AS loc
JOIN wys.raw_data AS raw ON
    loc.api_id = raw.api_id 
    AND raw.datetime_bin > loc.installation_date 
    AND raw.datetime_bin < loc.removal_date
JOIN wys.speed_bins_old AS sb ON
    raw.speed >= lower(sb.speed_bin)
    AND raw.speed < upper(sb.speed_bin)
LEFT OUTER JOIN wys.sign_schedules_list AS lst ON lst.api_id = loc.api_id
LEFT OUTER JOIN wys.sign_schedules_clean AS ssc USING (schedule_name)
WHERE
    removal_date >= _mon
    AND removal_date < _mon + interval '1 month'
GROUP BY
    loc.location_id,
    loc.ward_no,
    loc.location,
    loc.from_street, 
    loc.to_street,
    loc.direction,
    loc.installation_date, 
    loc.removal_date,
    ssc.schedule,
    ssc.min_speed
$BODY$;

REVOKE EXECUTE ON FUNCTION wys.stationary_summary_for_month (DATE)FROM public;
GRANT EXECUTE ON FUNCTION wys.stationary_summary_for_month (DATE) TO wys_bot;