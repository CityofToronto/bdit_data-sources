/*
Parameters:
Name | Type | Description
_mon | date | Month whose data to be aggregated

Return: Void
Purpose: Summarizes the data of stationary WYS signs for the given month
*/
CREATE OR REPLACE FUNCTION wys.stationary_summary_for_month(_mon date)
RETURNS void
LANGUAGE 'sql'

COST 100
VOLATILE SECURITY DEFINER
AS $BODY$

SELECT wys.clear_stationary_summary_for_month(_mon);

INSERT INTO wys.stationary_summary
SELECT 
    sign_id,
    _mon AS mon,
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
    SUM(CASE sb.speed_id WHEN 1 THEN "count" ELSE 0 END) AS spd_00,
    SUM(CASE sb.speed_id WHEN 2 THEN "count" ELSE 0 END) AS spd_05,
    SUM(CASE sb.speed_id WHEN 3 THEN "count" ELSE 0 END) AS spd_10,
    SUM(CASE sb.speed_id WHEN 4 THEN "count" ELSE 0 END) AS spd_15,
    SUM(CASE sb.speed_id WHEN 5 THEN "count" ELSE 0 END) AS spd_20,
    SUM(CASE sb.speed_id WHEN 6 THEN "count" ELSE 0 END) AS spd_25,
    SUM(CASE sb.speed_id WHEN 7 THEN "count" ELSE 0 END) AS spd_30,
    SUM(CASE sb.speed_id WHEN 8 THEN "count" ELSE 0 END) AS spd_35,
    SUM(CASE sb.speed_id WHEN 9 THEN "count" ELSE 0 END) AS spd_40,
    SUM(CASE sb.speed_id WHEN 10 THEN "count" ELSE 0 END) AS spd_45,
    SUM(CASE sb.speed_id WHEN 11 THEN "count" ELSE 0 END) AS spd_50,
    SUM(CASE sb.speed_id WHEN 12 THEN "count" ELSE 0 END) AS spd_55,
    SUM(CASE sb.speed_id WHEN 13 THEN "count" ELSE 0 END) AS spd_60,
    SUM(CASE sb.speed_id WHEN 14 THEN "count" ELSE 0 END) AS spd_65,
    SUM(CASE sb.speed_id WHEN 15 THEN "count" ELSE 0 END) AS spd_70,
    SUM(CASE sb.speed_id WHEN 16 THEN "count" ELSE 0 END) AS spd_75,
    SUM(CASE sb.speed_id WHEN 17 THEN "count" ELSE 0 END) AS spd_80,
    SUM(CASE sb.speed_id WHEN 18 THEN "count" ELSE 0 END) AS spd_85,
    SUM(CASE sb.speed_id WHEN 19 THEN "count" ELSE 0 END) AS spd_90,
    SUM(CASE sb.speed_id WHEN 20 THEN "count" ELSE 0 END) AS spd_95,
    SUM(CASE sb.speed_id WHEN 21 THEN "count" ELSE 0 END) AS spd_100_and_above,
    SUM("count") AS volume,
    raw.api_id
FROM wys.raw_data raw
INNER JOIN wys.stationary_signs sign ON
    raw.api_id = sign.api_id
    AND (
        prev_start IS NULL
        OR datetime_bin >= start_date
    ) AND (
        next_start IS NULL
        OR datetime_bin < next_start
    )
JOIN wys.speed_bins_old AS sb ON
    raw.speed >= lower(sb.speed_bin)
    AND (
        raw.speed < upper(sb.speed_bin)
        OR upper(sb.speed_bin) IS NULL
    )
WHERE
    datetime_bin >= _mon
    AND datetime_bin < _mon + interval '1 month'
GROUP BY
    sign_id,
    mon,
    raw.api_id;

$BODY$;

ALTER FUNCTION wys.stationary_summary_for_month(date) OWNER TO wys_admins;

REVOKE EXECUTE ON FUNCTION wys.stationary_summary_for_month(date) FROM public;
GRANT EXECUTE ON FUNCTION wys.stationary_summary_for_month(date) TO wys_bot;
