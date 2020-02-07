DROP VIEW IF EXISTS miovision.report_summary;

CREATE VIEW miovision.report_summary AS

SELECT 
class_type,
street_main,
street_cross,
dir,
period_name, 
avg(CASE WHEN period_type = 'Baseline' THEN total_volume ELSE NULL END) AS baseline,
avg(CASE WHEN period_type = 'Dec 2017' THEN total_volume ELSE NULL END) AS dec_17,
avg(CASE WHEN period_type = 'Jan 2018' THEN total_volume ELSE NULL END) AS jan_18,
avg(CASE WHEN period_type = 'Feb 2018' THEN total_volume ELSE NULL END) AS feb_18,
avg(CASE WHEN period_type = 'Mar 2018' THEN total_volume ELSE NULL END) AS mar_18,
    avg(
        CASE
            WHEN report_daily.period_type = 'Apr 2018'::text THEN report_daily.total_volume
            ELSE NULL::numeric
        END) AS apr_18,
    avg(
        CASE
            WHEN report_daily.period_type = 'May 2018'::text THEN report_daily.total_volume
            ELSE NULL::numeric
        END) AS may_18,
    avg(
        CASE
            WHEN report_daily.period_type = 'Jun 2018'::text THEN report_daily.total_volume
            ELSE NULL::numeric
        END) AS jun_18
,
,
    avg(
        CASE
            WHEN report_daily.period_type = 'Jul 2018'::text THEN report_daily.total_volume
            ELSE NULL::numeric
        END) AS jul_18

FROM miovision.report_daily
GROUP BY class_type, intersection_uid, street_main, street_cross, dir, period_name;