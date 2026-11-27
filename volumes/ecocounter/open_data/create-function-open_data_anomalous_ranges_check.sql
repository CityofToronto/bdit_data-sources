CREATE OR REPLACE FUNCTION ecocounter.open_data_anomalous_ranges_check()
RETURNS TABLE (
    error_msg_flag boolean,
    error_msg text
)
LANGUAGE sql
COST 100
VOLATILE

AS $BODY$

WITH anomalous_counts AS(
	SELECT ar.site_id, 
		MIN(od.datetime_bin) AS min_datetime, 
		MAX(od.datetime_bin) AS max_datetime,
		'15m_counts' AS source_table
		FROM ecocounter.open_data_15min_counts AS od
	JOIN ecocounter.anomalous_ranges AS ar
		ON od.site_id = ar.site_id
   		AND od.datetime_bin <@ ar.time_range
	---only sites with appropriate problem_level
	WHERE ar.problem_level = 'do-not-use'
	---when flow_id is null, the whole site is in an anomalous range
	AND flow_id IS NULL
	GROUP BY ar.site_id
	UNION ALL
	SELECT ar.site_id, 
		MIN(od.dt) AS min_datetime, 
		MAX(od.dt) AS max_datetime,
		'daily_counts' AS source_table
		FROM ecocounter.open_data_daily_counts AS od
	JOIN ecocounter.anomalous_ranges AS ar
		ON od.site_id = ar.site_id
   		AND od.dt <@ daterange(
		    lower(ar.time_range)::date,
		    upper(ar.time_range)::date)
	WHERE ar.problem_level = 'do-not-use'
	AND flow_id IS NULL
	GROUP BY ar.site_id
	)
SELECT     
	CASE
        WHEN EXISTS (SELECT 1 FROM anomalous_counts)
        THEN False
        ELSE True
	END AS error_msg_flag,
	
	'WARNING: Data in the anomalous ranges table was detected in the open_data tables. You may want to delete them before publication.' || E'\n\n'||
	
	'Rows from `ecocounter.open_data_15min_counts`:' || E'\n'	|| 
	string_agg(
		CASE
			WHEN source_table = '15m_counts'
			THEN format('`WHERE site_id=%s AND datetime_bin <@ daterange(%s, %s)`', site_id, min_datetime::date, max_datetime::date)
		END,
    	E'\n'
	)

	|| E'\n\n'

	'Rows from `ecocounter.open_data_daily_counts`:' || E'\n'	|| 
	string_agg(
		CASE
			WHEN source_table = 'daily_counts'
			THEN format('`WHERE site_id=%s AND dt <@ daterange('%s', %s)`', site_id, min_datetime::date, max_datetime::date)
		END,
    	E'\n'
	)
	
	 AS error_msg

FROM anomalous_counts

$BODY$;

GRANT SELECT ON TABLE ecocounter.anomalous_ranges TO ecocounter_bot;

ALTER FUNCTION ecocounter.open_data_anomalous_ranges_check() OWNER TO ecocounter_admins;

GRANT EXECUTE ON FUNCTION ecocounter.open_data_anomalous_ranges_check() TO ecocounter_admins;
GRANT EXECUTE ON FUNCTION ecocounter.open_data_anomalous_ranges_check() TO ecocounter_bot;
REVOKE EXECUTE ON FUNCTION ecocounter.open_data_anomalous_ranges_check() FROM bdit_humans;

COMMENT ON FUNCTION ecocounter.open_data_anomalous_ranges_check() IS
'Function which compares `ecocounter.anomalous_ranges` against `ecocounter.open_data_15min_counts` and `ecocounter.open_data_daily_counts`. '
'Does not insert or delete data.';

