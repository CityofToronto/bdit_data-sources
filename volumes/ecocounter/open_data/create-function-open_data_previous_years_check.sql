CREATE OR REPLACE FUNCTION ecocounter.open_data_previous_years_check()
RETURNS TABLE (
    error_msg_flag BOOLEAN,
    error_msg text
)
LANGUAGE sql
COST 100
VOLATILE

AS $BODY$

WITH recent_actions AS (
	SELECT 
		* 
	FROM ecocounter.logged_actions
    --- determine the lookback period
	WHERE action_tstamp_clk >= (CURRENT_DATE - INTERVAL '1 month')::date
),
grouped_summary AS (

    --- sites_unfiltered and flows_unfiltered
	SELECT 
	    COUNT(DISTINCT(la.row_data ->> 'site_id')) AS n_instances,
        --- first active is a reliable start date (not representitive of every effected year)
		ARRAY_AGG(DISTINCT(EXTRACT(YEAR FROM s.first_active)::integer)) AS effected_years,
		la.table_name
	FROM recent_actions AS la
	JOIN ecocounter.sites AS s
		ON la.row_data ->> 'site_id' = s.site_id::text
	WHERE 
        --- filter for instances before our current year
		s.first_active < date_trunc('year', CURRENT_DATE)::date
		AND table_name IN ('sites_unfiltered', 'flows_unfiltered')
		AND la.changed_fields ? 'validated'
	GROUP BY table_name
	
UNION ALL
    --- sensitivity history
	SELECT 
	    COUNT(DISTINCT(row_data ->> 'flow_id')) AS n_instances,
		ARRAY_AGG(DISTINCT(EXTRACT(YEAR FROM lower((row_data ->> 'date_range')::daterange))::int)) AS effected_years,
		table_name
	FROM recent_actions
	WHERE 
        table_name = 'sensitivity_history'
        --- filter for instances before our current year
		AND lower((row_data ->> 'date_range')::daterange) < date_trunc('year', CURRENT_DATE)
	GROUP BY table_name
	
UNION ALL

    --- anomalous_ranges
	SELECT 
		COUNT(DISTINCT(row_data ->> 'site_id')) AS n_instances,
		ARRAY_AGG(DISTINCT(EXTRACT(YEAR FROM lower((row_data ->> 'time_range')::daterange))::int)) AS effected_years,
		table_name
	FROM recent_actions
	WHERE 
		table_name = 'anomalous_ranges'
        --- filter for instances before our current year
        AND lower((row_data ->> 'time_range')::daterange) < date_trunc('year', CURRENT_DATE)
	GROUP BY table_name
)
SELECT
	CASE
        WHEN EXISTS (SELECT 1 FROM grouped_summary)
        THEN False
        ELSE True
	END AS error_msg_flag,

		
	'WARNING: Changes were detected which could effect data from previous years. You may need to run the `ecocounter_open_data` dag for a previous year' || E'\n\n'||
	STRING_AGG(
	FORMAT('%s instances were detected in `%s`. The earliest changes occur in %s', n_instances, table_name, effected_years),
	E'\n'

)

	AS error_msg
	
FROM grouped_summary

$BODY$;


ALTER FUNCTION ecocounter.open_data_previous_years_check() OWNER TO ecocounter_admins;

GRANT SELECT ON TABLE ecocounter.logged_actions TO ecocounter_bot;

GRANT EXECUTE ON FUNCTION ecocounter.open_data_previous_years_check() TO ecocounter_admins;
GRANT EXECUTE ON FUNCTION ecocounter.open_data_previous_years_check() TO ecocounter_bot;
REVOKE EXECUTE ON FUNCTION ecocounter.open_data_previous_years_check() FROM bdit_humans;

COMMENT ON FUNCTION ecocounter.open_data_previous_years_check() IS
'Function which searches `ecocounter.logged_actions` to determine if any recent changes require the ecocounter_open_data DAG '
'to be executed on previous years';