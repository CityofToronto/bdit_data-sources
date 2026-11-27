CREATE OR REPLACE FUNCTION ecocounter.identify_data_spikes()
RETURNS TABLE (
	flow_id int, 
	daily_volume float, 
	mean_volume float, 
	flow_std float,
	anomaly text,
	date_bin timestamp
)
LANGUAGE sql
COST 100
VOLATILE

AS $BODY$

--- outliers
WITH daily_flows AS (
	SELECT
		datetime_bin::date AS date_bin,
		flow_id,
		SUM(calibrated_volume) AS daily_volume
	FROM ecocounter.counts
		WHERE datetime_bin >= now() - interval '2 months'
	GROUP BY (1, flow_id)
),
mean_flows AS (
	SELECT
		flow_id,
		AVG(daily_volume) AS mean_volume,
		STDDEV(daily_volume) AS flow_std
	FROM daily_flows
	GROUP BY (flow_id)
)
SELECT 
	flow_id, 
	daily_volume, 
	mean_volume, 
	flow_std,
	'data_spike' AS anomaly,
	date_bin
	FROM daily_flows AS df
	JOIN mean_flows AS mf
		USING (flow_id)
	--- Identify outliers as values with a difference greater than 3 standard deviations from the mean
	WHERE (daily_volume > mean_volume + (flow_std * 3)
		OR daily_volume < mean_volume - (flow_std * 3))
	--- Remove low volumes which should not be considered spikes
		AND ABS(mean_volume - daily_volume) >= 100
	--- Zero volumes are for zero volumes (let's make it exclusive to the second table)
		AND daily_volume > 0
--- 0 volumes
UNION ALL
	SELECT 
		flow_id, 
		daily_volume,
		mean_volume, 
		flow_std,
		'0_volume' AS anomaly,
		date_bin
		FROM daily_flows AS df
		JOIN mean_flows AS mf
			USING (flow_id)
		--- Identify 0 daily volumes
		WHERE daily_volume = 0
		--- 0s could naturally occur at lower volume sites, so we set the lower bound at 50
		AND mean_volume > 50;

$BODY$;

ALTER FUNCTION ecocounter.identify_data_spikes() OWNER TO ecocounter_admins;
GRANT ALL ON FUNCTION ecocounter.identify_data_spikes() TO ecocounter_admins;

GRANT EXECUTE ON FUNCTION ecocounter.identify_data_spikes() TO bdit_humans;
GRANT EXECUTE ON FUNCTION ecocounter.identify_data_spikes() TO ecocounter_bot;

COMMENT ON FUNCTION ecocounter.identify_data_spikes()
IS 'A function to identify spikes or potentially anomalous 0-volumes in Ecocounter data by flow_id and day.';
