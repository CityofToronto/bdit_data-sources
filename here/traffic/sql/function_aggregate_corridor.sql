-- This function takes in date range, time range, corridor definition defined by the user, 
-- and aggregates HERE data using the congestion daily segment tables. 
-- Returning a table containing corridor level period data for each analysis period. 
-- e.g. Yonge street from Eglinton to St Clair from November to December 2022 (analysis period) 
-- for each peak periods (AM Peak, PM Peak).

-- User will need to define date range, time range, and corridor in a standardize format prior to using this function, 
-- there are no default values. 

CREATE OR REPLACE FUNCTION here.aggregate_corridor(_selected_time_range text, 
												  _selected_segments text, 
												  _analysis_name text,
												  _s_date date,
												  _e_date date,
												  _no_holiday boolean DEFAULT true)
    RETURNS TABLE(
		corridor_id integer,
		corridor text,
		from_street text,
		to_street text,
		direction text,
		cor_length numeric,
		analysis_name text,
		period_name text,
		average_tt_min numeric,
		minimum_tt_min numeric,
		maximum_tt_min numeric,
		median_tt_min numeric,
		stddev_tt_min numeric,
		pct_15_tt numeric,
		pct_85_tt numeric,
		average_speed_kph numeric,
		minimum_speed_kph numeric,
		maximum_speed_kph numeric,
		median_speed_kph numeric,
		days_with_data integer)
		
    LANGUAGE 'plpgsql'
    COST 100
    VOLATILE STRICT SECURITY DEFINER PARALLEL UNSAFE
AS $BODY$


BEGIN
	IF _no_holiday THEN
	
		RETURN QUERY EXECUTE format($$
			WITH corridor_hourly_daily_agg AS (

			SELECT 
				corridor_id,
				corridor,
				from_street,
				to_street,
				direction,
				cor_length,
				daily_data.dt,
				hr,
				'%s'::Text AS analysis_name,
				period_name,
				round(sum(unadjusted_tt) * ((cor_length/sum(length_w_data))), 2) as corr_hourly_daily_tt_adjusted

			FROM %I
			JOIN congestion.network_segments_daily daily_data USING (segment_id)
			INNER JOIN %I ON daily_data.hr <@ selected_time_range.time_range  
				AND EXTRACT(dow from daily_data.dt)::integer <@ selected_time_range.dow_range 
			LEFT JOIN ref.holiday holiday ON daily_data.dt = holiday.dt
			WHERE holiday.dt IS NULL -- exclude holiday
				AND daily_data.dt >= '%s'::date 
				AND daily_data.dt < '%s'::date

			GROUP BY 
				corridor_id, corridor, from_street, to_street, direction, num_seg, cor_length,
				daily_data.dt, hr, analysis_name, period_name

			HAVING sum(length_w_data)/cor_length >= 0.8 ) -- where 80/100 of corridor length has data

			-- corridor_period_daily_avg_tt: 
			-- Generates the daily period level travel time for each corridor defined from corridor level hourly data 
			, corridor_period_daily_avg_tt as (
			SELECT
				corridor_id,
				corridor,
				from_street,
				to_street,
				direction,
				cor_length,
				dt,
				analysis_name,
				period_name,
				avg(corr_hourly_daily_tt_adjusted) as avg_corr_period_daily_tt

			FROM corridor_hourly_daily_agg
			GROUP BY corridor_id,corridor,from_street,to_street,direction,cor_length,dt,analysis_name,period_name)

			-- Final output: 
			-- Generates the period level travel time for each corridor defined 	
			SELECT 
				corridor_id,
				corridor,
				from_street,
				to_street,
				direction,
				cor_length,
				analysis_name,
				period_name,
				round(avg(avg_corr_period_daily_tt)/60,1) AS average_tt_min,
				round(min(avg_corr_period_daily_tt)/60,1) AS minimum_tt_min,
				round(max(avg_corr_period_daily_tt)/60,1) AS maximum_tt_min,
				round(median(avg_corr_period_daily_tt)/60,1) AS median_tt_min,
				round(stddev(avg_corr_period_daily_tt)/60,1) AS stddev_tt_min,
				round(percentile_cont(0.15) WITHIN GROUP (ORDER BY avg_corr_period_daily_tt)::numeric/60,1) AS pct_15_tt,
				round(percentile_cont(0.85) WITHIN GROUP (ORDER BY avg_corr_period_daily_tt)::numeric/60,1) AS pct_85tt,
				round((cor_length/1000)/(avg(avg_corr_period_daily_tt)/3600),0) AS average_speed_kph,
				round((cor_length/1000)/(min(avg_corr_period_daily_tt)/3600),0) AS minimum_speed_kph,
				round((cor_length/1000)/(max(avg_corr_period_daily_tt)/3600),0) AS maximum_speed_kph,
				round((cor_length/1000)/(median(avg_corr_period_daily_tt)/3600),0) AS median_speed_kph,
				count(1)::int AS days_with_data

			FROM corridor_period_daily_avg_tt 
			GROUP BY corridor_id, corridor, from_street, to_street, direction, cor_length, analysis_name, period_name
			ORDER by corridor_id, analysis_name;$$, 
						   _analysis_name, _selected_segments, _selected_time_range, _s_date, _e_date);
	ELSE
		RETURN QUERY EXECUTE format($$
			WITH corridor_hourly_daily_agg AS (

				SELECT 
					corridor_id,
					corridor,
					from_street,
					to_street,
					direction,
					cor_length,
					daily_data.dt,
					hr,
					'%s'::Text AS analysis_name,
					period_name,
					round(sum(unadjusted_tt) * ((cor_length/sum(length_w_data))), 2) as corr_hourly_daily_tt_adjusted

				FROM %I
				JOIN congestion.network_segments_daily daily_data USING (segment_id)
				INNER JOIN %I ON daily_data.hr <@ selected_time_range.time_range  
					AND EXTRACT(dow from daily_data.dt)::integer <@ selected_time_range.dow_range 
				WHERE daily_data.dt >= '%s'::date AND daily_data.dt < '%s'::date

				GROUP BY 
					corridor_id, corridor, from_street, to_street, direction, num_seg, cor_length,
					daily_data.dt, hr, analysis_name, period_name

				HAVING sum(length_w_data)/cor_length >= 0.8 ) -- where 80/100 of corridor length has data

				-- corridor_period_daily_avg_tt: 
				-- Generates the daily period level travel time for each corridor defined from corridor level hourly data 
				, corridor_period_daily_avg_tt as (
				SELECT
					corridor_id,
					corridor,
					from_street,
					to_street,
					direction,
					cor_length,
					dt,
					analysis_name,
					period_name,
					avg(corr_hourly_daily_tt_adjusted) as avg_corr_period_daily_tt

				FROM corridor_hourly_daily_agg
				GROUP BY corridor_id,corridor,from_street,to_street,direction,cor_length,dt,analysis_name,period_name)

				-- Final output: 
				-- Generates the period level travel time for each corridor defined 	
				SELECT 
					corridor_id,
					corridor,
					from_street,
					to_street,
					direction,
					cor_length,
					analysis_name,
					period_name,
					round(avg(avg_corr_period_daily_tt)/60,1) AS average_tt_min,
					round(min(avg_corr_period_daily_tt)/60,1) AS minimum_tt_min,
					round(max(avg_corr_period_daily_tt)/60,1) AS maximum_tt_min,
					round(median(avg_corr_period_daily_tt)/60,1) AS median_tt_min,
					round(stddev(avg_corr_period_daily_tt)/60,1) AS stddev_tt_min,
					round(percentile_cont(0.15) WITHIN GROUP (ORDER BY avg_corr_period_daily_tt)::numeric/60,1) AS pct_15_tt,
					round(percentile_cont(0.85) WITHIN GROUP (ORDER BY avg_corr_period_daily_tt)::numeric/60,1) AS pct_85tt,
					round((cor_length/1000)/(avg(avg_corr_period_daily_tt)/3600),0) AS average_speed_kph,
					round((cor_length/1000)/(min(avg_corr_period_daily_tt)/3600),0) AS minimum_speed_kph,
					round((cor_length/1000)/(max(avg_corr_period_daily_tt)/3600),0) AS maximum_speed_kph,
					round((cor_length/1000)/(median(avg_corr_period_daily_tt)/3600),0) AS median_speed_kph,
					count(1)::int AS days_with_data

				FROM corridor_period_daily_avg_tt 
				GROUP BY corridor_id, corridor, from_street, to_street, direction, cor_length, analysis_name, period_name
				ORDER by corridor_id, analysis_name;$$, 
							   _analysis_name, _selected_segments, _selected_time_range, _s_date, _e_date);
	END IF;
END;
$BODY$;



ALTER FUNCTION here.aggregate_corridor(text, text, text, date, date, boolean)
    OWNER TO here_admins;

GRANT EXECUTE ON FUNCTION here.aggregate_corridor(text, text, text, date, date, boolean) TO bdit_humans;

COMMENT ON FUNCTION here.aggregate_corridor(text, text, text, date, date, boolean)
    IS 'It takes in date range, time range, corridor definition defined by the user, 
	and aggregates HERE data using the congestion daily segment tables. 
	Returning a table containing corridor level period data for each analysis period. 
	e.g. Yonge street from Eglinton to St Clair from November to December 2022 (analysis period) 
	for each peak periods (AM Peak, PM Peak).
	User will need to define date range, time range, and corridor in a standardize format prior to using this function, 
	there are no default values.';
