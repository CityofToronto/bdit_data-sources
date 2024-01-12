CREATE OR REPLACE FUNCTION mohan.route_status(
	insert_value date
)
	returns table(
		an_id bigint,
		last_received date,
		status text,
		ddown int
)
	
language plpgsql
	as $$
	
	begin
		return query
			select DISTINCT(analysis_id), MAX (datetime_bin::date),
			CASE
			WHEN  MAX (datetime_bin::date)>= (insert_value-1) then 'active'::text
		else 'inactive'::text
		END 
		AS route_status,
		CASE
		WHEN MAX (datetime_bin::date) >= (insert_value-1) then 0
		else (insert_value - MAX (datetime_bin::date))
		END AS days_down
		from 
		bluetooth.all_analyses
		LEFT JOIN bluetooth.aggr_5min USING (analysis_id) 
		where  bluetooth.all_analyses.pull_data = 'true'
		--datetime_bin = insert_value
		GROUP BY analysis_id;
		end; 

