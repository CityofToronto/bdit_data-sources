select startpoint_name, endpoint_name, extract(year from measured_timestamp) as yr, extract(month from measured_timestamp) as mth, extract(day from measured_timestamp) as dy, 
	CASE WHEN extract(hour from measured_timestamp) < 12 THEN 0
	ELSE 1
	END as bucket, count(*)
from bluetooth.raw_store
group by startpoint_name, endpoint_name, extract(year from measured_timestamp), extract(month from measured_timestamp), extract(day from measured_timestamp), CASE WHEN extract(hour from measured_timestamp) < 12 THEN 0
	ELSE 1
	END
having count(*) > 9985
order by startpoint_name, endpoint_name, extract(year from measured_timestamp), extract(month from measured_timestamp), extract(day from measured_timestamp), CASE WHEN extract(hour from measured_timestamp) < 12 THEN 0
	ELSE 1
	END
