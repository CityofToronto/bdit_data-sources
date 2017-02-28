select startpoint_name, endpoint_name, extract(year from (measured_timestamp - INTERVAL '1 second')) as yr, extract(month from (measured_timestamp - INTERVAL '1 second')) as mth, count(*)
from bluetooth.observations
where outlier_level = 0
group by startpoint_name, endpoint_name, extract(year from (measured_timestamp - INTERVAL '1 second')), extract(month from (measured_timestamp - INTERVAL '1 second'))
order by startpoint_name, endpoint_name, extract(year from (measured_timestamp - INTERVAL '1 second')), extract(month from (measured_timestamp - INTERVAL '1 second'))