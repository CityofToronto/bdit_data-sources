
CREATE VIEW open_data.ksp_agg_travel_time AS

SELECT to_char(date_trunc('month', dt), 'Mon ''YY') AS "month", street, direction,
from_intersection, to_intersection,
  day_type,
  period || ' ' || period_range AS time_period,
to_char(baseline.tt, '999.0')  as baseline_travel_time,
  to_char(AVG(pilot.tt), '999.0') AS average_travel_time
  FROM king_pilot.dash_daily_dev pilot
  INNER JOIN king_pilot.dash_baseline baseline USING (street, direction, day_type, period)
  WHERE category = 'Pilot'
  GROUP BY "month", street, from_intersection, to_intersection, direction, day_type, time_period, baseline.tt ;
  
GRANT SELECT ON TABLE open_data.ksp_agg_travel_time TO od_extract_svc;
GRANT SELECT ON TABLE open_data.ksp_agg_travel_time TO bdit_humans;
