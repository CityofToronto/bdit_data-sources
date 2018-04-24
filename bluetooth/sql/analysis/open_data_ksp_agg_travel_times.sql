
DROP VIEW open_data.ksp_agg_travel_time ;
CREATE VIEW open_data.ksp_agg_travel_time AS

SELECT to_char(date_trunc('month', dt), 'Mon ''YY') AS "month", street, direction,
from_intersection, to_intersection,
  day_type,
  period || ' ' || period_range AS time_period,
  round(baseline.tt, 1)  as baseline_travel_time,
  round(AVG(pilot.tt), 1) AS average_travel_time
  FROM king_pilot.dash_daily_dev pilot
  INNER JOIN king_pilot.dash_baseline baseline USING (street, direction, day_type, period)
  WHERE category = 'Pilot' AND dt < date_trunc('month', current_date)
  GROUP BY (to_char(date_trunc('month'::text, pilot.dt::timestamp with time zone), 'Mon ''YY'::text)), date_trunc('month', dt), pilot.street, baseline.from_intersection, baseline.to_intersection, pilot.direction, pilot.day_type, ((pilot.period || ' '::text) || baseline.period_range), baseline.tt
  ORDER BY date_trunc('month', dt);
  
GRANT SELECT ON TABLE open_data.ksp_agg_travel_time TO od_extract_svc;
GRANT SELECT ON TABLE open_data.ksp_agg_travel_time TO bdit_humans;
