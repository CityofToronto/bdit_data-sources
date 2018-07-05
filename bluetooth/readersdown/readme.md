# How to use brokenreaders.py

The script `brokenreaders.py` finds all of the sensors which stopped reporting yesterday and emails a list of them. This script currently runs every day.

This script requires the [`email_notifications`](https://github.com/CityofToronto/bdit_python_utilities/tree/master/email_notifications) python module.

## Backfilling `bluetooth.dates_without_data`

In case something happens when running `brokenreaders.py`, or the blip_api script didn't complete successfully so not all the data went into our database, you can run the below query to insert dates that are missing data for those particular analyses, just change the range of dates.

```sql
INSERT INTO bluetooth.dates_without_data
	SELECT all_analyses.analysis_id, date_with_data
   FROM bluetooth.all_analyses
   CROSS JOIN (SELECT '2018-06-07'::DATE + generate_series(1,'2018-06-17'::DATE - '2018-06-07'::DATE) * INTERVAL '1 Day' date_with_data) d 
   LEFT OUTER JOIN bluetooth.observations obs ON all_analyses.analysis_id=obs.analysis_id AND measured_timestamp::DATE = date_with_data
  WHERE all_analyses.pull_data 
  GROUP BY all_analyses.analysis_id, date_with_data
--   ORDER BY COUNT
  HAVING COUNT(obs.analysis_id) =0
```
