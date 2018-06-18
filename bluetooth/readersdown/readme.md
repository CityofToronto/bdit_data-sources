# How to use brokenreaders.py
***
The script `brokenreaders.py` finds all of the most recent non-functioning bluetooth readers, and creates a `.csv` file that contains the names of these readers, the time they were last active, in addition to the routes that were affected by these readers. Follow these steps to use this script:

<br>

1. Download and open the script.

<br>

2. Between the two pound sign breaks, there is a variable `cfg_path`. To this variable, assign the file path of your `.cfg` file used for connecting to the Postgres database.

<br>

3. Run the `brokenreaders.py` file in your python environment.

<br>

4. Proceed to your `documents` folder on your computer. You will see a `broken_readers.csv` file. It will contain the table mentioned above. 


# Backfilling `bluetooth.dates_without_data`

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
