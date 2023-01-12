# RESCU - Loop Detectors

Road Emergency Services Communication Unit (RESCU) track traffic volume on expressways using loop detectors. 
More information can be found on the [city's website](https://www.toronto.ca/services-payments/streets-parking-transportation/road-restrictions-closures/rescu-traffic-cameras/) 
or [here](https://en.wikipedia.org/wiki/Road_Emergency_Services_Communications_Unit).

Raw data is available in `rescu.raw_15min` whereas processed 15-min data is available in `rescu.volumes_15min`.

## `rescu_pull.py`

This [script](rescu_pull.py) is located in the terminal server and takes in three variables which are `start_date`, `end_date` (exclusive) and `path`. It is a job that runs on a daily basis in the morning and imports the latest 15-minute volume file from a particular drive into our RDS. By default, the `start_date` and `end_date` are the beginning and the end of the day before (today - 1 day and 12:00 today respectively). However, the user can also specify the date range from which the data should be pulled to ensure that this process can be used for other applications too. This script is automated to run daily on the terminal server to ingest a day worth of data collected from the day before. The steps are as followed:

1) The script reads information from a .rpt file and then inserts the date into a table named `rescu.raw_15min`. The table has the following information

|raw_uid|dt|raw_info|
|--|--|--|
|12852|2020-01-13|1700 - 1715 de0010deg    633|
|12853|2020-01-13|1700 - 1715 de0010der    62|

2) There is also a trigger function named [`rescu.insert_rescu_volumes()`](create-trigger-function-populate_volumes_15min.sql) which processes the newly added data from `rescu.raw_15min` and inserts the processed data into the table `rescu.volumes_15min`. All information from `raw_info` in the raw table is then processed into the 3 columns which are `detector_id`, `datetime_bin` and `volume_15min` whereas `aretrycode` is taken from the table `rescu.detector_inventory` by matching them using `detector_id`. The table `rescu.volumes_15min` has the following information

|volume_uid|detector_id|datetime_bin|volume_15min|arterycode|
|--|--|--|--|--|
|9333459|DE0010DEG|2020-01-13 17:00:00|	749|	23984|
|9333460|DE0010DER|2020-01-13 17:00:00|	80|	3272|

## `check_rescu.py`

Since the terminal server does not have an internet connection, we will not get notified if the process fails. Therefore, we created an Airflow task to do that job for us. There is a dag named [`check_rescu`](/dags/check_rescu.py) which runs at 6am everyday that checks the number of rows inserted for the day before in both tables `rescu.raw_15min` and `rescu.volumes_15min`. If the number of rows is 0 OR if the number of rows from the raw table is less than that in the volumes table OR if the total number of rows from the volumes table is less than 7000 (the average number of rows per day is about 20,000), a Slack notification will be sent to notify the team. The line that does exactly that is shown below.
```python
if raw_num == 0 or raw_num < volume_num or volume_num < 7000:
  raise Exception ('There is a PROBLEM here. There is no raw data OR raw_data is less than volume_15min OR volumes_15min is less than 7000 which is way too low')
```

When the Slack message is sent, we can run the following check to find out what exactly is wrong with the data pipeline. The Airflow dag only shows us the number of rows in the raw and volumes tables but the reason of failing may still be unclear. Therefore, this query can be used to have a better picture on what is happening with that day of data.
```sql
SELECT 
TRIM(SUBSTRING(raw_info, 15, 12)) AS detector_id,
dt + LEFT(raw_info,6)::time AS datetime_bin,
nullif(TRIM(SUBSTRING(raw_info, 27, 10)),'')::int AS volume_15min
FROM rescu.raw_15min 
WHERE dt = '2020-09-03'::date --change to the date that you would like to investigate
AND nullif(TRIM(SUBSTRING(raw_info, 27, 10)),'')::int < 0
```
If the column `volume_15min` is `-1`, that means that there is something wrong with the data from the source end and we have to notify the person in charge as this is not something that we can fix. 
