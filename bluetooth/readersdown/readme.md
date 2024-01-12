# How to use brokenreaders.py

The script `brokenreaders.py` finds all of the sensors which stopped reporting yesterday and emails a list of them. This script currently runs every day.

This script requires the [`email_notifications`](https://github.com/CityofToronto/bdit_python_utilities/tree/master/email_notifications) python module.

## Backfilling `bluetooth.dates_without_data`

In case something happens when running `brokenreaders.py`, or the blip_api script didn't complete successfully so not all the data went into our database, you can run the [below functions](update_dates_without_data.sql) to insert dates that are missing data for those particular analyses, just change the range of dates.

```sql
SELECT bluetooth.update_dates_wo_data('2018-08-08'::DATE);
--OR
SELECT bluetooth.update_dates_wo_data('2018-08-08'::DATE, '2018-08-15'::DATE);
```


## Function: detector_status

This function takes the date as an input for example: '2020-10-05'.

It returns three fields:
1. reader_name: Name of the bluetooth reader that is mentioned in the bluetooth.all_analyses table in the route_points field which is in json format.
2. data_last_aggregated: The date when the data was last aggregated in the bluetooth.aggr_5min table.
3. reader_status: The status of the reader whether it is 'active' or 'inactive' in the date input in the function.

## Function: route_status
This function also takes the date as an input for example:  '2020-10-01'.

It returns four fields:
1. an_id: This is the same analysis_id from aggr_5min table.
2. last_received: This is the date when the route received the data from detector and was aggregated in the aggr_5min table.
3. status: Either 'active' or 'inactive' depending on the date of last data aggregation for that particular route.
4. ddown: This is the number of days the route has either not received data from the reader or was not aggregated in the aggr_5min table.

## Function: route_status_details:

This function also takes the date as an input for example:  '2020-10-01'.

It returns the following eight fields:

1. an_id: This is the same analysis_id from aggr_5min table.
2. last_received: This is the date when the route received the data from detector and was aggregated in the aggr_5min table.
3. status: Either 'active' or 'inactive' depending on the date of last data aggregation for that particular route.
4. ddown: This is the number of days the route has either not received data from the reader or was not aggregated in the aggr_5min table.

The remaining four fields are retrieved from route_points field from all_analyses table. This field is a list of dictiorary containing two dictionary items. The first dictionary item has details of the starting point and the second item has details of the ending point.  
5. start_route_point_id: This id is retrieved from the first item in the list with the key = id. 
6. start_detector: start detector name is retrieved from the first item in the list with key = name.
7. end_route_point_id: This id is retrieved from the second item in the list with the key = id.
8. end_detector: end detector name is retrieved from the second item in the list with key = name.

## Function: route_point_status:

This function also takes the date as an input for example:  '2020-10-01'.

It returns the following four fields:
route_pnt: This is the route point id. This id is either the start_route_point_id or end_route_point_id.
reader: This is the given name for the bluetooth reader at that particular route point id.
active_date: This is the date when the last signal is received from the reader. If the date input in the function is lower than this date, input date is retrieved. Meaning that the route point was active as of the input date.
active: this is a bool field. If the route point is sending signal for the requested date, the value is true else false.
  
