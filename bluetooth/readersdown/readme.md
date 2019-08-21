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