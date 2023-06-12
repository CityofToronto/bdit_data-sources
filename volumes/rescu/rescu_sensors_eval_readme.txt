This PR accomplishes identification of network wide and individual detector outages.

1) **volumes/rescu/create-view-network-outages.sql** - Creates a table of network wide RESCU outages (no values from any sensor over any duration). 
This can be used to find good dates for a data request or to be part of an alert pipeline. 
For example, here is some sample code to start with a list of eligible dates for a request and exclude any date with a network outage of any length:

```
--use case: find dates and times when there were no network outages: 
--there are 34 dates so far in 2023 with no network wide outages. 
WITH list_dates AS (
    SELECT generate_series('2023-01-01', '2023-06-12', '1 day'::interval)::timestamp AS date
)

SELECT l.date 
FROM list_dates AS l
LEFT JOIN gwolofs.network_outages AS nout ON
    nout.date_start <= l.date AND
    nout.date_end >= l.date
WHERE nout.date_start IS NULL
```

2) **volumes/rescu/create-mat-view-individual-outages.sql** - identify individual detector outages. Could be useful for future data requests.
Similar to network outages but for each individual detector. Currently network wide and individual outages overlap due to difficulty of separating overlapping datetime ranges. 

Here is a sample query which finds a list of dates where a list of sensors are all active with no individual outages. 
```
WITH list_dates AS (
    SELECT generate_series('2023-01-01', '2023-05-15', '1 day'::interval)::timestamp AS date
),

detectors AS (
    SELECT detector_id
    FROM rescu.detector_inventory
    WHERE det_group = 'FGG' --all gardiner sensors
) 

SELECT l.date
FROM list_dates AS l
CROSS JOIN detectors AS d
LEFT JOIN gwolofs.rescu_individual_outages AS iout ON
    iout.detector_id = d.detector_id AND
    iout.date_start <= l.date AND
    iout.date_end >= l.date
WHERE iout.time_start IS NULL
GROUP BY 1
HAVING COUNT(*) = (SELECT COUNT(*) FROM detectors)
```

3) **volumes/rescu/Data validation/identify_sensors_to_repair_i0617.sql** - Create a list of detectors classified as good/bad/inactive to map for geographic distribution.
-Uses table 'network_outages' to elimante dates where all detectors are inactive from the denominator of up-time to get more realistic numbers. 
-Create a summary table of detectors with different stats: total_volume, bins_active_percent, bins_active, last_active.
-Includes summary stats over a list of dates for active sensors and 'last active date' for any sensors not active during this period for completeness. 

4) **volumes/rescu/Data validation/identify_detectors_for_repair.png** - visual output from above sql used to determine sensors to repair.

5) **volumes/rescu/validation/evaluate_rescu_network.ipynb**
A short ipynb to explore the results of the above queries. 


