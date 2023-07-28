# Vehicle Detector System (VDS) data  

# Table of contents
1. [Introduction](#introduction)
2. [Table Structure](#table-structure)
    1. [vds.raw_vdsdata](#vdsraw_vdsdata)
    2. [vds.raw_vdsvehicledata](#vdsraw_vdsvehicledata)
    3. [vds.counts_15min](#vdscounts_15min)
    4. [vds.counts_15min_bylane](#vdscounts_15min_bylane)
    5. [vds.vdsconfig](#vdsvdsconfig)
    6. [vds.entity_locations](#vdsentity_locations)
    7. [vds.veh_speeds_15min](#vdsveh_speeds_15min)
    8. [vds.veh_length_15min](#vdsveh_length_15min)
    9. [vds.raw_vdsdata_div8001](#vdsraw_vdsdata_div8001)
3. [DAG Design](#dag-design)
    1. [vds_pull_vdsdata](#vds_pull_vdsdata)
    2. [vds_pull_vdsvehicledata](#vds_pull_vdsvehicledata)
    3. [vds_monitor](#vds_monitor)

# Introduction  
VDS data is pulled daily using by the Airflow DAGs described in [DAG Design](#dag-design) from ITS Central database. The dags need to be run on-prem to access ITSC. 
VDS system consists of:  
    division_id=2:  
        -RESCU loop/radar detectors  
        -Blue City VDS  
        -SmartCity sensors
    division_id=8001: (Only 1 day sample pulled)  
        -Intersection signal detectors + Special Detectors (Special Function and Preemption detectors)  

# Table Structure  
## vds.raw_vdsdata
This table contains parsed data from ITSC public.vdsdata. 
Volumes are in vehicles per hour for the 20 sec bin. To convert to 15 minute volume, group by datetime_15min and take `SUM(volume_veh_per_hr) / 4 / 45` where / 4 represents hourly to 15 min conversion and / 45 represents number of 20 sec bins in a 15 minute period. This assumes both missing bins and zero values are zeros, in line with old pipeline. 
This table retains zero bins to enable potential future differente treatment of missing and zero values. 
Contains only division_id = 2. A sample of data for division_id = 8001 is stored in `vds.raw_vdsdata_div8001` for future investigation. 

Row count: 1,203,083 (7 days)
| column_name       | data_type                   | sample              | description   |
|:------------------|:----------------------------|:--------------------|:--------------|
| volume_uid        | bigint                      | 105906844           | pkey          |
| division_id       | smallint                    | 2                   |               |
| vds_id            | integer                     | 2000410             |               |
| dt    | timestamp without time zone | 2023-06-29 00:01:02 | Timestamp of record. Not always 20sec increments, depending on sensor. |
| datetime_15min    | timestamp without time zone | 2023-06-29 00:00:00 | Floored to 15 minute bins. |
| lane              | integer                     | 1                   |               |
| speed_kmh         | double precision            | 99.5                | Average speed during bin? |
| volume_veh_per_hr | integer                     | 1800                | In vehicles per hour, need to convert to get # vehicles. |
| occupancy_percent | double precision            | 10.31               | % of time the sensor is occupied. Goes up with congestion (higher vehicle density). |

## vds.raw_vdsvehicledata
This table contains individual vehicle detections from ITSC public.vdsvehicledata. 
This data can be useful to identify highway speeds and vehicle type mix (from length column).
Note these observations do not align exactly with the binned data.

Row count: 1,148,765 (7 days)
| column_name         | data_type                   | sample                     | description   |
|:--------------------|:----------------------------|:---------------------------|:--------------|
| volume_uid          | bigint                      | 244672315                  | pkey          |
| division_id         | smallint                    | 2                          |               |
| vds_id              | integer                     | 5059333                    |               |
| dt                  | timestamp without time zone | 2023-06-28 00:00:03.378718 |               |
| lane                | integer                     | 1                          |               |
| sensor_occupancy_ds | smallint                    | 104                        |               |
| speed_kmh           | double precision            | 15.0                       |               |
| length_meter        | double precision            | 4.0                        |               |

## vds.counts_15min
A summary of 15 minute vehicle counts from vds.raw_vdsdata. 
Summary assumes that null values are zeroes (in line with assumption made in old RESCU pipeline).

Data quality checks:
-- You can compare `num_obs` to `expected_bins * num_lanes`. Consider using a threshold.
-- There should be a total of 96 15 minute datetime bins per day.  
-- Check `num_distinct_lanes = num_lanes` to see if data from all lanes is present in bin.  

Row count: 927,399
| column_name        | data_type                   | sample              | description   |
|:-------------------|:----------------------------|:--------------------|:--------------|
| volumeuid          | bigint                      | 2409198             | pkey          |
| detector_id        | text                        | DE0040DWG           |               |
| division_id        | smallint                    | 2                   |               |
| vds_id             | integer                     | 3                   |               |
| num_lanes          | smallint                    | 4                   | Number of lanes according to sensor inventory. |
| datetime_bin       | timestamp without time zone | 2023-07-17 00:00:00 | Timestamps are floored and grouped into 15 minute bins. For 20s bins it doesn't make a big difference flooring vs. rounding, however for 15 minute sensor data (some of the Yonge St sensors), you may want to pay close attention to this and consider for example if bin timestamp represents start or end of 15 minute period. |
| count_15min       | smallint                    | 217                 |               |
| expected_bins      | smallint                    | 45                  | Expected bins per lane in a 15 minute period |
| num_obs            | smallint                    | 84                  | Number of actual observations in a 15 minute period. Shouldn't be larger than num_lanes * expected_bins. |
| num_distinct_lanes | smallint                    | 4                   | Number of distinct lanes present in this bin. |


## vds.counts_15min_bylane
Same as above but by lane. 
Will be used to determine when individual lane sensors are down. 

Data quality checks: 
-- You can compare `num_obs` to `expected_bins`. Consider using a threshold.
-- There should be a total of 96 15 minute datetime bins per day.  

Row count: 1,712,401
| column_name   | data_type                   | sample              | description   |
|:--------------|:----------------------------|:--------------------|:--------------|
| volumeuid     | bigint                      | 2228148             |               |
| detector_id   | text                        | DE0040DWG           |               |
| division_id   | smallint                    | 2                   |               |
| vds_id        | integer                     | 3                   |               |
| lane          | smallint                    | 1                   |               |
| datetime_bin  | timestamp without time zone | 2023-06-07 00:00:00 |               |
| count_15min  | smallint                    | 8                   |               |
| expected_bins | smallint                    | 45                  |               |
| num_obs       | smallint                    | 45                  |               |

Across all detectors, here is the percentage of volume by lane, grouped by road width. 
This summary does not account for differences in data availability between lanes.  

Road width | Lane 1	| Lane 2 | Lane 3 |	Lane 4 | Lane 5 |
|:---------|:-------|:-------|:-------|:-------|:-------|
2 Lanes |	46.2%	| 53.8% |          |        | 		|	
3 Lanes |	34.7%	| 36.2% | 	29.1%  |	    |       |
4 Lanes |	29.0%	| 33.3% | 	24.8%  | 12.9%	|       |
5 Lanes |	24.0%	| 28.5% | 	22.7%  | 19.9%	| 4.9%  |


## vds.vdsconfig
This table contains details about vehicle detectors from ITSC public.vdsconfig. 
A number of different sensor types are contained in this table: 
RESCU Loop detectors: `detector_id LIKE 'D%' AND division_id = 2`
Signal "Detectors" (DET): `detector_id SIMILAR TO 'PX[1-9]{4}-DET%'`
Signal "Special Function" (SF) detectors: `detector_id SIMILAR TO 'PX[1-9]{4}-SF%'`
Signal "Preemption" (PE) detectors: `detector_id SIMILAR TO 'PX[1-9]{4}-PE%'`
Blue City AI VDS: `detector_id LIKE 'BCT%'`
Smartmicro Sensors: `detector_id LIKE ANY ('{"YONGE & DAVENPORT SMARTMICRO%", "YONGE HEATH%", "YONGE DAVISVILLE%", "%YONGE AND ROXBOROUGH%"}')`

Row count: 10,219
| column_name        | data_type                   | sample                 | description   |
|:-------------------|:----------------------------|:-----------------------|:--------------|
| division_id        | smallint                    | 8001                   |               |
| vds_id             | integer                     | 5462004                |               |
| detector_id        | character varying           | PX1408-DET019          |               |
| start_timestamp    | timestamp without time zone | 2022-09-26 09:04:41    |               |
| end_timestamp      | timestamp without time zone |                        |               |
| lanes              | smallint                    | 1                      |               |
| has_gps_unit       | boolean                     | False                  |               |
| management_url     | character varying           |                        |               |
| description        | character varying           |                        |               |
| fss_division_id    | integer                     |                        |               |
| fss_id             | integer                     |                        |               |
| rtms_from_zone     | integer                     | 1                      |               |
| rtms_to_zone       | integer                     | 1                      |               |
| detector_type      | smallint                    | 1                      |               |
| created_by         | character varying           | TorontoSpatDataGateway |               |
| created_by_staffid | uuid                        |                        |               |
| signal_id          | integer                     | 2005518                |               |
| signal_division_id | smallint                    | 8001                   |               |
| movement           | smallint                    |                        |               |
| uid                | integer                     | 1                      | pkey          |

## vds.entity_locations
This table contains locations for vehicle detectors from ITSC public.entitylocations.
To get the current location, join on entity_locations.entity_id = vdsconfig.vdsid and `SELECT DISTINCT ON (entity_id) ... ORDER BY entity_id, location_timestamp DESC`. 

Row count: 16,013
| column_name                    | data_type                   | sample                                     | description   |
|:-------------------------------|:----------------------------|:-------------------------------------------|:--------------|
| division_id                    | smallint                    | 8001                                       |               |
| entity_type                    | smallint                    | 5                                          |               |
| entity_id                      | integer                     | 2004114                                    |               |
| location_timestamp             | timestamp without time zone | 2021-07-04 22:05:28.957568                 |               |
| latitude                       | double precision            | 43.64945                                   |               |
| longitude                      | double precision            | -79.371464                                 |               |
| altitude_meters_asl            | double precision            |                                            |               |
| heading_degrees                | double precision            |                                            |               |
| speed_kmh                      | double precision            |                                            |               |
| num_satellites                 | integer                     |                                            |               |
| dilution_of_precision          | double precision            |                                            |               |
| main_road_id                   | integer                     | 3741                                       |               |
| cross_road_id                  | integer                     | 3471                                       |               |
| second_cross_road_id           | integer                     | 3471                                       |               |
| main_road_name                 | character varying           | Jarvis St                                  |               |
| cross_road_name                | character varying           | Front St E                                 |               |
| second_cross_road_name         | character varying           | Front St E                                 |               |
| street_number                  | character varying           |                                            |               |
| offset_distance_meters         | double precision            |                                            |               |
| offset_direction_degrees       | double precision            |                                            |               |
| location_source                | smallint                    | 4                                          |               |
| location_description_overwrite | character varying           | JARVIS ST and FRONT ST E / LOWER JARVIS ST |               |
| uid                            | integer                     | 1                                          |               |

## vds.veh_speeds_15min
Summarization of vdsvehicledata with count of observation (vehicle) speeds grouped by 15 min / 5kph / vds_id. 

Data quality: 
--There is a suspicious volume of very high speeds reported. 
--There are null speed values which are excluded from total_count. 

Row count: 6,415,490
| column_name    | data_type                   | sample              | description   |
|:---------------|:----------------------------|:--------------------|:--------------|
| division_id    | smallint                    | 2                   |               |
| vds_id         | integer                     | 3                   |               |
| datetime_15min | timestamp without time zone | 2023-06-13 00:00:00 |               |
| speed_5kph     | smallint                    | 0                   | 5km/h speed bins, rounded down. |
| count          | smallint                    | 14                  |               |
| total_count    | smallint                    | 25                  | Use count::numeric/total_count to get proportion. |
| uid            | bigint                      | 6774601             |               |

## vds.veh_length_15min
Summarization of vdsvehicledata with count of observation (vehicle) lengths grouped by 15 min / 1m length / vds_id. 

Data quality: 
--There is a suspicious volume of very long vehicles. 
--There are null length values which are excluded from total_count. 

Row count: 4,622,437
| column_name    | data_type                   | sample              | description   |
|:---------------|:----------------------------|:--------------------|:--------------|
| division_id    | smallint                    | 2                   |               |
| vds_id         | integer                     | 3                   |               |
| datetime_15min | timestamp without time zone | 2023-06-13 00:00:00 |               |
| length_meter   | smallint                    | 0                   | 1m length bins, rounded down. |
| count          | smallint                    | 3                   |               |
| total_count    | smallint                    | 5                   | Use count::numeric/total_count to get proportion. |
| uid            | bigint                      | 4866932             |               |


## vds.raw_vdsdata_div8001
A sample of 1 day of vdsdata for sensors from division_id 8001. The data is mostly blank rows and may not be of any utility. Main `vds.raw_vdsdata` table is now filtered to only division_id = 2. 
These types of sensors include intersection "detectors" (DET), preemption (PE) (transit /  fire emergency services), special function (SF), which you can identify through the detector_id (example below).

Row count: 601,460
| column_name       | data_type                   | sample              | description   |
|:------------------|:----------------------------|:--------------------|:--------------|
| division_id       | smallint                    | 8001                |               |
| vds_id            | integer                     | 3437088             |               |
| dt    | timestamp without time zone | 2023-06-28 00:00:02 |               |
| datetime_15min    | timestamp without time zone | 2023-06-28 00:00:00 |               |
| lane              | integer                     | 1                   |               |
| speed_kmh         | double precision            |                     |               |
| volume_veh_per_hr | integer                     | 0                   |               |
| occupancy_percent | double precision            | 0.0                 |               |

### Special Function and Preemption detectors (Division 8001): 
A sample query exploring this data:
```
WITH volumes AS (
    SELECT
        v.vds_id, 
        c.detector_id,
        SUM(volume_veh_per_hr) / 4 / 1 AS daily_volume,
            -- / 4 to convert hourly to 15 minute count.
            -- / 1 since there is only 1 bin per 15 minute period. 
        CASE
            WHEN c.detector_id SIMILAR TO 'PX[0-9]{4}-DET%' THEN 'Detector'
            WHEN c.detector_id SIMILAR TO 'PX[0-9]{4}-SF%' THEN 'Special Function'
            WHEN c.detector_id SIMILAR TO 'PX[0-9]{4}-PE%' THEN 'Preemption'
        END as type
    FROM vds.raw_vdsdata_div8001 AS v
    LEFT JOIN
        vds.vdsconfig AS c ON v.vds_id = c.vds_id
        AND c.start_timestamp <= v.dt
        AND (
            c.end_timestamp > v.dt
            OR c.end_timestamp IS NULL)
    WHERE c.detector_id IS NOT NULL 
    GROUP BY
        v.vds_id,
        c.detector_id
)

SELECT type, count(*), min(daily_volume), avg(daily_volume), median(daily_volume), max(daily_volume), sum(daily_volume)
FROM volumes
GROUP BY type 
```

A total of 28 "special function" detections and 31 "preemption" detections seem dubious. 
The regular detectors (DET) may have some utility but it is hard to tell with the zeros (more than half of all records).
| "type"             | "count" | "min" | "avg"                  | "median"               | "max" | "sum"   |
|--------------------|---------|-------|------------------------|------------------------|-------|---------|
| "Special Function" | 435     | 0     | 0.06436781609195402299 | 0.00000000000000000000 | 3     | 28      |
| "Detector"         | 9226    | 0     | 326.0718621287665294   | 12.0000000000000000    | 6156  | 3008339 |
| "Preemption"       | 78      | 0     | 0.39743589743589743590 | 0.00000000000000000000 | 16    | 31      |

And binning the daily counts we find:
-5028 sensors reported 0 volume. More than half of all sensors!
-3886 sensors reported between (0, 1000] volume. 
-825 sensors reported between (1000, 4000] volume. Would expect more in this range, and would expect them to follow more obvious spatial patterns. 
-8 sensors reported > 4000 volume. 

You can also explore spatially, but there are many overlapping points and zero values which make it hard to draw conclusions:
```
WITH volumes AS (
    SELECT
        vds_id, 
        SUM(volume_veh_per_hr) / 4 / 1 AS daily_volume
            -- / 4 to convert hourly to 15 minute count.
            -- / 1 since there is only 1 bin per 15 minute period. 
    FROM vds.raw_vdsdata_div8001
    GROUP BY vds_id
)

SELECT DISTINCT ON (v.vds_id)
    v.vds_id, 
    c.detector_id,
    st_makepoint(e.longitude, e.latitude) as geom, 
    v.daily_volume
FROM volumes AS v
LEFT JOIN vds.vdsconfig AS c ON v.vds_id = c.vds_id
LEFT JOIN vds.entity_locations AS e ON e.entity_id = c.vds_id
ORDER BY v.vds_id, e.location_timestamp DESC
```

# DAG Design 

## [vds_pull_vdsdata](../../../dags/vds_pull_vdsdata.py)

**pull_vdsdata**  
    [*delete_vdsdata* >> *pull_raw_vdsdata*]  

Deletes data from RDS `vds.raw_vdsdata` for specific date and then pulls into RDS from ITS Central database table `vdsdata`. 

**pull_vdsdata** >> **summarize_v15**  
    [*delete_v15* >> *summarize_v15*]  
    [*delete_v15_bylane* >> *summarize_v15_bylane*]

First deletes any existing data then inserts summaries of `vds.raw_vdsdata` into `vds.counts_15min` and `vds.counts_15min_bylane`. 

**update_inventories**  
    *pull_and_insert_detector_inventory*  
    *pull_and_insert_entitylocations*

Pulls entire detector inventory (`vdsconfig`, `entitylocations` tables) into RDS daily. Only runs these tasks for most recent schedule interval (doesn't backfill). 

## [vds_pull_vdsvehicledata](../../../dags/vds_pull_vdsvehicledata.py)

**pull_vdsvehicledata**  
    *delete_vdsvehicledata* >> *pull_raw_vdsvehicledata*  

Deletes data from RDS `vds.raw_vdsvehicledata` for specific date and then pulls into RDS from ITS Central database. 

**summarize_vdsvehicledata**  
    [*delete_veh_speed_data* >> *summarize_speeds*]  
    [*delete_veh_length_data* >> *summarize_lengths*]  

Deletes data from RDS `vds.veh_length_15min` and `vds.veh_speeds_15min` for specific date and then summarizes into these tables from RDS `vds.raw_vdsvehicledata`. 

## [vds_monitor](../../../dags/vds_monitor.py)

**monitor_late_vdsdata**  
    *monitor_vdsdata* >> [*no_backfill*, *clear_[0-N]*]

Checks row count for `vdsdata` in ITS Central vs. row count in `raw_vdsdata` in RDS. If new rows exceed a threshold (currently 1%), re-triggers dag runs for those days using TriggerDagRunOperator.  

**monitor_late_vdsvehicledata**  
    *monitor_vdsvehicledata* >> [*no_backfill*, *clear_[0-N]*]

Checks row count for `vdsvehicledata` in ITS Central vs. row count in `raw_vdsvehicledata` in RDS. If new rows exceed a threshold (currently 1%), re-triggers dag runs for those days using TriggerDagRunOperator.  

Currently scheduled @monthly with 60 day lookback. Set `lookback_days` under task group `monitor_row_count` in vds_monitor.py. 