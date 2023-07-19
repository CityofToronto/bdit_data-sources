# Vehicle Detector System (VDS) data  

VDS data is pulled daily from ITS Central database on terminal server by dags running on Morbius.  
VDS system consists of:  
    Division 2:  
        -RESCU loop detectors  
        -Blue City VDS  
    Division 8001: (Only 1 day sample pulled)  
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
| datetime_20sec    | timestamp without time zone | 2023-06-29 00:01:02 | Timestamp of record. Not always 20sec increments, depending on sensor. |
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
| division_id         | smallint                    | 2                          |               |
| vds_id              | integer                     | 5059333                    |               |
| dt                  | timestamp without time zone | 2023-06-28 00:00:03.378718 |               |
| lane                | integer                     | 1                          |               |
| sensor_occupancy_ds | smallint                    | 104                        |               |
| speed_kmh           | double precision            | 15.0                       |               |
| length_meter        | double precision            | 4.0                        |               |
| volume_uid          | bigint                      | 244672315                  | pkey          |

## vds.volumes_15min
A summary of volumes from vds.raw_vdsdata. 
Summary assumes that null values are zeroes (in line with assumption made in old RESCU pipeline).

Row count: 927,399
| column_name        | data_type                   | sample              | description   |
|:-------------------|:----------------------------|:--------------------|:--------------|
| volumeid           | bigint                      | 2409198             | pkey          |
| detector_id        | text                        | DE0040DWG           |               |
| division_id        | smallint                    | 2                   |               |
| vds_id             | integer                     | 3                   |               |
| num_lanes          | smallint                    | 4                   | Number of lanes according to sensor inventory. |
| datetime_bin       | timestamp without time zone | 2023-07-17 00:00:00 | Timestamps are floored and grouped into 15 minute bins. For 20s bins it doesn't make a big difference flooring vs. rounding, however for 15 minute sensor data (some of the Yonge St sensors), you may want to pay close attention to this and consider for example if bin timestamp represents start or end of 15 minute period. |
| volume_15min       | smallint                    | 217                 |               |
| expected_bins      | smallint                    | 45                  | Expected bins per lane in a 15 minute period |
| num_obs            | smallint                    | 84                  | Number of actual observations in a 15 minute period. Shouldn't be larger than num_lanes * expected_bins. |
| volumeuid          | bigint                      | 2409198             |               |
| num_distinct_lanes | smallint                    | 4                   | Number of distinct lanes present in this bin. |


## vds.volumes_15min_bylane
Same as above but by lane. 
Will be used to determine when individual lane sensors are down. 
Row count: 1,712,401
| column_name   | data_type                   | sample              | description   |
|:--------------|:----------------------------|:--------------------|:--------------|
| volumeuid     | bigint                      | 2228148             |               |
| detector_id   | text                        | DE0040DWG           |               |
| division_id   | smallint                    | 2                   |               |
| vds_id        | integer                     | 3                   |               |
| lane          | smallint                    | 1                   |               |
| datetime_bin  | timestamp without time zone | 2023-06-07 00:00:00 |               |
| volume_15min  | smallint                    | 8                   |               |
| expected_bins | smallint                    | 45                  |               |
| num_obs       | smallint                    | 45                  |               |

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
A sample of 1 day of vdsdata for sensors from division_id 8001. The data is mostly blank rows and may not be of any utility. Main `raw_vdsdata` table is now filtered to only division_id = 2. 

Row count: 601,460
| column_name       | data_type                   | sample              | description   |
|:------------------|:----------------------------|:--------------------|:--------------|
| division_id       | smallint                    | 8001                |               |
| vds_id            | integer                     | 3437088             |               |
| datetime_20sec    | timestamp without time zone | 2023-06-28 00:00:02 |               |
| datetime_15min    | timestamp without time zone | 2023-06-28 00:00:00 |               |
| lane              | integer                     | 1                   |               |
| speed_kmh         | double precision            |                     |               |
| volume_veh_per_hr | integer                     | 0                   |               |
| occupancy_percent | double precision            | 0.0                 |               |

### Special Function and Preemption detectors (Division 8001): 
These types of sensors include transit preemption, fire emergency services preemption. The one day sample we have only contains 22 detections which doesn't seem accurate. Sample query: 
```
SELECT DISTINCT ON (vds_id, datetime_20sec)
    c.detector_id, d.* 
FROM vds.raw_vdsdata_div8001 AS d
LEFT JOIN vds.vdsconfig AS c
    ON d.vds_id = c.vds_id
--WHERE c.detector_id SIMILAR TO 'PX[1-9]{4}-DET%'
WHERE (detector_id SIMILAR TO 'PX[1-9]{4}-SF%' OR detector_id SIMILAR TO 'PX[1-9]{4}-PE%')
    AND (volume_veh_per_hr > 0 OR occupancy_percent > 0)
ORDER BY vds_id, datetime_20sec, c.start_timestamp DESC
LIMIT 1000
```


# Dag Design 

## vds_pull_vdsdata [../../../dags/vds_pull_vdsdata.py]

**pull_vdsdata**  
    [*delete_vdsdata* >> *pull_raw_vdsdata*]  

Deletes data from RDS `vds.raw_vdsdata` for specific date and then pulls into RDS from ITS Central database table `vdsdata`. 

**pull_vdsdata** >> **summarize_v15**  
    [*delete_v15* >> *summarize_v15*]  
    [*delete_v15_bylane* >> *summarize_v15_bylane*]

First deletes any existing data then inserts summaries of `vds.raw_vdsdata` into `vds.volumes_15min` and `vds.volumes_15min_bylane`. 

**update_inventories**  
    *pull_and_insert_detector_inventory*  
    *pull_and_insert_entitylocations*

Pulls entire detector inventory (`vdsconfig`, `entitylocations` tables) into RDS daily. 

## vds_pull_vdsvehicledata [../../../dags/vds_pull_vdsvehicledata.py]

**pull_vdsvehicledata**  
    *delete_vdsvehicledata* >> *pull_raw_vdsvehicledata*  

Deletes data from RDS `vds.raw_vdsvehicledata` for specific date and then pulls into RDS from ITS Central database. 

**summarize_vdsvehicledata**  
    [*delete_veh_speed_data* >> *summarize_speeds*]  
    [*delete_veh_length_data* >> *summarize_lengths*]  

Deletes data from RDS `vds.veh_length_15min` and `vds.veh_speeds_15min` for specific date and then summarizes into these tables from RDS `vds.raw_vdsvehicledata`. 

## vds_monitor [../../../dags/vds_monitor.py]

**monitor_late_vdsdata**  
    *monitor_vdsdata* >> [*no_backfill*, *clear_[0-N]*]

Checks row count for `vdsdata` in ITS Central vs. row count in `raw_vdsdata` in RDS. If new rows exceed a threshold (currently 1%), re-triggers dag runs for those days using TriggerDagRunOperator.  

**monitor_late_vdsvehicledata**  
    *monitor_vdsvehicledata* >> [*no_backfill*, *clear_[0-N]*]

Checks row count for `vdsvehicledata` in ITS Central vs. row count in `raw_vdsvehicledata` in RDS. If new rows exceed a threshold (currently 1%), re-triggers dag runs for those days using TriggerDagRunOperator.  

Currently scheduled @monthly with 60 day lookback. Set `lookback_days` under task group `monitor_row_count` in vds_monitor.py. 