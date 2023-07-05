VDS data is pulled daily from ITS Central database on terminal server by dag: /bdit_data-sources/dags/vds_pull.py

## vds.raw_vdsdata
This table contains parsed data from ITSC public.vdsdata. 
Volumes are in vehicles per hour for the 20 sec bin. To convert to 15 minute volume, group by datetime_15min and take `SUM(volume_veh_per_hr) / 4 / 45` where / 4 represents hourly to 15 min conversion and / 45 represents number of 20 sec bins in a 15 minute period. This assumes both missing bins and zero values are zeros, in line with old pipeline. 
This table retains zero bins to enable potential future differente treatment of missing and zero values. 

Row count: 1,203,083 (7 days)
| column_name       | data_type                   | sample              | description   |
|:------------------|:----------------------------|:--------------------|:--------------|
| division_id       | smallint                    | 2                   |               |
| vds_id            | integer                     | 2000410             |               |
| datetime_20sec    | timestamp without time zone | 2023-06-29 00:01:02 |               |
| datetime_15min    | timestamp without time zone | 2023-06-29 00:00:00 |               |
| lane              | integer                     | 1                   |               |
| speed_kmh         | double precision            | 99.5                |               |
| volume_veh_per_hr | integer                     | 1800                |               |
| occupancy_percent | double precision            | 10.31               |               |

## vds.raw_vdsvehicledata
This table contains individual vehicle detections from ITSC public.vdsvehicledata. 
This data can be useful to identify highway speeds and vehicle type mix (from length column).

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

## vds.volumes_15min
A summary of volumes from vds.raw_vdsdata. 
Summary assumes that null values are zeroes (in line with assumption made in old RESCU pipeline).

Row count: 633,448 (7 days)
| column_name   | data_type                   | sample              | description   |
|:--------------|:----------------------------|:--------------------|:--------------|
| volume_uid    | bigint                      | 2888803             |               |
| detector_id   | text                        | DW0161DEG           |               |
| division_id   | smallint                    | 2                   |               |
| vds_id        | integer                     | 2000381             |               |
| datetime_bin  | timestamp without time zone | 2023-06-29 00:00:00 |               |
| volume_15min  | integer                     | 632                 |               |

## vds.vdsconfig
This table is a copy of ITSC public.vdsconfig which contains details about vehicle detectors. 

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

## vds.entity_locations
This table is a copy of ITSC public.entitylocations which contains locations for vehicle detectors. 
Join on entity_locations.entity_id = vdsconfig.vdsid and DISTINCT ON (entity_id), ORDER BY DESC location_timestamp. 

Row count: 0
| column_name                    | data_type                   | description   |
|:-------------------------------|:----------------------------|:--------------|
| division_id                    | smallint                    |               |
| entity_type                    | smallint                    |               |
| entity_id                      | integer                     |               |
| location_timestamp             | timestamp without time zone |               |
| latitude                       | double precision            |               |
| longitude                      | double precision            |               |
| altitude_meters_asl            | double precision            |               |
| heading_degrees                | double precision            |               |
| speed_kmh                      | double precision            |               |
| num_satellites                 | integer                     |               |
| dilution_of_precision          | double precision            |               |
| main_road_id                   | integer                     |               |
| cross_road_id                  | integer                     |               |
| second_cross_road_id           | integer                     |               |
| main_road_name                 | character varying           |               |
| cross_road_name                | character varying           |               |
| second_cross_road_name         | character varying           |               |
| street_number                  | character varying           |               |
| offset_distance_meters         | double precision            |               |
| offset_direction_degrees       | double precision            |               |
| location_source                | smallint                    |               |
| location_description_overwrite | character varying           |               |

## vds.raw_vdsdata_div8001
A sample of 2 days of vdsdata for sensors from division_id 8001. The data is mostly blank rows and may not be of any utility. vdsdata is now filtered to only division_id = 2. 

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

