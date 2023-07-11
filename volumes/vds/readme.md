VDS data is pulled daily from ITS Central database on terminal server by dag: /bdit_data-sources/dags/vds_pull.py

VDS system consists of: 
-RESCU loop detectors
-Blue City VDS
-Intersection signal detectors
-

## vds.raw_vdsdata
This table contains parsed data from ITSC public.vdsdata. 
Volumes are in vehicles per hour for the 20 sec bin. To convert to 15 minute volume, group by datetime_15min and take `SUM(volume_veh_per_hr) / 4 / 45` where / 4 represents hourly to 15 min conversion and / 45 represents number of 20 sec bins in a 15 minute period. This assumes both missing bins and zero values are zeros, in line with old pipeline. 
This table retains zero bins to enable potential future differente treatment of missing and zero values. 
Contains only division_id = 2. A sample of data for division_id = 8001 is stored in `vds.raw_vdsdata_div8001` for future investigation. 

Row count: 1,203,083 (7 days)
| column_name       | data_type                   | sample              | description   |
|:------------------|:----------------------------|:--------------------|:--------------|
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
| datetime_bin  | timestamp without time zone | 2023-06-29 00:00:00 | Timestamps are floored and grouped into 15 minute bins. For 20s bins it doesn't make a big difference flooring vs. rounding, however for 15 minute sensor data (some of the Yonge St sensors), you may want to pay close attention to this and consider for example if bin timestamp represents start or end of 15 minute period. |
| volume_15min  | integer                     | 632                 |               |

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

## vds.entity_locations
This table contains locations for vehicle detectors from ITSC public.entitylocations.
To get the current location, join on entity_locations.entity_id = vdsconfig.vdsid and `SELECT DISTINCT ON (entity_id) ... ORDER BY entity_id, location_timestamp DESC`. 

Row count: 15,930
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

## Special Function and Preemption detectors: 
These types of sensors include transit preemption, fire emergency services preemption. The one day sample we have only contains 22 detections which doesn't seem accurate. Sample query: 
```SELECT DISTINCT ON (vds_id, datetime_20sec)
    c.detector_id, d.* 
FROM vds.raw_vdsdata_div8001 AS d
LEFT JOIN vds.vdsconfig AS c
    ON d.vds_id = c.vds_id
--WHERE c.detector_id SIMILAR TO 'PX[1-9]{4}-DET%'
WHERE (detector_id SIMILAR TO 'PX[1-9]{4}-SF%' OR detector_id SIMILAR TO 'PX[1-9]{4}-PE%')
    AND (volume_veh_per_hr > 0 OR occupancy_percent > 0)
ORDER BY vds_id, datetime_20sec, c.start_timestamp DESC
LIMIT 1000```

Inquire with Simon about backfilling?
Hard to tell if required 

On RDS:
```
SELECT
    date_trunc('day', datetime_15min)
    COUNT(DISTINCT vds_id::text || datetime_20sec::text)
FROM vds.raw_vdsdata
GROUP BY 1
```

On ITS Central: 
```
SELECT
    TIMEZONE('EST5EDT', TO_TIMESTAMP(d.timestamputc))::date, 
	COUNT(*)
FROM public.vdsdata AS d
WHERE
	timestamputc >= extract(epoch from timestamp with time zone '2023-06-28 00:00:00 EST5EDT')
	AND timestamputc < extract(epoch from timestamp with time zone '2023-07-06 00:00:00 EST5EDT' + INTERVAL '1 DAY')
	AND d.divisionid = 2 --other is 8001 which are traffic signal detectors and are mostly empty
GROUP BY 1
ORDER BY 1
```

There are some missing records in RDS from the last 9 days. 
|date_trunc	      |RDS Count| ITSC Count | Dif |
| 06/28/2023 0:00 | 56285 | 57149 | 864 |
| 06/29/2023 0:00 | 53754 | 53826 | 72 |
| 06/30/2023 0:00 | 55417 | 56713 | 1296 |
| 07/01/2023 0:00 | 52893 | 56349 | 3456 |
| 07/02/2023 0:00 | 53727 | 54339 | 612 |
| 07/03/2023 0:00 | 54090 | 54090 | 0 |
| 07/04/2023 0:00 | 53958 | 53958 | 0 |
| 07/05/2023 0:00 | 53506 | 53506 | 0 |
| 07/06/2023 0:00 | 53286 | 53286 | 0 |

In `vdsvehicledata`:

RDS: 
```
SELECT
        d.dt::date AS dt, --convert timestamp (without timezone) at UTC to EDT/EST
		count(*)
FROM vds.raw_vdsvehicledata AS d
WHERE
	d.division_id = 2 --8001 and 8046 have only null values for speed/length/occupancy
	--AND TIMEZONE('UTC', d.timestamputc) >= ''::timestamptz
	--AND TIMEZONE('UTC', d.timestamputc) < {start}::timestamptz + INTERVAL '1 DAY'
GROUP BY 1
ORDER BY 1
```

ITSC: 
```
SELECT
        (TIMEZONE('UTC', d.timestamputc) AT TIME ZONE 'EST5EDT')::date AS dt, --convert timestamp (without timezone) at UTC to EDT/EST
		count(*)
FROM public.vdsvehicledata AS d
LEFT JOIN public.vdsconfig AS c ON
	d.vdsid = c.vdsid
	AND d.divisionid = c.divisionid
	AND d.timestamputc >= c.starttimestamputc
	AND (
		d.timestamputc <= c.endtimestamputc
		OR c.endtimestamputc IS NULL) --no end date
WHERE
	d.divisionid = 2 --8001 and 8046 have only null values for speed/length/occupancy
	AND TIMEZONE('UTC', d.timestamputc) >= '2023-06-28 00:00:00 EST5EDT'::timestamptz
	AND TIMEZONE('UTC', d.timestamputc) < '2023-07-09 00:00:00 EST5EDT'::timestamptz + INTERVAL '1 DAY'
	AND substring(c.sourceid, 1, 3) <> 'BCT' --bluecity.ai sensors have no data
GROUP BY 1
ORDER BY 1
```

date	rds_count	itsc_count	
06/28/2023	173903	173903	0
06/29/2023	172128	172128	0
06/30/2023	172501	172501	0
07/01/2023	157790	157790	0
07/02/2023	147145	147145	0
07/03/2023	146517	146517	0
07/04/2023	178781	178782	1
07/05/2023	178354	178354	0
07/06/2023	183501	183501	0
07/07/2023	191859	191860	1
07/08/2023	179182	179182	0
07/09/2023	170521	170521	0
