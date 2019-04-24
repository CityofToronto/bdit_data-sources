# Traffic Bylaw 

This dataset was acquired from GCCVIEW's REST API cot_geospatial2 MapServer. It includes digitalized data for the following chapters and schedules mainly around the downtown area, and east of downtown. For more information on each specific bylaw, click [here](https://www.toronto.ca/legdocs/bylaws/lawmcode.htm#III).

### Extent of digitalized data

The current extent of the digitalized version is highlighted in blue, where the toronto centreline network is represented in grey.

![bylaw_extent_dark](https://user-images.githubusercontent.com/46324452/56687273-e6d60980-66a3-11e9-9ac6-fffa8deb55d7.PNG)


### Chapters and Schedules included in the Traffic Bylaw

| Chapter | Schedule | Schedule Name                                                                 |
|---------|----------|-------------------------------------------------------------------------------|
| 886     | D        | Designated Lanes for Bicycles                                                 |
| 886     | E        | Cycle Tracks                                                                  |
| 903     | 2        | Designated On-Street Parking for   Permit Holders                             |
| 903     | 3        | Designated On-Street Loading Zones for   Permit Holders                       |
| 910     | 1        | Parking Machines                                                              |
| 910     | 2        | Parking Machines Locations Designated   as Electric Vehicle Charging Stations |
| 910     | 3        | Parking Meters                                                                |
| 925     | A        | Permit Parking                                                                |
| 950     | 1        | Bicycles Prohibited                                                           |
| 950     | 10       | Bus Loading Zones                                                             |
| 950     | 11       | Safety Zones                                                                  |
| 950     | 12       | Permitted Angle Parking                                                       |
| 950     | 13       | No Parking                                                                    |
| 950     | 14       | No Stopping                                                                   |
| 950     | 15       | Parking for Restricted Periods                                                |
| 950     | 16       | No Standing                                                                   |
| 950     | 17A      | Parking and Standing during Snow   Emergencies                                |
| 950     | 17B      | Parking and Standing on Streetcar   Tracks during Snow Emergencies            |
| 950     | 18       | One-Way Highways                                                              |
| 950     | 19       | One-Way Traffic Lanes                                                         |
| 950     | 2        | Pedestrians Prohibited on Certain   Highways                                  |
| 950     | 20       | Two-Way Left-Turn-Only Lanes                                                  |
| 950     | 21       | Vehicles Prohibited in Left Lanes on   Certain Highways                       |
| 950     | 22       | Reserved Lanes                                                                |
| 950     | 23       | Prohibited Turns                                                              |
| 950     | 26       | Through Highways                                                              |
| 950     | 29       | Restricted Width of Vehicles on   Highways                                    |
| 950     | 3        | Prohibited Pedestrian Crossings                                               |
| 950     | 30       | Heavy Vehicles Prohibited                                                     |
| 950     | 31       | Weight Limits on Bridges                                                      |
| 950     | 32       | Speed Limits on Bridges                                                       |
| 950     | 33       | Community Safety Zones                                                        |
| 950     | 35       | Speed Limits on Public Highways                                               |
| 950     | 36       | School Speed Zones                                                            |
| 950     | 37       | School Bus Loading Zones                                                      |
| 950     | 40       | Highways with Traffic Calming Measures                                        |
| 950     | 41       | Speed Control Zones - Public Laneways                                         |
| 950     | 43       | Car-Share Vehicle Parking Areas                                               |
| 950     | 44       | Electric Vehicle Charging Station   Parking                                   |
| 950     | 5        | Stands for Taxicabs                                                           |
| 950     | 6        | Commercial Loading Zones                                                      |
| 950     | 7        | Passenger Loading Zones                                                       |
| 950     | 8        | Bus Parking Zones                                                             |
| 950     | 9        | Delivery Vehicle Parking Zones                                                |

## Fields returned from the layer

| Columns     | Description                                                            | Example                                    |
|-------------|------------------------------------------------------------------------|--------------------------------------------|
| id          | Unique id for each geometry                                            | 124                                        |
| objectid    | Unique id for each geometry                                            | 536892                                     |
| city        | The city where the by-law originated from prior to consolidation                                                      | TO                                         |
| deleted     | State of this feature                                                  | FALSE                                      |
| bylawno_ol  | By-law number that existed before all the former Traffic and Parking Schedules were consolidated into Chapter 950                                                      |                                            |
| bylawno     | Date of when this by-law was added                                     | [Added 2010-08-27 by By-law No. 1115-2010] |
| chapter     | City of Toronto Municipal Code for by-laws, each chapter is a   by-law | 886                                        |
| schedule    | Specific schedule under each by-law                                    | D                                          |
| schedule_n  | Name of the schedule                                                   | Designated Lanes for Bicycles              |
| col1c       | Street where the by-law is effective                                   | Lansdowne Avenue                           |
| col2c       | Additional information depends on schedule                             | Bloor Street West and Paton Road           |
| col3c       | Additional information depends on schedule                             | Easterly Northbound                        |
| col4c       | Additional information depends on schedule                             | Anytime                                    |
| col5c       | Additional information depends on schedule                             | null                                       |
| col6c       | N/A                                                                    | null                                       |
| gis         | Reserved column for linkages to other datasets                                                   | 2015-07-09                                 |
| anomalies   | Identifier for potential problems with entry                                                   | P                                          |
| symbol_id   | Unique identifier for each schedule in a chapter                       | 886_D                                      |
| legend_id   | Unique identifier for each schedule in a chapter                       | 886_D                                      |
| last_update | Date of when this feature is last updated                              | 2015-07-08                                 |


## Processing

For the vhf pick-up and drop-off analysis, we used this filter to extract certain schedules:

| Schedule | Schedule Name                 | 
|----------|-------------------------------|
| 950_14   | No Stopping                   |
| 886_D    | Designated Lanes for Bicycles | 
| 886_E    | Cycle Tracks                  | 
| 950_37   | School Bus Loading Zones      | 
| 950_6    | Stands for Taxicabs           | 
| 950_5    | Commerical Loading Zones      |
| 950_7    | Passenger Loading Zones       |
| 950_10   | Bus Loading Zones             |

```sql 
create table traffic_bylaw_filtered as 
with tempa as (select id, objectid, city, deleted, bylawno_ol, bylawno, chapter, schedule, schedule_n, col1c, col2c, col3c, col4c, col5c, col6c, gis, anomalies, symbol_id, legend_id, last_update, 
(ST_dump(geom)).geom as geom from gis.traffic_bylaw where symbol_id in ('886_D', '886_E', '950_10', '950_14', '950_37',  '950_5', '950_6', '950_7'))
select legend_id, schedule_n, case when legend_id = '886_D' then "col4c" 
								   when legend_id = '886_E' then "col4c"
								   when legend_id = '950_5' then "col4c"
						   		   when legend_id = '950_6' then "col4c"
								   when legend_id = '950_7' then "col4c"
								   when legend_id = '950_10' then "col4c"
								   when legend_id = '950_14' then "col4c"
								   when legend_id = '950_37' then "col4c"
								   end as time_day
								  , geom
from tempa
```
