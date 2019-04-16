# Traffic Bylaw 

This dataset was acquired from GCCVIEW's REST API cot_geospatial2 MapServer. It includes digitalized data for the following chapters and schedules mainly around the downtown area, and east of downtown. For more information on each specific bylaw, click [here](https://www.toronto.ca/legdocs/bylaws/lawmcode.htm#III).
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
| city        | (to be filled in)                                                      | TO                                         |
| deleted     | State of this feature                                                  | FALSE                                      |
| bylawno_ol  | (to be filled in)                                                      |                                            |
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
| gis         | (gis layer created?)                                                   | 2015-07-09                                 |
| anomalies   | (to be filled in)                                                      | P                                          |
| symbol_id   | Unique identifier for each schedule in a chapter                       | 886_D                                      |
| legend_id   | Unique identifier for each schedule in a chapter                       | 886_D                                      |
| last_update | Date of when this feature is last updated                              | 2015-07-08                                 |
