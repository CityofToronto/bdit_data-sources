# TTC Data
## Incoming Data Structure
Incoming data is stored in the `avl` table of the `ttc` schema.

| Column                              | Type/Format                                     | Description/Format                                                                  | 
|-------------------------------------|--------------------------------|-------------------------------------------------------------------------------------| 
| LineNumber                          | smallint                       | Route number                                                                        | 
| Date_Key                            | integer                        | Date values concatenated into a number; YYYYMMDD                                    | 
| Date_KeyName                        | text                           | Day of week                                                                         | 
| PatternName                         | text                           | Some shorthand description of the preplanned route                                  | 
| DirectionName                       | text                           | Direction of travel; EAST or WEST                                                   | 
| TripId                              | integer                        | Identifier for the trip; 8 digits                                                   | 
| VehicleNumber                       | smallint                       | Vehicle identifier; 4 digits                                                        | 
| VehicleType                         | text                           | Describes the type of vehicle; streetcars or replacement buses                      | 
| TimePeriod_FromStop                 | text                           | Time period of the day; AM Peak, PM Peak, Midday, Evening, Other; with hour range   | 
| FromStop                            | text                           | Stop description of From stop                                                       | 
| ArrivalTime_FromStop                | timestamp without time zone    | Timestamp of actual arrival time to From stop; mm/dd/yyyy h:MM:ss AM/PM             | 
| ScheduledArrivalTime_FromStop       | timestamp without time zone    | Timestamp of scheduled arrival time to From stop; mm/dd/yyyy h:MM:ss AM/PM          | 
| ScheduleAdherence_FromStop(Seconds) | integer                        | Difference between actual and scheduled arrival times; (+) is late, (-) is early    |
| ToStop                              | text                           | Stop description of To stop                                                         | 
| ArrivalTimeTime_ToStop              | timestamp without time zone    | Timestamp of actual arrival time to To stop; mm/dd/yyyy h:MM:ss AM/PM               | 
| ScheduledArrivalTime_ToStop         | timestamp without time zone    | Timestamp of scheduled arrival time to To stop; mm/dd/yyyy h:MM:ss AM/PM            | 
| ScheduleAdherence_ToStop(Seconds)   | integer                        | Difference between actual and scheduled arrival times; (+) is late, (-) is early    |
| CurrentStop_Latitude                | numeric                        | FromStop; Latitude to 6 decimal points                                              | 
| CurrentStop_Longitude               | numeric                        | FromStop; Longitude to 6 decimal points                                             | 
| CurrentStop_PointStop_Key           | integer                        | FromStop; Identifier for lookup to PointStop table                                  | 
| Lead_Latitude                       | numeric                        | ToStop; Latitude to 6 decimal points                                                | 
| Lead_Longitude                      | numeric                        | ToStop; Longitude to 6 decimal points                                               | 
| Lead_PointStop_Key                  | integer                        | ToStop; Identifier for lookup to PointStop table                                    | 

## TTC to Bluetooth Lookup Table
Using `route_id` stored in table `ttc_routes`, `segment_id` stored in table `ttc_segments`, and the connection and order provided by table `ttc_route_segments`, a lookup table was created so that TTC segments could be aggregated into matching Bluetooth segments.

### Lookup Table Structure
|Column|Type|Description|
|------|----|-----------|
|bdit_id|serial|unique id|
|bt_id|integer|Bluetooth id derived from report name|
|bt_id_name|text|report name of Bluetooth segment, from all_analyses|
|segment_id|integer|TTC segment id|
|f_id|integer|from stop id|
|f_stopname|text|from stop name|
|t_id|integer|to stop id|
|t_stopname|text|to stop name|

