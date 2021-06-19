# Collisions

The collisions dataset consists of data on individuals involved in traffic
collisions from approximately 1985 to the present day (though there are some
historical collisions from even earlier included). The data comes from the
Toronto Police Services (TPS) and the Collision Reporting Centre (CRC), and
is combined by a Data & Analytics team in Transportation Services Policy &
Innovation, currently led by Racheal Saunders. It is currently hosted in an
Oracle database and maintained using legacy software from the 1990s, but is in
the process of being completely transitioned into the [MOVE platform](https://github.com/CityofToronto/bdit_flashcrow).

## Table Structure

The `collisions` schema houses raw and derived collision data tables. The
`collision_factors` schema houses tables to convert raw Motor Vehicle Accident
(MVA) report codes to human-readable categories (discussed further below). Both
are owned by `collision_admins`.

### `acc`

The raw dataset is `collisions.acc`, a direct mirror of the same table on the
MOVE server (which itself mirrors from Oracle). The data dictionary for `ACC`
is maintained jointly with MOVE and found on [Notion here](
https://www.notion.so/bditto/5cf7a4b3ee7d40de8557ac77c1cd2e69?v=56499d5d41e04f2097750ca3267f44bc).
The guides that define values and categories for most columns can be found in
the [Manuals page on Notion](https://www.notion.so/bditto/ca4e026b4f20474cbb32ccfeecf9dd76?v=a9428dc0fb3447e5b9c1427f8868e7c8).
In particular see the Colliion Coding Manual, Motor Vehicle Collision Report
2015, and Motor Vehicle Accident Codes Ver. 1.

Properties of `collisions.acc`:
- Each row represents one individual involved in a collision, so some data is
  repeated between rows. The data dictionary indicates which values are at the
  event-level, and which at the individual level.
- There is no UID. `ACCNB` serves as one starting in 2013, but prior to that the
  number would reset annually. Derived tables use `collision_no`, defined in
  `collisions.collision_no`.
- Some rows are derived from other rows by the Data & Analytics team; for
  example `LOCCOORD` is a simplified version of `ACCLOC`.
- Categorical data is coded using numbers. These numbers come from the Motor
  Vehicle Accident (MVA) reporting scheme.
- Some columns, such as the TPS officer badge number, are not included due to
  privacy concerns. The most egregious of these are only available in the
  original Oracle database, and have already been removed in the MOVE server
  data.

### Collision Factors

Categorical data codes are in the `collision_factors` schema as tables. Each
table name corresponds to the categorical column in `collisions.acc`. These are
joined against `collisions.acc` to produce the derived tables.

### Derived Tables

Because `collisions.acc` in its raw form is somewhat difficult to use, we
generate three derived tables:

- `collisions.collision_no`: assigns a UID to each collision between 1985-01-01
  and the most recent data refresh date.
- `collisions.events`: all collision event-level data for collisions between
  1985-01-01 and present. Columns have proper data types (rather than all
  integers like in `collisions.acc`) and categorical columns use their text
  descriptions rather than numerical codes.
- `collisions.involved`: all collision individual-level data for collisions
  between 1985-01-01 and present, with refinements similar to
  `collisions.events`.

The derived tables use a different naming scheme for columns:

#### `collision.events`

Event   Column | Equivalent ACC Column | Definition | Notes
-- | -- | -- | --
collision_no |   | Collision event unique identifier |  
accnb | ACCNB | Original Oracle data UID | Restarted each year before 1996; use collision_no as a unique ID instead
accyear | Derived from ACCDATE | Year of collision |  
acctime | ACCTIME | Time of collision |  
longitude | LONGITUDE + 0.00021 | Longitude |  
latitude | LATITUDE + 0.000045 | Latitude |  
stname1 | STNAME1 | Street name or address | For intersections, the largest road is recorded under stname1
streetype1 | STREETYPE1 | Street type (Av, Ave, Blvd, etc.) |  
dir1 | DIR1 | Direction in street name | Eg. W if street is St. Clair W; NOT the street direction of travel!
stname2 | STNAME2 | Cross street name |  
streetype2 | STREETYPE2 | Cross street type |  
dir2 | DIR2 | Cross street direction |  
stname3 | STNAME3 | Additional street name or address |  
streetype3 | STREETYPE3 | Additional street type |  
dir3 | DIR3 | Additional street direction |  
road_class | ROAD_CLASS | Road class |  
location_type | ACCLOC | Detailed collision location classification |  
location_class | LOCCOORD | Simplified collision location classification | Simplified location_type
collision_type | ACCLASS | Ontario Ministry of Transportation collision class |  
visibility | VISIBLE | Visibility (usually due to inclement weather) |  
light | LIGHT | Road lighting conditions |  
road_surface_cond | RDSFCOND | Road surface conditions |  
traffic_control | TRAFFICTL | Type of traffic control |  
traffic_control_cond | TRAFCTLCOND | Status of traffic control |  
on_private_property | PRIVATE_PROPERTY | Whether collision is on private property |  
description | DESCRIPTION | Long-form comments |  


#### `collisions.involved`

Involved   Column | Equivalent ACC Column | Definition | Notes
-- | -- | -- | --
collision_no |   | Collision event unique identifier |  
vehicle_no | VEH_NO | Vehicle identifier for individual collision event |  
person_no | PER_NO | Person identifier for individual collision event |  
vehicle_class | VEHTYPE | Vehicle class |  
initial_dir | INITDIR | Initial direction of travel |  
impact_type | IMPACTYPE | Impact type (eg. rear end, sideswipe) |  
event1 | EVENT1 | First event that occurred for involved |  
event2 | EVENT2 | Second event |  
event3 | EVENT3 | Third event |  
involved_class | INVTYPE | Class of road user (eg. driver,   passenger, pedestrian)
involved_age | INVAGE or BIRTHDATE | Age of involved | Selects from whichever is available/more accurate
involved_injury_class | INJURY | Level of injury |  
safety_equip_used | SAFEQUIP | Safety equipment used (eg. seat belt) |  
driver_action | DRIVACT | Driver action |  
driver_condition | DRIVCOND | Driver condition (eg. impaired) |  
pedestrian_action | PEDACT | Pedestrian action |  
pedestrian_condition | PEDCOND | Pedestrian condition |  
pedestrian_collision_type | PEDTYPE | Pedestrian collision type (eg.   pedestrian hit on sidewalk or shoulder)
cyclist_action | CYCACT | Cyclist action |  
cyclist_condition | CYCCOND | Cyclist condition |  
cyclist_collision_type | CYCLISTYPE | Cyclist collision type (eg. cyclist   struck in parking lot)
manoeuver | MANOEUVER | Vehicle manoeuver |  
posted_speed | POSTED_SPEED | Posted speed limit |  
actual_speed | ACTUAL_SPEED | Speed of vehicle |  
failed_to_remain | FAILTOREM | Whether the involved fled the scene of the crash |  
is_validated | USERID | Whether the collision has been validated by Transportation Services | True if RSU user ID exists


## Data Replication Process

Currently, `pull_acc_script.py` is used to manually refresh the data. In the
future `collisions.acc` will be directly mirrored from the MOVE server.

`pull_acc_script.py` performs the following steps:
1. Obtain a copy of `ACC.dat` (a dump of the ACC table from the Oracle server).
2. `TRUCACTE` the current `collisions.acc` table on the BDITTO Postgres, then
   replace it using `ACC.dat`.
3. Refresh `collisions.collision_no`, `collisions.events` and
   `collisions.involved` materialized views.
4. Perform simple consistency checks to ensure all data were properly copied.

Scripts that define the tables and materialized views can be found in this
folder.

**It is highly recommended to run this process outside of regular work hours, as
large file transfers from Flashcrow and to the Postgres can sometimes be
interrupted by the VPN.**
