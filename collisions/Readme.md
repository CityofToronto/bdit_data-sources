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

The raw dataset is stored under `collisions.acc`, a direct mirror of the same
table on the MOVE server (which itself mirrors from Oracle). The data dictionary
for this table is maintained jointly with MOVE and found on [Notion
here](https://www.notion.so/bditto/5cf7a4b3ee7d40de8557ac77c1cd2e69?v=56499d5d41e04f2097750ca3267f44bc).

### Properties of `collisions.acc`
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
- Some columns are not included due to privacy concerns.

### Collision Factors

Categorical data codes are in the `collision_factors` schema as tables. Each
table name corresponds to the categorical column in `collisions.acc`.

### Derived Tables

Because `collisions.acc` in its raw form is somewhat difficult to use, we
generate three derived tables:

- `collisions.collision_no`: assigns a UID to each collision between 1985-01-01
  and the most recent data refresh date.
- `collisions.events`: all collision event-level data for collisions between
  1985-01-01 and present. Columns have proper data types (rather than all
  integers like in `collisions.acc`) and categorical columns use their text
  descriptions rather than numerical codes.
- `collisions.events`: all collision individual-level data for collisions
  between 1985-01-01 and present, with refinements similar to
  `collisions.events`.

The derived tables use a different naming scheme for columns:

#### `collision.events`

Event Column | Relation to ACC Column | Notes
-- | -- | --
collision_no |   |  
accnb | ACCNB |  
accyear | Derived from ACCDATE |  
acctime | ACCTIME |  
longitude | LONGITUDE + 0.00021 |  
latitude | LATITUDE + 0.000045 |  
stname1 | STNAME1 |  
streetype1 | STREETYPE1 |  
dir1 | DIR1 |  
stname2 | STNAME2 |  
streetype2 | STREETYPE2 |  
dir2 | DIR2 |  
stname3 | STNAME3 |  
streetype3 | STREETYPE3 |  
dir3 | DIR3 |  
road_class | ROAD_CLASS |  
location_type | ACCLOC |  
location_class | LOCCOORD | Simplified location_type
collision_type | ACCLASS |  
visibility | VISIBLE |  
light | LIGHT |  
road_surface_cond | RDSFCOND |  
traffic_control | TRAFFICTL |  
traffic_control_cond | TRAFCTLCOND |  
on_private_property | PRIVATE_PROPERTY |  
description | DESCRIPTION |  

#### `collisions.involved`

Involved Column | Relation to ACC Column | Notes
-- | -- | --
collision_no |   |  
vehicle_no | VEH_NO |  
person_no | PER_NO |  
vehicle_class | VEHTYPE |  
initial_dir | INITDIR |  
impact_type | IMPACTYPE |  
event1 | EVENT1 |  
event2 | EVENT2 |  
event3 | EVENT3 |  
involved_class | INVTYPE |  
involved_age | INVAGE or BIRTHDATE | Selects from whichever is available/more accurate
involved_injury_class | INJURY |  
safety_equip_used | SAFEQUIP |  
driver_action | DRIVACT |  
driver_condition | DRIVCOND |  
pedestrian_action | PEDACT |  
pedestrian_condition | PEDCOND |  
pedestrian_collision_type | PEDTYPE |  
cyclist_action | CYCACT |  
cyclist_condition | CYCCOND |  
cyclist_collision_type | CYCLISTYPE |  
manoeuver | MANOEUVER |  
posted_speed | POSTED_SPEED |  
actual_speed | ACTUAL_SPEED |  
failed_to_remain | FAILTOREM |  
is_validated | USERID | True if RSU user ID exists

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
