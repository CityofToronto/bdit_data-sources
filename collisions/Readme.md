# Collisions

The collisions dataset consists of data on individuals involved in traffic
collisions from approximately 1985 to the present day (though there are some
historical collisions from even earlier included).

## Data Sources and Ingestion Pipeline

The data comes from the Toronto Police Services (TPS) Versadex and the Collision
Reporting Centre (CRC) database, and is combined by a Data Collections team in
Transportation Services Policy & Innovation, currently led by David McElroy.
Data is transferred to a Transportation Services file server from TPS weekly as
a set of XML files, and from the CRC monthly as a single CSV file. A set of
scripts (managed by Jim Millington) read in these raw data into a
Transportation Services Oracle database table. This table is manually validated
by Data Collections, and edits are made using legacy software from the 1990s.
The table is copied to a file in the [MOVE data
platform](https://github.com/CityofToronto/bdit_flashcrow)'s AWS fileserver on a
weekly basis. `pull_acc_script.py`, described below, copies this file onto our
Postgres database.

This archaic (and unnecessarily complicated) pipeline will soon be superceded by
the [MOVE platform](https://github.com/CityofToronto/bdit_flashcrow).

## Table Structure

The `collisions_replicator` schema houses raw and derived collision data tables. The
`collision_factors` schema houses tables to convert raw Motor Vehicle Accident
(MVA) report codes to human-readable categories (discussed further below). Both
are owned by `collision_admins`.

### `ACC`

The raw dataset is `collisions_replicator.ACC`, a direct mirror of the same table on the
MOVE server. The data dictionary for `ACC`
is maintained jointly with MOVE and found on [Notion here](
https://www.notion.so/bditto/5cf7a4b3ee7d40de8557ac77c1cd2e69?v=56499d5d41e04f2097750ca3267f44bc).
The guides that define values and categories for most columns can be found in
the [Manuals page on Notion](https://www.notion.so/bditto/ca4e026b4f20474cbb32ccfeecf9dd76?v=a9428dc0fb3447e5b9c1427f8868e7c8).
In particular see the Colliion Coding Manual, Motor Vehicle Collision Report
2015, and Motor Vehicle Accident Codes Ver. 1.

Properties of `collisions_replicator.ACC`:
- Each row represents one individual involved in a collision, so some data is
  repeated between rows. The data dictionary indicates which values are at the
  **event** level, and which at the individual **involved** level.
- There is no UID. `ACCNB` serves as one starting in 2013, but prior to that the
  number would reset annually. Derived tables use `collision_no`, defined in
  `collisions_replicator.collision_no`.
  - `ACCNB` [is
    generated from TPS and CRC counterparts](https://github.com/CityofToronto/bdit_data-sources/pull/349#issuecomment-803133700)
    when data is loaded into the Oracle database. It can only be 10 characters
    long (by antiquated convention). TPS GO numbers, with format
    `GO-{YEAR_REPORTED}{NUMBER}`, are converted into `ACCNB` by extracting
    `{NUMBER}`, zero-padding it to 9 digits, and adding the last digit of the
    year to the front (eg. `GO-2020267847` is translated to `0000267847`). CRC
    collision numbers are recycled annually, with convention dictating that the
    first CRC number be `8000000` (then the next `8000001`, etc.). To convert
    these to `ACCNB`s, the last two digits of the year are added to the front
    (eg. `8000285` reported in 2019 is converted to `198000285`). The format of
    each `ACNB` is reverse engineered to determine the source of the data for
    the `data_source` column in `collisions_replicator.events`.
  - To keep the dataset current, particularly for fatal collisions, Data
    Collections will occasionally manually enter collisions using information
    directly obtained from TPS, or from public media. These entries may not
    follow `ACCNB` naming conventions. When formal data is transferred from TPS,
    they are manually merged with these human-generated entries.
  - The date the collision was reported is not included in `ACC`.
- Some rows are derived from other rows by the Data & Analytics team; for
  example `LOCCOORD` is a simplified version of `ACCLOC`.
- Categorical data is coded using numbers. These numbers come from the Motor
  Vehicle Accident (MVA) reporting scheme.
- Some columns, such as the TPS officer badge number, are not included due to
  privacy concerns. The most egregious of these are only available in the
  original Oracle database, and have already been removed in the MOVE server
  data.
- TPS and CRC send collision records once they are reported and entered into
  their respective databases, which often leads to collisions being reported
  months, or even years, after they occurred. TPS and CRC will also send
  changes to existing collision records (using the same XML/CSV pipeline
  described above) to correct data entry errors or update the health status
  of an injured individual. Moreover, staff at Data & Analytics are constantly
  validating collision records, writing these changes directly to the Oracle
  database. Therefore, one **cannot compare** historical control totals on eg.
  the number of individuals involved with recently-generated ones.

### Collision Factors

Categorical data codes are in the `collision_factors` schema as tables. Each
table name corresponds to the categorical column in `collisions_replicator.ACC`. These are
joined against `collisions_replicator.ACC` to produce the derived tables.

### Derived Tables

Because `collisions_replicator.ACC` in its raw form is somewhat difficult to use, we
generate three derived tables:

- `collisions_replicator.collision_no`: assigns a UID to each collision between 1985-01-01
  and the most recent data refresh date.
- `collisions_replicator.events`: all collision event-level data for collisions between
  1985-01-01 and present. Columns have proper data types (rather than all
  integers like in `collisions_replicator.ACC`) and categorical columns use their text
  descriptions rather than numerical codes.
- `collisions_replicator.involved`: all collision individual-level data for collisions
  between 1985-01-01 and present, with refinements similar to
  `collisions_replicator.events`.

The derived tables use a different naming scheme for columns:

#### `collisions_replicator.events`

Event   Column | Equivalent ACC Column | Definition | Notes
-- | -- | -- | --
collision_no |   | Collision event unique identifier |  
accnb | ACCNB | Original Oracle data UID | Restarted each year before 1996; use collision_no as a unique ID instead
accyear | Derived from ACCDATE | Year of collision |  
acctime | ACCTIME | Time of collision |  
longitude | LONGITUDE + 0.00021 | Longitude |  
latitude | LATITUDE + 0.000045 | Latitude |  
geom | Derived from longitude and latitude |  Point geometry of collision  |  Coordinate reference system is EPSG:4326
stname1 | STNAME1 | Street name or address | For intersections, the largest road is recorded under stname1
streetype1 | STREETYPE1 | Street type (Av, Ave, Blvd, etc.) |  
dir1 | DIR1 | Direction in street name | Eg. W if street is St. Clair W; NOT the street direction of travel!
stname2 | STNAME2 | Cross street name |  
streetype2 | STREETYPE2 | Cross street type |  
dir2 | DIR2 | Cross street direction |  
stname3 | STNAME3 | Additional street name, offset distance, or address |  
streetype3 | STREETYPE3 | Additional street type |  
dir3 | DIR3 | Additional street direction |  
road_class | ROAD_CLASS | Road class |  
location_type | ACCLOC | Detailed collision location classification |  
location_class | LOCCOORD | Simplified collision location classification | Simplified location_type. ⚠️Only exists for validated collisions, exercise caution in using.
collision_type | ACCLASS | Ontario Ministry of Transportation collision class |  
impact_type | IMPACTYPE | Impact type (eg. rear end, sideswipe) |  
visibility | VISIBLE | Visibility (usually due to inclement weather) |  
light | LIGHT | Road lighting conditions |  
road_surface_cond | RDSFCOND | Road surface conditions |  
px | PX | Signalized intersection PX number |  
traffic_control | TRAFFICTL | Type of traffic control |  
traffic_control_cond | TRAFCTLCOND | Status of traffic control |  
on_private_property | PRIVATE_PROPERTY | Whether collision is on private property |  
description | DESCRIPTION | Long-form comments |  
data_source | | Source of data | See properties of `collisions_replicator.ACC`, above, for details


#### `collisions_replicator.involved`

Involved   Column | Equivalent ACC Column | Definition | Notes
-- | -- | -- | --
collision_no |   | Collision event unique identifier |  
person_no | PER_NO | Person identifier for individual collision event |  
vehicle_no | VEH_NO | Vehicle identifier for individual collision event |  
vehicle_class | VEHTYPE | Vehicle class |  
initial_dir | INITDIR | Initial direction of travel |  
impact_location | IMPLOC | Location of impact on road |  
event1 | EVENT1 | First event that occurred for involved |  
event2 | EVENT2 | Second event |  
event3 | EVENT3 | Third event |  
involved_class | INVTYPE | Class of road user (eg. driver, passenger, pedestrian)
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
validation_userid | USERID | ID of last Transportation Services staff member to validate this involved |  
time_last_edited | TS | Time of last edit from Transportation Services |  


## Data Replication Process

Currently, `pull_acc_script.py` is used to manually refresh the data. In the
future `collisions_replicator.ACC` will be directly mirrored from the MOVE server.

`pull_acc_script.py` performs the following steps:
1. Obtain a copy of `ACC.dat` (a dump of the ACC table from the Oracle server).
2. `TRUNCACTE` the current `collisions_replicator.ACC` table on the BDITTO Postgres, then
   replace it using `ACC.dat`.
3. Refresh `collisions_replicator.collision_no`, `collisions_replicator.events` and
   `collisions_replicator.involved` materialized views.
4. Perform simple consistency checks to ensure all data were properly copied.

`pull_acc_script.py` requires:

```
click>=7.1.2
psycopg2>=2.8.4
```

for parsing command line arguments and connecting to Postgres, respectively.

Scripts that define the tables and materialized views can be found in this
folder.

### Running the Data Replication

1. Ensure you have a ConfigParser file available. See the [official Python
   documentation](https://docs.python.org/3/library/configparser.html) to set
   one up if you do not.
2. Obtain the PEM key for connecting to MOVE over SSH from either your manager
   or the MOVE product team.
3. Add `'POSTGRES'` and `'FLASHCROW'` entries to your ConfigParser file.
   `'POSTGRES'` should contain `user`, `password`, `host` and `port` entries,
   corresponding to the Postgres credentials of the user running
   `pull_acc_script.py`. `'FLASHCROW'` should have a `pem` entry, which gives
   the filepath and name of the PEM key.
4. Run `pull_acc_script.py`. You're required to pass the path of the
   ConfigParser file using the `cfgpath` keyword:
   ```
   python pull_acc_script.py --cfgpath /{PATH}/{TO}/{FILE}/{FILENAME}
   ```
   For additional options, run `python pull_acc_script.py --help`.

**It is highly recommended to run this process outside of regular work hours, as
large file transfers from Flashcrow and to the Postgres can sometimes be
interrupted by the VPN.**
