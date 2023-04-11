# Collisions

The collisions dataset consists of data on individuals involved in traffic collisions from approximately 1985 to the present day (though some historical collisions are included).

## Data Sources and Ingestion Pipeline

The data comes from the Toronto Police Services (TPS) Versadex and the Collision Reporting Centre (CRC) database, and is combined by a Data Collections team in Transportation Services Policy & Innovation, currently led by David McElroy.

Data are transferred to a Transportation Services file server from TPS on a weekly basis, as a set of XML files, and from the CRC on a monthly basis as a single CSV file. A set of scripts (managed by Jim Millington) read in these raw data into the Transportation Services Oracle database table. This table is manually validated by Data Collections, and edits are made using legacy software from the 1990s.

The collisions table is copied into the MOVE postgres data platform (`flashcrow`) and the `bigdata` postgres data platform on a daily basis. Several derived materialized views are subsequently updated on the `bigdata` postgres database to make querying fun, easy, and exciting! 

## Table Structure on the `bigdata` Postgres Database

The `collisions_replicator` schema houses raw and derived collision data tables. The `collision_factors` schema houses tables to convert raw Motor Vehicle Accident (MVA) report codes to human-readable categories (discussed further below). Both are owned by `collision_admins`.

### `ACC` and `acc_safe_copy`

The raw dataset is `collisions_replicator.ACC`, a direct mirror of the same table on the MOVE server. `collisions_replicator.acc_safe_copy` is a copy of `collisions_replicator.ACC`. The data dictionary for `ACC` is maintained jointly with MOVE and found on [Notion here](https://www.notion.so/bditto/5cf7a4b3ee7d40de8557ac77c1cd2e69?v=56499d5d41e04f2097750ca3267f44bc).

The guides that define values and categories for most columns can be found in the [Manuals page on Notion](https://www.notion.so/bditto/ca4e026b4f20474cbb32ccfeecf9dd76?v=a9428dc0fb3447e5b9c1427f8868e7c8).
In particular see the Collision Coding Manual, Motor Vehicle Collision Report 2015, and Motor Vehicle Accident Codes Ver. 1.

In order to stay up to date the `collisions_replicator.ACC` table is dropped (or deleted) and recreated every night. A third copy of the collisions table, called `collisions_replicator.acc_safe_copy`, was created to facilitate automatic updates on the `bigdata` postgres database. The `bigdata` postgres pipeline operates as follows:
1. The table `collisions_replicator.acc_safe_copy` is updated based on any differences between it and `collisions_replicator.ACC` (this is known as an "upsert" query).
2. The `collisions_replicator.collision_no` materialized view, which creates a unique id (UID) for each collision, is refreshed.
3. The `collisions_replicator.events` and `collisions_replicator.involved` materialized views are refreshed (more on this below).
4. New records, deletions and updates to existing records are tracked in a table called `collisions_replicator.logged_actions`.

**Please note:** `collisions_replicator.ACC` should never be queried directly, because any dependent views or tables would prevent `collisions_replicator.ACC` from being dropped and replaced (which would essentially freeze the whole pipeline). 

The `collisions_replicator.events` and `collisions_replicator.involved` materialized views should be used instead of `collisions_replicator.acc_safe_copy`, mostly because these materialized views contain human-readable categories instead of obscure numeric codes.

### Derived Materialized Views

As stated above, there are three materialzed views that are generated based on `collisions_replicator.acc_safe_copy`:

- `collisions_replicator.collision_no`: assigns a UID to each collision between 1985-01-01 and the most recent data refresh date. This is really just a materialized view of ids - there's no juicy collision data here - but generating the `collisions_no` id is essential for building the other two materialized views.
- `collisions_replicator.events`: all collision event-level data for collisions between 1985-01-01 and present. Columns have proper data types and categorical columns contain text descriptions rather than numerical codes.
- `collisions_replicator.involved`: all collision individual-level data for collisions between 1985-01-01 and present, with data type and categorical variable refinements similar to `collisions_replicator.events`.

The derived tables use a the following naming scheme for columns:

#### Fields in `collisions_replicator.events`

Event Column | Equivalent `acc_safe_copy` Column | Definition | Notes
-- | -- | -- | --
collision_no | (not a thing) | Collision event unique identifier |  
accnb | ACCNB | Original Oracle data UID | Restarted each year before 1996; use collision_no as a unique ID instead
accyear | Derived from ACCDATE | Year of collision |  
acctime | ACCTIME | Time of collision |  
longitude | LONGITUDE + 0.00021 | Longitude |  
latitude | LATITUDE + 0.000045 | Latitude |  
geom | Derived from longitude and latitude |  Point geometry of collision  |  Coordinate reference system is EPSG:4326
stname1 | STNAME1 | Street name or address | For intersections, the largest road is recorded under stname1
streetype1 | STREETYPE1 | Street type (Drive, Ave, Blvd, etc.) |  
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
data_source | (not a thing) | Source of data | Either TPS or CRC; see properties of `collisions_replicator.ACC`, below, for details


#### Fields in `collisions_replicator.involved`

Involved   Column | Equivalent ACC Column | Definition | Notes
-- | -- | -- | --
collision_no | (not a thing) | Collision event unique identifier |  
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
pedestrian_collision_type | PEDTYPE | Pedestrian collision type (eg. pedestrian hit on sidewalk or shoulder)
cyclist_action | CYCACT | Cyclist action |  
cyclist_condition | CYCCOND | Cyclist condition |  
cyclist_collision_type | CYCLISTYPE | Cyclist collision type (eg. cyclist struck in parking lot)
manoeuver | MANOEUVER | Vehicle manoeuver |  
posted_speed | POSTED_SPEED | Posted speed limit |  
actual_speed | ACTUAL_SPEED | Speed of vehicle |  
failed_to_remain | FAILTOREM | Whether the involved fled the scene of the crash |  
validation_userid | USERID | ID of last Transportation Services staff member to validate this involved |  
time_last_edited | TS | Time of last edit from Transportation Services |  


#### Properties of `collisions_replicator.acc_safe_copy`

Even though `collisions_replicator.acc_safe_copy` should not be directly queried (since the information is presented in a much more user-friendly way in the derived materialized views), it may be helpful to have some information about it...
- Each row represents one individual involved in a collision, so some data is repeated between rows. The data dictionary indicates which values are at the **event** level, and which at the individual **involved** level.
- `ACCNB` is not a UID. It kind of serves as one starting in 2013, but prior to that the number would reset annually, and even now, the TPS-derived `ACCNB`s might repeat every decade. Derived tables use `collision_no`, as defined in `collisions_replicator.collision_no`. You could use `RECID` if you want an "involved persons" level ID.
- `ACCNB` is generated from TPS and CRC counterparts when data is loaded into the Oracle database. It can only be 10 characters long (by antiquated convention). 
  - TPS GO numbers, with format `GO-{YEAR_REPORTED}{NUMBER}`, (example: `GO-2021267847`) are converted into `ACCNB` by:
      - extracting `{NUMBER}`, 
      - zero-padding it to 9 digits (by adding zeros before the `{NUMBER}`), and 
      - adding the last digit of the year as the first digit (and that's how `GO-2021267847` becomes `1000267847`). 
  - CRC collision numbers are recycled annually, with convention dictating that the first CRC number be `8000000` (then the next `8000001`, etc.). To convert these to `ACCNB`s: 
      - take the last two digits of the year and 
      - add the 'year' digits as the first two digits of the `ACCNB` (so `8000285` reported in 2019 becomes `198000285`). 
  - The length of each `ACCNB` is used to determine the source of the data for the `data_source` column in `collisions_replicator.events` (since TPS `ACCNB`s have 10 digits while CRC `ACCNB`s have nine).
  - To keep the dataset current, particularly for fatal collisions, Data Collections will occasionally manually enter collisions using information directly obtained from TPS, or from public media. These entries may not follow `ACCNB` naming conventions. When formal data is transferred from TPS, they are manually merged with these human-generated entries.
- The date the collision was reported is not included in `acc_safe_copy`.
- Some rows are derived from other rows by the Data & Analytics team; for example `LOCCOORD` is a simplified version of `ACCLOC`.
- Categorical data is coded using numbers. These numbers come from the Motor Vehicle Accident (MVA) reporting scheme.
- Some columns, such as the TPS officer badge number, are not included due to privacy concerns. The most egregious of these are only available in the original Oracle database, and have already been removed in the MOVE server data.
- TPS and CRC send collision records once they are reported and entered into their respective databases, which often leads to collisions being reported months, or even years, after they occurred. 
- TPS and CRC will also send changes to existing collision records (using the same XML/CSV pipeline described above) to correct data entry errors or update the health status of an injured individual. 
- Moreover, staff at Data & Analytics are constantly validating collision records, writing these changes directly to the Oracle database. Therefore, one **cannot compare** historical control totals on eg. the number of individuals involved with recently-generated ones.

### Collision Factors

Categorical data codes are stored in the `collision_factors` schema as tables. Each table name corresponds to the categorical column in `collisions_replicator.acc_safe_copy`. These are joined against `collisions_replicator.acc_safe_copy` to produce the derived materialized views.