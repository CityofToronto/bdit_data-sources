# Traffic Archive <!-- omit in toc -->

This folder contains sql & documentation used for processing old count data coming from the FLOW system in an Oracle database.

- [Traffic Archive](#traffic-archive)
  - [Contents](#contents)
  - [Old documentation](#old-documentation)


## Contents

There are two ERDs 

- `countinfomics.png`: appears to be a screenshot from an Oracle viewer, or maybe some other source of documentation
- `flow_tables_relationship.png`: was created using a diagramming application 

![Entity Relationship Diagram of some traffic count tables](countinfomics.png)
![Entity Relationship Diagram of some traffic count tables](flow_tables_relationship.png)

[`exploring relationships.ipynb`](exploring relationships.ipynb) is a jupyter notebook exploring relationships between the various tables. This has surely been superseded by deeper dives performed by the MOVE team.

[`cal_dictionary.md`](cal_dictionary.md) is a data dictionary for `TRAFFIC.CAL`


## Old documentation




### Core Tables

The previous data structure is archived in schema `traffic_archive`. This is the (OUTDATED) Copy of (old schema) traffic counts from MOVE.
- Turning Movement Count (TMC)
  - [`countinfomics`](#tmc-metadata-countinfomics): metadata
  - [`det`](#tmc-observations-det): count observations
  - [`countinfo`](#atr-metadata-countinfo): metadata
  - [`cnt_det`](#atr-observations-cnt_det): count observations
- Spatial reference
  - [`arterydata`](#spatial-temporal-reference-arterydata): an internal reference system that maps a count to a location, used by _both_ TMC and ATR tables
- Other reference
  - [`category`](#category): reference table for traffic count type or data source, used by _both_ TMC and ATR tables

The following diagrams show the relationship between the above-mentioned tables.

### (ARCHIVED TO traffic_archive) TMC Metadata (`countinfomics`)

This table contains Turning Movement Count metadata only. This table contains the location reference, date, and source for each Turning Movement Count. Each Turning Movement Count is defined by a unique `count_info_id`.

Field Name|Type|Description
----------|----|-----------
count_info_id|bigint|Unique ID for a count linked to [`det`](#tmc-observations-det) table containing detailed count entries
arterycode|bigint|ID number linked to [`arterydata`](#spatial-temporal-reference-arterydata) table containing information for the count location
count_type|varchar(1)|Count hours<sup>1</sup> during which data are recorded, Routine (R) or School/Pedestrian (P)
count_date|date|Date on which the count was conducted
day_no|bigint|Day of the week (ISO standard; 1 = Monday, 7 = Sunday)
category_id|int|ID number linked to [`category`](#category) table containing the text description of the count type or source

1 - Routine and School Hours

Routine hours are the "typical" hours during which data would be collected. School hours were specifically selected to observe school pickup, dropoff, and lunch periods. We are moving towards continuous collection periods (e.g. 6:00am-8:00pm), but legacy data are still reported during these standard 8-hour disaggregate periods.

- Routine Hours: 7:30 - 9:30 / 10:00 - 12:00 / 13:00 - 15:00 / 16:00 - 18:00
- School Hours: 7:30 - 9:30 / 10:00 - 11:00 / 12:00 - 13:30 / 14:15 - 15:45 / 16:00 - 18:00

### (ARCHIVED TO traffic_archive) TMC Observations (`det`)
This table contains individual data entries for Turning Movement Counts in 15-minute non-continuous increments. This is a "wide" format, where each direction-mode-movement has its own column. For a long (instead of wide) version of this table, see the matview `traffic.tmc_miovision_long_format`.

Field Name|Type|Description
----------|----|-----------
ID|Autonumber|Autonumber function
COUNT_INFO_ID|number|Unique ID number for a count linked to [`countinfomics`](#tmc-metadata-countinfomics) table containing count metadata (higher-level information)
COUNT_TIME|Date/Time|Effective time of counts (**time displayed is the end time period**)
N_CARS_R|number|S/B cars turning right
N_CARS_T|number|S/B cars going through
N_CARS_L|number|S/B cars turning left
S_CARS_R|number|N/B cars turning right
S_CARS_T|number|N/B cars going through
S_CARS_L|number|N/B cars turning left
E_CARS_R|number|W/B cars turning right
E_CARS_T|number|W/B cars going through
E_CARS_L|number|W/B cars turning left
W_CARS_R|number|E/B cars turning right
W_CARS_T|number|E/B cars going through
W_CARS_L|number|E/B cars turning left
N_TRUCK_R|number|S/B trucks turning right
N_TRUCK_T|number|S/B trucks going through
N_TRUCK_L|number|S/B trucks turning left
S_TRUCK_R|number|N/B trucks turning right
S_TRUCK_T|number|N/B trucks going through
S_TRUCK_L|number|N/B trucks turning left
E_TRUCK_R|number|W/B trucks turning right
E_TRUCK_T|number|W/B trucks going through
E_TRUCK_L|number|W/B trucks turning left
W_TRUCK_R|number|E/B trucks turning right
W_TRUCK_T|number|E/B trucks going through
W_TRUCK_L|number|E/B trucks turning left
N_BUS_R|number|S/B buses turning right
N_BUS_T|number|S/B buses going through
N_BUS_L|number|S/B buses turning left
S_BUS_R|number|N/B buses turning right
S_BUS_T|number|N/B buses going through
S_BUS_L|number|N/B buses turning left
E_BUS_R|number|W/B buses turning right
E_BUS_T|number|W/B buses going through
E_BUS_L|number|W/B buses turning left
W_BUS_R|number|E/B buses turning right
W_BUS_T|number|E/B buses going through
W_BUS_L|number|E/B buses turning left
N_PEDS|number|North side pedestrians
S_PEDS|number|South side pedestrians
E_PEDS|number|East side pedestrians
W_PEDS|number|West side pedestrians
N_BIKE|number|S/B bicycles from the north side
S_BIKE|number|N/B bicylcles from the south side
E_BIKE|number|W/B bicycles from the east side
W_BIKE|number|E/B bicycles from the west side
N_OTHER|number|North side  - optional field
S_OTHER|number|South side - optional field
E_OTHER|number|East side - optional field
W_OTHER|number|West side - optional field

### (ARCHIVED TO traffic_archive) ATR Metadata (`countinfo`)

Similar to [TMC Metadata (`countinfomics`)](#tmc-metadata-countinfomics), this table contains the location reference, date, and data type/source from all sources other than Turning Movement Counts.

See [TMC Metadata (`countinfomics`)](#tmc-metadata-countinfomics).

### (ARCHIVED TO traffic_archive) ATR Observations (`cnt_det`)

This table contains individual data entries for all counts or sources other than Turning Movement Counts.

Field Name|Type|Description
----------|----|-----------
count_info_id|bigint|Unique ID number for a count linked to [`countinfo`](#atr-metadata-countinfo) table containing count metadata (higher-level information)
count|bigint|Vehicle count
timecount|Date/Time|Effective time of counts (**time displayed is the end time period**) (**except for ATRs, where time is the start of the count**)
speed_class|int|Speed class codes indicating speed bins associated with the 'prj_volume.speed_classes' table. speed_class=0 refers to non-speed counts.

### (ARCHIVED TO traffic_archive) Spatial-Temporal Reference (`arterydata`)

This table contains the location information of each volume count.

Field Name|Type|Description
----------|----|-----------
arterycode|bigint|ID number referred to by [`countinfomics`](#tmc-metadata-countinfomics) and [`countinfo`](#atr-metadata-countinfo)
street1|text|first street name
street2|text|second street name
location|text|full description of count location (**do not use PX references, not consistent and can change without warning from upstream sources**)
apprdir|text|direction of the approach referred to by this arterycode
sideofint|text|the side of the intersection that the arterycode refers to
linkid|text|in the format of 8digits @ 8digits, with each 8 digits referring to a node

#### What is an arterycode?

It's very important to understand the humble arterycode. The arterycode identifier system is an internal legacy location reference system that describes intersections and segments of the Toronto street network _where a count has occurred_. Arterycodes do not describe the entire Toronto transportation network.

#### From arterycode to centreline

Given an arterycode, you can find the corresponding modern-day location by cross-referencing with `traffic.arteries_centreline`.




### (ARCHIVED TO traffic_archive) `category`

This is a reference table referencing the count type or data source of each entry.

Field Name|Type|Description
----------|----|-----------
category_id|int|ID number referred to by [`countinfomics`](#tmc-metadata-countinfomics) and [`countinfo`](#atr-metadata-countinfo)
category_name|text|name of the count type or data source


#### Category Reference

Category Name|Status|Meaning
-------------|------|-------
24 HOUR|-|Volume ATR
RESCU|**DO NOT USE.** For current RESCU data and pipeline information, see [volumes/vds](../vds).|Volume ATR data from RESCU permanent counters. Highway and major arterial in-road loop detectors.
CLASS|-|Vehicle Classification ATR
SPEED|-|Speed / Volume ATR
MANUAL|**Don't use this without further investigation.**|Likely counts loaded via manual counting boards.
PERM STN|**Don't use this without further investigation.** Unclear how this is different from other specified permanent counters.|Permanent Count Stations
BICYCLE|**DO NOT USE.** For current Eco-Counter data and pipeline information, see [volumes/ecocounter](../ecocounter).|Bicycle Volume ATR from Eco-Counter permanent count stations, manually loaded via portable device.
SPD OCC|**DO NOT USE.**|Likely a permanent counter that collected speed ("SPD") and occupancy ("OCC") data.
SENSYS SPEED|**DO NOT USE.**|Sensys permanent counters that collected speed data.

### Artery groups and count groups 
A gaggle of cascading tables that aggregate "counts" into "studies" (counts at the same location that occurred on continuous days) and "arterycodes" into "arterycode groups" (counts that occured at the same location). These intermediary tables are used to create [`studies`](#studies).

- `traffic.arteries_groups`
  - arterycodes describe a count-location
  - for ATR counts, an arterycode *also* describes a direction
  - for a two-way street where a count was conducted to observe traffic in both directions, two arterycodes exist for this one road segment
  - as such, there can be multiple arterycodes that exist at the same physical road segment
  - this table groups arterycodes that belong together at the same location
- `traffic.arteries_centreline`
  - a mapping of legacy arterycodes to current centreline nodes and segments
  - this is the output of the MOVE conflation
- `traffic.counts_multiday_runs`
  - aggregates single-day ATR "counts" into continuous multi-day "studies"
- `traffic.arteries_counts_groups`
  - brings together artery groups and count groups
  - single-day counts aggregated into continous studies (`count_group_id`) at colocated arterycodes (`artery_group_id`)


### Load Sources Summary

The previous data structure is archived in schema `traffic_archive`. This is the (OUTDATED) Copy of (old schema) traffic counts from MOVE.

| Study Type // Loading Mechanism | FlowLoad (`traffic.*`)                           | MOVE Loader (`traffic.atr_*`)        | Spectrum API Loader (`traffic.tmc_*`) |
|---------------------------------|--------------------------------------------------|--------------------------------------|---------------------------------------|
| Turning Movement Count          | All-time TMC data*                               | n/a                                  | September 2023 to present             |
| Volume ATR                      | All-time Volume ATRs**                           | n/a                                  | n/a                                   |
| Speed / Volume ATR              | All-time Speed/Vol ATRs*                         | May 2023 to present                  | n/a                                   |
| Vehicle Classification ATR      | Classification ATR data from 1985 to May 2023*** | No Classification ATR data loaded*** | n/a                                   |

*The `traffic.*` tables contain all-time TMC and Speed/Vol ATR data from all loading mechanisms

**Around 2021, we stopped collecting Volume-only ATR counts, and switched to Speed/Vol ATR counts, as they're a similar price but more data rich

***Classification ATR data is spotty for two reasons: 1) the legacy loader did not allow for co-located Speed/Vol ATR and Classification ATR data to be loaded for the same day; 2) there is curently no loading mechanism for Classification ATR data post-May 2023, when the MOVE Loader was introduced.

