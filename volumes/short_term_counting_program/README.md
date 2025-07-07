# Short Term Traffic Volumes <!-- omit in toc -->

Short-term Traffic volume data (traffic counts and turning movements) from the FLOW database and other data sources.

## Table of Contents <!-- omit in toc -->

- [Introduction](#introduction)
- [What is counted?](#what-is-counted)
  - [Turning Movement Counts (TMC)](#turning-movement-counts-tmc)
  - [Midblock Speed-Volume-Classification (SVC) (Previously Automated Traffic Record (ATR))](#midblock-speed-volume-classification-svc-previously-automated-traffic-record-atr)
- [Where does it come from?](#where-does-it-come-from)
- [How often is data updated?](#how-often-is-data-updated)
- [Where can I access the data?](#where-can-i-access-the-data)
- [Where can I find what data?](#where-can-i-find-what-data)
- [How is the data structured?](#how-is-the-data-structured)
  - [Core Tables](#core-tables)
    - [Vehicle movement](#vehicle-movement)
    - [Bike movement](#bike-movement)
    - [Pedestrian movement](#pedestrian-movement)
    - [TMC Relations](#tmc-relations)
    - [SVC Relations](#svc-relations)
  - [Other Useful Tables](#other-useful-tables)
- [Relevant Tables](#relevant-tables)
- [Useful Views](#useful-views)
- [Cycling Seasonality Adjustment](#cycling-seasonality-adjustment)
- [What's the old FLOW Oracle Schema?](#whats-the-old-flow-oracle-schema)

## Introduction

The City of Toronto collects ad-hoc traffic volume data for projects and service requests. The traffic data collection program serves many internal transportation projects and operations teams for project planning, capital planning, engineering design, project analysis, and operational functions like signal timing.

The most common traffic studies conducted are the **Turning Movement Count** (TMC) and the **Midblock Speed-Volume-Classification (SVC)** count (previously known as the **Automated Traffic Recorder** (ATR) count). TMCs observe movements of motor vehicle, bicycle, and pedestrian volumes at intersections. SVCs observe volumes, speeds, and vehicle classification of motor vehicles travelling along a section of road.

Other studies include pedestrian delay and classification, pedestrian crossover observation, stop-sign compliance, queue-delay, cordon count, and radar speed studies.

## What is counted?

### Turning Movement Counts (TMC)

- Type of road user: car, truck, bus, bicycle, pedestrian, other
- Intersection approach leg: N / S / E / W
- Type of movement:
  - Motor vehicle: Through / Left / Right
  - Cyclist: cyclist volume by approach leg
  - Pedestrian: pedestrian volume by leg of intersection crossed

#### Data Elements <!-- omit in toc -->
* Location Identifier (SLSN *Node* ID)
* 15 min aggregated interval time
* 15 min aggregated volume per movement (turning and approach) by:
	- vehicle types
	- cyclists and pedestrian counts are approach only
	
#### Notes <!-- omit in toc -->
* No regular data load schedule
* Data files collected by 2-3 staff members
* Manually geo-reference volume data to an SLSN node during data import process
* Counts are typically conducted on Tuesdays, Wednesdays, and/or Thursdays during school season (September - June) for 1 to 3 consecutive days
* If collected data varies more than defined historical value threshold by 10%, the collected data will not be loaded
* Volumes are available at both signalized and non-signalized intersections
* Each count station is given a unique identifier to avoid duplicate records
* Data will not be collected under irregular traffic conditions (construction, closure, etc), but it maybe skewed by unplanned incidents

### Midblock Speed-Volume-Classification (SVC) (Previously Automated Traffic Record (ATR))

- Volume
  - Direction
  - Volume
- Speed
  - Direction
  - Speed bin
  - Volume
- Vehicle classification
  - Direction
  - Vehicle classification
  - Volume

#### Data Elements <!-- omit in toc -->
* Location Identifier (SLSN *Link* ID)
* Direction
* 15 min aggregated interval time
* 15 min volume
	- typically aggregated by direction, although data may be available by lane

#### Notes <!-- omit in toc -->
* The counts represent roadway and direction(s), not on a lane-by-lane level
* No regular data load schedule
* Manually geo-reference volume data to an SLSN node during data import process
* Typical ATR counts 24h * 3 days at location in either 1 or both directions
* Each PCS/ATR is given a unique identifier to avoid duplicate records

## Where does it come from?

The City of Toronto retains a traffic counting contractor who conducts data collection. They schedule and install temporary counting equipment, or send staff into the field to observe volumes, and process data.

Data are collected through various technologies. Originally, data were collected by field staff who would manually observe and record volumes. Pneumatic road tubes were introduced to record motor vehicle volumes, speeds, and classifications. More recently, counting has shifted to video observation.

Once the City receives data from the contractor, staff load the data into our database. Until recently, staff would load data files into a legacy Oracle database through an application called "FlowLoad". Data would then be retrieved through a user interface application called "Flow", where the data were formatted into nice reports. In recent years, "Flow" was replaced by [MOVE](https://github.com/CityofToronto/bdit_flashcrow). For TMC's an API has been set up to ingest data directly from the contractor into the MOVE database, then copied into bigdata. An API for SVC's is in the works.

## How often is data updated?

Traffic counts are conducted ad-hoc, usually on request for a specific project need. As such, the data is not necessarily systematically collected. We do not have comprehensive coverage across time or space.

TMCs are processed automatically, nightly, once made available from the contractor. ATRs are loaded manually by staff once data files are received from the contractor.

## Where can I access the data?

Internal to the Transportation Data & Analytics team, legacy data flows from legacy Oracle database, nightly to MOVE (`flashcrow` RDS), and is then replicated to the `bigdata` RDS. Newer data is loaded directly into MOVE and then replicated to the `bigdata` RDS.

Look in the `traffic` schema for all ad-hoc data tables.

## Where can I find what data?

Speed Volume Classification counts and Turning Movement Counts are being replicated into Bigdata `traffic` schema from MOVE (FLASHCROW database). Every table that is replicated has a link to internal documentation for the corresponding table on FLASHCROW in the table comment, viewable in table properties in PGAdmin.

Public documentation including data dictionaries are accessible on the Open Data pages:

- SVC: [Traffic Volumes - Midblock Vehicle Speed, Volume and Classification Counts](https://open.toronto.ca/dataset/traffic-volumes-midblock-vehicle-speed-volume-and-classification-counts/) 
- TMC: [Traffic Volumes - Multimodal Intersection Turning Movement Counts](https://open.toronto.ca/dataset/traffic-volumes-at-intersections-for-all-modes/)

Load Sources Summary

| Study Type                      | FlowLoad (study_source = 'OTI / FlowLoad')       | MOVE Loader (study_source = 'MOVE Load') | Spectrum API Loader (count_source = SPECTRUM / LEGACY) |
|---------------------------------|--------------------------------------------------|------------------------------------------|--------------------------------------------------------|
| Turning Movement Count          | All-time TMC data*                               | n/a                                      | September 2023 to present                              |
| Volume ATR                      | All-time Volume ATRs**                           | n/a                                      | n/a                                                    |
| Speed / Volume ATR              | All-time Speed/Vol ATRs*                         | May 2023 to present                      | n/a                                                    |
| Vehicle Classification ATR      | Classification ATR data from 1985 to May 2023*** | No Classification ATR data loaded***     | n/a                                                    |


## How is the data structured?

### Core Tables

The database is structured around three types of tables: metadata, count observations, summary stats, and reference tables (spatial, temporal, or categorical).

The mapping of tables between Bigdata-MOVE-Open Data is summarized below. Replicated tables have a documentation link in the table comment, viewable in PGAdmin in table properties.

| Bigdata `traffic` Table         | MOVE (FLASHCROW) Table       | Open Data file | Description |
|---------------------------------|------------------------------|------------------------------------------|--------------------------|
| `svc_metadata` | `atr.metadata_json` | included in `svc_summary_data` | Table containing 15-minute observations for classification ATRs (SVCs). |
| `svc_study_class` | `atr.study_class_human` | `svc_raw_data_class_*` | Table containing 15-minute observations for classification ATRs (SVCs). |
| `svc_study_speed` | `atr.study_speed_human` | `svc_raw_data_speed_*` | Table containing 15-minute observations for speed volume ATRs (SVCs). |
| `svc_study_volume` | `atr.study_volume_human` | `svc_raw_data_volume_*` | Raw SVC volume counts. |
| `svc_summary_stats` | `atr.summary_stats` | `svc_summary_data` and `svc_most_recent_summary_data` | Summary statistics for ATR (SVC) counts. Join to atr.metadata_json for full study metadata. |
| `tmc_metadata` | `tmc.metadata` | included in `tmc_summary_data` | Count-level study metadata for TMCs that contains all counts including both 14 and 8 hour legacy counts. Studies have been joined to the MOVE centreline. |
| `tmc_study_data` | `tmc.study_human` | `tmc_raw_data_*` | Table containing 15-minute observations for TMCs |
| `tmc_summary_stats` | `tmc.summary_stats` | `tmc_summary_data` and `tmc_most_recent_summary_data` | Count level summary statistics for all TMCs. |
| `fhwa_classes` | - | `fwha_classification.png` | Provides a reference for the FWHA classification system. [Notion doc](https://www.notion.so/bditto/Feature-Classification-ATRs-27ece0049d654c9ba06136bffc07e2e8?pvs=4#e618feab5f8d4bb48e88f879915cbeab) |
| `midblocks` | `centreline2.midblocks` | - | Simplified midblock network to which MOVE2 conflates studies to. Includes improved naming for midblock segments. |
| `intersections` | `centreline2.intersections` | - | Simplified intersection file that corresponds to the midblocks used by MOVE 2. |
| `pxo` | `centreline2.pxo` | - | Contains a mapping of `intersection_id` or `midblock_id` to `px` crossing numbers for pedestrian cross-overs. | 
| `traffic_signal` | `centreline2.traffic_signal` | - | Contains a mapping of `intersection_id` or `midblock_id` to `px` crossing numbers for traffic control signals. |
| `mto_length_bin_classification` | - | - | MTO 6 length bin classification guide. Used to summarize vehicle lengths observed |
| `studies` | `counts2.studies` | included in `svc_summary_data` | Contains metadata for all study types available in MOVE. Copied from "move_staging"."counts2_studies", this table uses the legacy data structure and only use it when comparing to whats in the MOVE web application. |

Useful Views
  - `svc_daily_totals` - A daily summary of `traffic.svc_unified_volumes` by leg and centreline_id. Only rows with data for every 15 minute timebin are included. 
  - `svc_unified_volumes` - A unified view of Speed, Volume, and Classification study volumes by 15 minute bin.

Note on `study_id`
- The SVC count identifier `study_id` are common for a given centreline_id and multi-day study
- This means `study_id` is common for the two directions of traffic at a midblock SVC count if they map to the same location (centreline_id). If they were done on opposite side of the an interseciton (a common scenario), the two directions will have separate `study_id`. This scenario still requires manual matching of studies to group directional data obtained on the same day, if desired.
- Because `study_id` is point-location-based, it will adapt to version changes of the Toronto centreline


#### Vehicle movement
The following image depicts motor vehicle movements. This example shows south approach, or northbound travel, movements.

- `S_[CARS|TRUCK|BUS]_L`
- `S_[CARS|TRUCK|BUS]_T`
- `S_[CARS|TRUCK|BUS]_R`

!['tmc_turning_movements'](../img/tmc_movements.png)

Notes:
- Exits can be calculated by summing associated movements.
- U-turns are currently not available [in `bigdata`](#where-can-i-access-the-data).

#### Bike movement

At the time of writing, bike totals are reported only by the number of cyclists that enter the intersection from a given approach/leg. Turning movements are currently not available [in `bigdata`](#where-can-i-access-the-data).

#### Pedestrian movement

Pedestrians are counted based on the side of the intersection they cross on. The example below shows `S_PEDS` or pedestrians crossing on the south side of the intersection. Note that they could be travelling either east or west in this example.

!['tmc_ped_cross'](../img/ped_cross_movement.png)

Pedestrians are only counted when they cross the roadway, meaning that pedestrians who turn at the intersections without crossing the roadway are _not_ counted.

For 3-legged or "T" intersections, pedestrians have typically _not_ been counted on the side of the intersection without a crosswalk, even when present in large numbers. The count in these cases will be given as zero. Going forward however (circa late 2024), the intention is to count that sidewalk as though it was a crossing of a typical 4-legged intersection.




#### TMC Relations

!['tmc_flow_tables_relationship'](../img/2025_TMC_ERD_relations_short_term_count-FK_is_highlighted_green.png)

#### SVC Relations

!['svc_flow_tables_relationship'](../img/2025_ATR_ERD_svc_relations_short_term_counting-FK_is_highlighted_green.png)



### Other Useful Tables

#### `studies` <!-- omit in toc -->
FOR LEGACY PURPOSES ONLY, this is conflated to the legacy MOVE centreline.

A human-friendly interpretation of studies. Grouped by colocated arterycodes and with single-day ATR "counts" into continuous study days.

Find at `traffic.studies`.



#### New TMCs <!-- omit in toc -->
Recent TMCs (September 2023 and on) loaded through new mechanisms. Includes 14-hour TMC data. Designed to mimic the legacy data tables for backwards compatibility.

  - `tmc_metadata_legacy`
    - includes additional metadata like centreline, geometry, corresponding study request, and human-readable location name
  - `tmc_study_legacy`

#### New ATRs <!-- omit in toc -->
Recent ATRs (May 2022 and on) loaded through new mechanisms. Includes speed and volume data only. Designed to mimic the legacy data tables for backwards compatibility.

  - `atr_metadata`
    - includes additional metadata like centreline, geometry, corresponding study request, and human-readable location name
  - `atr_study`

## Relevant Tables




## Useful Views

- `traffic.tmc_miovision_long_format` - Takes the wide TMC table `traffic.det` and transforms it into a long format designed to be integrated with miovision-derived TMCs as in `miovision_api.volumes_15min_mvt`. 

- `traffic.artery_locations_px` -  A lookup view between artery codes and px numbers (intersections). Created using MOVE's `traffic.traffic_signals` lookup. 

- `traffic.artery_objectid_pavement_asset` - A lookup view between artery codes and objectid. Used, for example, to link an arterycode to pavement asset information in vz_analysis.gcc_pavement_asset. This view uses the intermediate table `gis_shared_streets.centreline_pavement_180430` which was last updated three years ago and it will be updated via issue [Update pavement assets #620](https://github.com/CityofToronto/bdit_data-sources/issues/620).

- `gis_core.centreline_leg_directions` - Maps the four cardinal directions (N, S, E, & W) referenced by TMCs onto specific edges of the centreline network for all 3- & 4-legged intersections.

- `traffic.svc_centreline_directions` - Maps the four cardinal directions (NB, SB, EB, & WB) referenced by SVCs onto specific directions of travel along edges of the centreline network.

## Cycling Seasonality Adjustment

A model was developed to adjust cycling counts for before after evaluations of new infrastructure based on sparse counts. It can be found in the [`cycling_seasonality`](cycling_seasonality/) folder

## What's the old FLOW Oracle Schema?

This is documented in [`traffic_archive`/](traffic_archive) folder