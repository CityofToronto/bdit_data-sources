- [Introduction](#introduction)
  - [RODARS vs RODARS New ("rodars\_new\_approved")](#rodars-vs-rodars-new-rodars_new_approved)
  - [What's included?](#whats-included)
- [Querying RODARs Data](#querying-rodars-data)
  - [Data Dictionary](#data-dictionary)
    - [`congestion_events.rodars_locations`](#congestion_eventsrodars_locations)
      - [`rodars_issue_locations.lanesaffectedpattern`](#rodars_issue_locationslanesaffectedpattern)
- [Data Ops](#data-ops)
  - [RODARS DAG](#rodars-dag)
  - [`lanesaffected`](#lanesaffected)


# Introduction

> [!IMPORTANT]  
> The city website gives a good overview of RoDARS (here/below): [Road Disruption Activity Reporting System (RoDARS)](https://www.toronto.ca/services-payments/streets-parking-transportation/road-restrictions-closures/road-disruption-activity-reporting-system-rodars/)

> RoDARS is a system that informs the public of planned roadway closures throughout the City. The submission procedure follows the acquisition of an approved [Street Occupation Permit](https://www.toronto.ca/?page_id=80501) (construction) or [Street Closure Permit](https://www.toronto.ca/?page_id=84975) (event).
> 
> When occupying any portion of the City’s public right of way that is not an expressway, the applicant must submit a [**RoDARS Notification Form**](https://www.toronto.ca/wp-content/uploads/2019/03/8de1-TS_Fillable-RoDARS-Form.pdf) to TMC Dispatch at least two business days before the start of occupation.
> The RoDARS Notification Form must be approved by the appropriate Work Zone Traffic Coordinator (WZTC) before being submitted to TMC Dispatch.
> 
> When occupying any portion of a City expressway (F.G.G., DVP or Allen Rd between Eglinton Ave W and Transit Rd), the applicant must submit a RoDARS Notification Form to TMC Dispatch at least seven business days before the start of occupation. The RoDARS Notification Form must be approved by the appropriate City project manager/engineer before submittal to TMC Dispatch. Once attained from TMC Dispatch, TMC’s RESCU Unit will then notify the applicant of the approval verdict.
> 
> A separate RoDARS Notification Form is required for each occupied roadway. If the daily schedule varies, separate RoDARS Notification Forms are required for each day. Once the RoDARS form has been submitted and approved, the information then appears on the [Traffic Restrictions Map](https://www.toronto.ca/?page_id=63656). Please refer to the [City Expressway Closure Guidelines](https://www.toronto.ca/wp-content/uploads/2017/11/9184-0_RoDARS-City-Expressway-Closure-Guidelines-a.pdf) for allowable roadway occupancy times.
> 
> The applicant must notify the City if either of the following situations arise:
> 
>  1. the work schedule and/or work zone plan has been revised or postponed. The applicant must submit a revised and approved RoDARS Notification Form at least one business day before changes occur
>  2. the work has been cancelled or completed early. The applicant must contact TMC Dispatch

> [!TIP]  
> The RoDARS form is public here: https://rodars.transnomis.com/Permit/PermitApplicationCreate/a9180443-b97f-548e-ae1c-fc70cae18a7a?previewMode=Applicants

Here is a screenshot of the extremely detailed geographic/lane management plan UI (which you can access at the link above):
![Rodars Form](rodars_form.png)

## RODARS vs RODARS New ("rodars_new_approved")

> [!IMPORTANT]  
> In 2024 a new version of RODARS debuted which should result in a more reliable data source.  

**RODARS (New)**
- RODARs New has only been around since 2024-03 (already has more than 28,000 issues!)
- An online form which contractors fill out directly. Approvals are done by work zone coordinators.
- QR codes will start appearing at sites in 2024/2025, which should help enforceability (citizen reporting/bylaw officers). 
  - There will be penalties.
- Most records have `centreline_id`!
- Contains detailed description of lane closure pattern (`lanesaffectedpattern`).

**RODARS (Old)**
- Apparently fax was involved and not all forms were processed = completeness is a concern. 
- `centreline_id` was introduced later in the lifespan of original RODARS (Only about 1/3 of those records have a centreline_id, starting from 2021-09).

Here is a small comparison of the data of the new and old RODARS (differentiated by `divisionid` / `divisionname` as seen below): 

| "divisionid" | "divisionname"        | "avg_actual_duration"     | "avg_proposed_duration"   | "min_starttimestamp" | "max_starttimestamp" | "count" | "has_centreline_id" | "start_centreline"           |
|--------------|-----------------------|---------------------------|---------------------------|----------------------|----------------------|---------|---------------------|------------------------------|
| 8014         | "RODARS"              | "15 days 28:43:05.992087" | "15 days 09:49:54.340779" | "1930-08-31"         | "2024-12-19"         | 366100  | 99119               | "2021-09-27 20:55:57.855961" |
| 8048         | "rodars_new_approved" | "20 days 24:26:34.079984" | "18 days 12:11:21.306625" | "2024-03-06"         | "2024-12-19"         | 28418   | 27837               | "2024-03-06 09:48:30.392945" |

```sql
SELECT
    divisionid,
    divisionname,
    AVG(actual_duration) AS avg_actual_duration,
    AVG(proposed_duration) AS avg_proposed_duration,
    MIN(starttimestamp::date) AS min_starttimestamp,
    MAX(starttimestamp::date) AS max_starttimestamp,
    COUNT(*),
    COUNT(*) FILTER (WHERE centreline_id IS NOT NULL) AS has_centreline_id,
    MIN(starttimestamp) FILTER (WHERE centreline_id IS NOT NULL) AS start_centreline
FROM congestion_events.rodars_locations
GROUP BY 1, 2 ORDER BY 1, 2;
```

## What's included?
- As noted in [the intro](#introduction), both construction and events (eg. parades, marathons) are included. 
- Emergency utilities - maybe included. 
- Notably, CafeTO is not included (As at EOY 2024). 

# Querying RODARs Data

> [!WARNING]
> Use of this data is largely untested. If you have an example of use, please help others by proposing a change to this readme!

You could query construction along specific centreline_ids and during specific dates using this sample query. 
```sql
WITH target_geom AS (
    SELECT unnest(links)::integer AS centreline_id
    FROM gis_core.get_centreline_btwn_intersections(13466931, 13463747)
)

--visualizing results in dbeaver is preferable to pgadmin due to better handling of overlapping geoms.
SELECT rl.*
FROM congestion_events.rodars_locations AS rl
JOIN target_geom USING (centreline_id)
WHERE tsrange(starttimestamp, endtimestamp, '[)') @> tsrange('2024-12-01', '2025-01-01', '[)')
```

Or you could cast a wider net by using `st_intersects` to also include intersecting cross streets:
```sql
WITH target_geom AS (
    SELECT * FROM gis_core.get_centreline_btwn_intersections(13466931, 13463747)
)

SELECT rl.*
FROM congestion_events.rodars_locations AS rl, target_geom
WHERE
    tsrange(starttimestamp, endtimestamp, '[)') @> tsrange('2024-12-01', '2025-01-01', '[)')
    AND st_intersects(rl.centreline_geom, target_geom.geom)
```

> [!CAUTION]
> For older time periods you may need to use `issue_geom` instead as `centreline_id` is not consistently populated.

## Data Dictionary

### `congestion_events.rodars_locations`
This view joins together issue metadata from `congestion_events.rodars_issues` and location descriptions from `congestion_events.rodars_issue_locations`.  


| Column                       | Sample                                                              | Description                                                                                                                                                                                           |
|------------------------------|---------------------------------------------------------------------|-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| divisionid                   | 8048                                                                | 8014 for RODARS, 8048 for "rodars_new_approved"                                                                                                                                                       |
| divisionname                 | rodars_new_approved                                                 | Division name                                                                                                                                                                                         |
| issueid                      | 9535420                                                             | An issueid may have more than one location in this table.                                                                                                                                             |
| issuetype                      | 3                                                                 | See description in next column. table.                                                                                                                                             |
| issuetype_desc                    | City Transit Work                                              | Issue description identified from ITS Central; may be useful for location specific issues. |
| sourceid                     | Tor-RDAR2024-3658                                                   | This ID is helpful for searching on ITS Central                                                                                                                                                       |
| description                  | City of Toronto Contract 24ECS-LU-08SU                              | This contract ID will yield helpful results on Google.                                                                                                                                                |
| priority                     | High                                                                | Critical/High/Medium/Low/None                                                                                                                                                                         |
| status                       | In Progress                                                         | Future/In Progress/Ended/Cancelled/Overdue                                                                                                                                                            |
| starttimestamp               | 10/02/2024 17:43                                                    |                                                                                                                                                                                                       |
| endtimestamp                 |                                                                     | null if on-going.                                                                                                                                                                                     |
| actual_duration              |                                                                     | endtimestamp - starttimestamp                                                                                                                                                                         |
| proposedstarttimestamp       | 10/01/2024 7:00                                                     |                                                                                                                                                                                                       |
| proposedendtimestamp         | 10/31/2025 7:00                                                     |                                                                                                                                                                                                       |
| proposed_duration            | 395 days                                                            | proposedendtimestamp - proposedstarttimestamp                                                                                                                                                         |
| proposedstart_tod            | 7:00:00                                                             | The time of day, extracted from the proposedstarttimestamp (and in   conjuction with "recurrence_schedule" field) seems to indicate the   working hours in some cases, but is applied inconsistently. |
| proposedend_tod              | 7:00:00                                                             | The time of day, extracted from the proposedendtimestamp (and in   conjuction with "recurrence_schedule" field) seems to indicate the   working hours in some cases, but is applied inconsistently.   |
| recurrence_schedule          | Weekdays                                                            | Continuous/Daily/Weekdays/Weekends/Activity Schedule                                                                                                                                                  |
| location_description         | Harbord St at James Hales Lane                                      | A high level location                                                                                                                                                                                 |
| mainroadname                 | Harbord St                                                          |                                                                                                                                                                                                       |
| fromroadname                 | James Hales Lane                                                    |                                                                                                                                                                                                       |
| toroadname                   |                                                                     |                                                                                                                                                                                                       |
| streetnumber                 |                                                                     |                                                                                                                                                                                                       |
| locationblocklevel           | Full Closure with Emergency Access                                  | This field is the highest level "closure" description and seems   to frequently contradict lower level descriptions, as in this case.                                                                 |
| roadclosuretype_desc         | No Closure                                                          | Another "closure" description. Contradicts above in this case.                                                                                                                                        |
| locationdescription_toplevel | Harbord St at James Hales Lane                                      | Another high level description                                                                                                                                                                        |
| direction                    | West                                                                | Each direction on the affected street will have it's own entry.                                                                                                                                       |
| roadname                     | Harbord St                                                          | lowest level description; roadname corresponding to centreline.                                                                                                                                       |
| centreline_id                | 14021560                                                            |                                                                                                                                                                                                       |
| centreline_geom              | 0102000020E61000000200000…                                          | centreline_latest geom, or the most recent geom ordered by version_date   DESC in gis_core.centreline. Note this is not always populated, especially   for older entries.                             |
| issue_geom                   | 0101000020E610000035B56CA…                                          |                                                                                                                                                                                                       |
| linear_name_id               | 3605                                                                | Also from the centreline.                                                                                                                                                                             |
| lanesaffectedpattern         | TOLOWO                                                              | `lanesaffectedpattern` describes the road closures in most detail and   seem to be the most accurate. This code is deciphered in the next 9 columns.                                                  |
| lap_descriptions             | {"Sidewalk Open","Normal Open","Lane Open   Traffic Opposite Side"} | The description of each 2 letter code from `lanesaffectedpattern`.                                                                                                                                    |
| lane_open_auto               | 2                                                                   | These next 8 columns are a numeric description of the road closure from   `lanesaffectedpattern`. Any partial closures are coded as 0.5 open/0.5   closed. See notes [here](#lanesaffectedpattern).                                           |
| lane_closed_auto             | 0                                                                   |                                                                                                                                                                                                       |
| lane_open_bike               | 0                                                                   |                                                                                                                                                                                                       |
| lane_closed_bike             | 0                                                                   |                                                                                                                                                                                                       |
| lane_open_ped                | 1                                                                   |                                                                                                                                                                                                       |
| lane_closed_ped              | 0                                                                   |                                                                                                                                                                                                       |
| lane_open_bus                | 0                                                                   |                                                                                                                                                                                                       |
| lane_closed_bus              | 0                                                                   |                                                                                                                                                                                                       |

#### `rodars_issue_locations.lanesaffectedpattern`

This column offers the most detailed look at the lane closures. However is has been under development since 2022 and some codes did not enter use until later. For example; Sidewalk closures were not noted until 2023-01-18 and the first bike lane closure on 2023-03-03. They may have not entered regular use until later. 

```sql
--query to identify first occurence of each lanesaffectedpattern code.
SELECT lap.code, lanesaffectedpattern.lane_status, MIN(timestamputc) AS "min(timestamputc)"
FROM congestion_events.rodars_issue_locations,
UNNEST(regexp_split_to_array(lanesaffectedpattern, E'(?=(..)+$)')) AS lap(code)
JOIN itsc_factors.lanesaffectedpattern ON lap.code = lanesaffectedpattern.code
WHERE lanesaffectedpattern <> ''
GROUP BY 1, 2
ORDER BY 3
```

|code|lane_status                             |min(timestamputc)      |
|---|-----------------------------------------|-----------------------|
LO  |Normal Open                              |2022-05-03 12:16:07.072|
FO  |Left Turn Open                           |2022-05-03 12:16:07.072|
LC  |Normal Closed                            |2022-11-16 23:25:27.637|
SC  |Shoulder Closed                          |2022-12-09 05:24:24.713|
UC  |Buffer Closed                            |2022-12-14 00:00:34.325|
TO  |Lane Open Traffic Opposite Side          |2022-12-14 00:00:34.325|
TA  |Lane Open Traffic Alternating            |2023-01-05 14:54:09.856|
RC  |Right Turn Closed                        |2023-01-18 21:00:17.778|
WC  |Sidewalk Closed                          |2023-01-18 21:00:17.778|
FC  |Left Turn Closed                         |2023-01-31 21:00:18.192|
KC  |Bicycle Closed                           |2023-03-03 05:01:08.396|
RO  |Right Turn Open                          |2023-05-23 19:00:12.517|
UO  |Buffer Open                              |2023-07-18 20:00:53.965|
WO  |Sidewalk Open                            |2023-10-30 18:00:10.039|
SO  |Shoulder Open                            |2023-11-04 12:00:14.511|
WP  |Sidewalk Closed – Protected Path Provided|2023-11-09 19:23:56.367|
KM  |Bicycle Closed – Merge with other traffic|2023-11-24 21:00:43.557|
KD  |Bicycle Closed – Detour Path Provided    |2023-12-08 00:00:40.657|
WD  |Sidewalk Closed – Detour Path Provided   |2023-12-08 00:00:40.657|
KP  |Bicycle Closed – Protected Path Provided |2023-12-21 00:00:42.532|
KO  |Bicycle Open                             |2024-04-04 19:30:37.703|
HC  |HOV Closed                               |2024-05-10 21:00:13.375|
HO  |HOV Open                                 |2024-06-25 20:00:14.220|

# Data Ops

- Note there is a data dictionary from the vendor saved here: `K:\tra\GM Office\Big Data Group\Data Sources\RODARS\Municipal 511 Custom Datafeed API Documentation - 2.4 (Toronto).pdf`
  - Resources taken from this data dictionary are saved in the `itsc_factors` schema.
- Other codes were deciphered manually using ITS Central. 

## RODARS DAG
`rodars_pull` DAG runs on Morbius in order to access ITS Central database. See code here: [rodars_pull.py](../../dags/rodars_pull.py).

<!-- rodars_pull_doc_md -->

- `pull_rodars_issues`: pulls RODARS issue data from ITSC and inserts into RDS.
- `pull_rodars_locations`: pulls RODARS issue location data from ITSC and inserts into RDS.

<!-- rodars_pull_doc_md -->

## `lanesaffected`

`lanesaffected` is a loosely formatted json column in the ITSC issuelocationnew table. 

Notes: 
- This field is unpacked with `process_lanesaffected` function in [rodars_functions.py](./rodars_functions.py) and converted to tabular format. There are lots of near duplicate records that get unpacked from this column, which prevents any meaningful unique constraints on the `congestion_events.rodars_issue_locations` table. 
- Some of the same fields names are used in the top level and the nested json, `LaneApproaches`, eg. `RoadClosureType`. The `_toplevel` suffix is used in `congestion_events.rodars_issue_locations` for the top level fields. 
  - It is assumed the lower level details are more descriptive when available. 
- FeatureId = centreline_id!
- `LanesAffectedPattern` is a code describing lane closures. In `congestion_events.rodars_issue_locations` it is converted to numeric columns: `lane_open_auto, lane_closed_auto, lane_open_bike, lane_closed_bike, lane_open_ped, lane_closed_ped, lane_open_bus, lane_closed_bus`
  
Sample:
```
`lanesaffected`: "{""LocationDescription"":""Huron St from Harbord St to Classic Ave"",""EncodedCoordinates"":""{_oiGpyrcNrDoA"",""LaneApproaches"":[{""Direction"":3,""RoadName"":""Huron St"",""FeatureId"":1143425,""RoadId"":3716,""LanesAffectedPattern"":""LOWO"",""LaneBlockLevel"":2,""RoadClosureType"":20},{""Direction"":2,""RoadName"":""Huron St"",""FeatureId"":1143425,""RoadId"":3716,""LanesAffectedPattern"":""LOWO"",""LaneBlockLevel"":2,""RoadClosureType"":20}],""LocationBlockLevel"":3,""RoadClosureType"":20}"
```