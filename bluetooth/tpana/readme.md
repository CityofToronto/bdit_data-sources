# Introduction

A one time extract (received 2026-06-02) of TPANA Detector, Link and Route info was loaded into the database. 

## Tables

### `bluetooth.tpana_detectors` (table)
Detector details extracted from TPANA's Equipment.xml received on 2026-06-02.
Row count:                    120
| Column Name               | Data Type         | Sample                                                    | Comments   |
|---------------------------|-------------------|-----------------------------------------------------------|------------|
| detector_id               | character varying | A1                                                        |            |
| detector_name             | character varying | Allen@Wilson Ave                                          |            |
| short_name                | character varying | Allen@Wils                                                |            |
| equipment_type            | character varying | BluFax                                                    |            |
| latitude                  | double precision  | 43.73195                                                  |            |
| longitude                 | double precision  | -79.44981                                                 |            |
| altitude                  | double precision  | -100.0                                                    |            |
| utc_offset                | integer           | 0                                                         |            |
| allowed_silence_s         | integer           | 1500                                                      |            |
| gapout_time_s             | integer           | 240                                                       |            |
| real_time_stats           | integer           | 0                                                         |            |
| battery_alarm_threshold_v | double precision  | 11.7                                                      |            |
| additional_info           | text              | update BTM-X with new firmware supporting BEACON@20180619 |            |
| comments                  | text              | None                                                      |            |
| geom                      | geometry          | 0101000020E61000007BDAE1AFC9DC53C05227A089B0DD4540        |            |

### `bluetooth.tpana_links` (table)
Link details extracted from TPANA's Equipment.xml received on 2026-06-02.
Row count:                    574
| Column Name          | Data Type         | Sample                       | Comments   |
|----------------------|-------------------|------------------------------|------------|
| link_id              | character varying | LAE_BE-S4                    |            |
| link_name            | character varying | Adelaide_E1:Bathurst-Spadina |            |
| short_name           | character varying | ADD_E1-500                   |            |
| additional_info      | text              | None                         |            |
| src_detector_id      | character varying | BE                           |            |
| dest_detector_id     | character varying | S4                           |            |
| line_distance_m      | double precision  | 613.0                        |            |
| path_distance_m      | double precision  | 613.0                        |            |
| route_direction_name | character varying | E                            |            |
| speed_limit_kmh      | integer           | 50                           |            |
| max_travel_time_s    | integer           | 2400                         |            |
| detection_rules      | character varying | FF                           |            |
| real_time_stats      | character varying | None                         |            |
| comments             | text              | None                         |            |
| extra_info           | text              | None                         |            |

### `bluetooth.tpana_routes` (table)
Route details extracted from TPANA's Equipment.xml received on 2026-06-02.
Row count:                    734
| Column Name     | Data Type         | Sample                             | Comments   |
|-----------------|-------------------|------------------------------------|------------|
| route_id        | character varying | RADE_50-63                         |            |
| route_name      | character varying | Adelaide_EB:University-Bay         |            |
| short_name      | character varying | None                               |            |
| start_offset_m  | double precision  | 0.0                                |            |
| end_offset_m    | double precision  | 0.0                                |            |
| comments        | text              | None                               |            |
| extra_info      | text              | None                               |            |
| links           | text[]            | ['LAE_50-63']                      |            |
| additional_info | text              | adaptive=1 D1=S4 D2=50 D3=63 D4=Y2 |            |

## Sample Queries

Find the text descriptions of TPANA routes/links:
```sql
SELECT
    itp.source_id,
    routes.route_name,
    links.link_name
FROM bluetooth.itsc_tt_paths AS itp
--source_id can be either route_id or link_id:
LEFT JOIN bluetooth.tpana_routes AS routes ON routes.route_id = itp.source_id
LEFT JOIN bluetooth.tpana_links AS links ON links.link_id = itp.source_id
WHERE itp.division_id = 8026
ORDER BY source_id
```
