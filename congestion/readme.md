- [`here_agg` Tables](#here_agg-tables)
    - [`here_agg.raw_segments` (partitioned table)](#here_aggraw_segments-partitioned-table)
    - [`here_agg.hourly_avg_tt` (table)](#here_agghourly_avg_tt-table)
    - [`here_agg.monthly_segment_vkt` (table)](#here_aggmonthly_segment_vkt-table)
    - [`here_agg.segment_overnight_tts` (table)](#here_aggsegment_overnight_tts-table)
    - [`here_agg.area_tti` (table)](#here_aggarea_tti-table)


# `here_agg` Tables

### `here_agg.raw_segments` (partitioned table)
This table stores raw dynamic bin observations for segments on the congestion network. It is populated each day by `here_dynamic_binning_agg_hm`.

Approx row count:          657,039,200
| Column Name   | Data Type                   | Sample                                     | Comments                                                                                                                                                                                                  |
|---------------|-----------------------------|--------------------------------------------|-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| segment_id    | integer                     | 2                                          |                                                                                                                                                                                                           |
| dt            | date                        | 2023-12-13                                 | The date of aggregation for the record. Records may not overlap dates.                                                                                                                                    |
| bin_start     | timestamp without time zone | 2023-12-13 05:15:00                        | The start of the observation. It is recommended to use `hr` to group the bin instead. This column is used in the primary key, although the main constraint occurs during insert (non overlapping ranges). |
| bin_range     | tsrange                     | [2023-12-13 05:15:00, 2023-12-13 05:20:00) | Bin range. An exclusion constraint on a temp table prevents overlapping ranges during insert.                                                                                                             |
| tt            | real                        | 20.63033                                   | Travel time in seconds.                                                                                                                                                                                   |
| num_obs       | real                        | 1.0                                        | The vehicle-distance travelled (using sample_size from here.ta_path) divided by the segment length, for the approximate number of vehicles travelling the segment.                                        |
| hr            | smallint                    | 5                                          | The hour the majority of the record occured in. Ties are rounded up.                                                                                                                                      |

### `here_agg.hourly_avg_tt` (table)
This table stores the hourly average travel time, calculated from `here_agg.raw_segments`. It is used to calculate the TTI for each hour. It is populated by the function `here_agg.hourly_avg_tt_agg`. 

Approx row count:           73,837,100
| Column Name   | Data Type        | Sample             | Comments   |
|---------------|------------------|--------------------|------------|
| segment_id    | integer          | 1231               |            |
| dt            | date             | 2024-12-01         |            |
| hr            | smallint         | 1                  |            |
| avg_tt        | double precision | 38.936590830485024 |            |

### `here_agg.monthly_segment_vkt` (table)
This table stores the monthly segment sample sizes for use in weighting segment level TTI. 
Note: We use the previous 6 months VKT to weight TTI.
This table is populated by the function `here_agg.monthly_segment_vkt_agg`. 

Approx row count:            1,251,900
| Column Name   | Data Type                   | Sample              | Comments   |
|---------------|-----------------------------|---------------------|------------|
| mnth          | timestamp without time zone | 2024-02-01 00:00:00 | The month for which the 6 month lookback applies. January 2026 = July 2025 to December 2025           |
| segment_id      | bigint                        | 7800          |            |
| ver_id        | text                        | 24_4                |            |
| highway        | boolean                        | False                |            |
| sample_size   | numeric                      | 5634                |            |
| vkt_km   | double precision                      | 5634                |            |
| sqrt_vkt_km   | double precision                      | 5634                |            |


### `here_agg.segment_overnight_tts` (table)
This table stores the segment level overnight average travel times for previous 6 months. The table is populated by the function `here_agg.agg_overnight_tt`. 

Approx row count:              111,000
| Column Name              | Data Type   | Sample     | Comments   |
|--------------------------|-------------|------------|------------|
| segment_id               | integer     | 2          |            |
| mnth                     | date        | 2024-07-01 |            |
| overnight_avg_tt         | real        | 43.32359   |            |
| rolling_6month_quasi_obs | integer     | 415        |            |

### `here_agg.area_tti` (table)
This table stores the daily-hourly TTI for different areas & road categories. The table is populated by the function `here_agg.area_tti_agg`. 

Approx row count:              242,800
| Column Name   | Data Type        | Sample             | Comments   |
|---------------|------------------|--------------------|------------|
| area_name     | text             | Citywide           |            |
| dt            | date             | 2024-07-01         |            |
| hr            | smallint         | 0                  |            |
| tti           | double precision | 1.0812660631815794 |            |
| num_segments  | integer          | 4777               |            |
| road_category | text             | Non-Highway        |            |
