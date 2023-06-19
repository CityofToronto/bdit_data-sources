# Identifying 'Good Days' of RESCU Data

## Table of Contents
- [Context](#Context)
- [Steps and Methodology](#Steps-and-Methodology)
- [Checks](#Checks)

## Context

This research stems from a request that required all the volume data the City has ever collected. It turns out that there is some funkiness going on with RESCU cameras in particular, so we wanted a quick way to identify which detectors were recording reasonable data on what days (hence "good days" of data).

There are two types of sql files in this folder:
1. sql files that start with 'sD_' (where 'D' is a digit) lay out the steps followed to identify detectors with "good days" of data
2. sql files that start with 'xD_' (where 'D' is a digit) contain checks that allow you to examine the results

Most of this analysis was completed for 2021, so please remember to change the dates in the sql scripts to determine thresholds for other years.

## Steps and Methodology

Here is a description of each of the 'sD_' sql files:
1. [s1_rescu_lane_stats.sql](s1_rescu_lane_stats.sql) calculates the average and median lane volumes for weekdays and weekends for detectors and days with 24 hours (or 96 15-minute bins) of data. Data produced by this sql were used to visualize data and determine thresholds.

When this exercise was first explored in Spring 2023, the lane-level data were graphed in Excel. Separate graphs were made for weekdays and weekends on each of the four corridors (resulting in 8 graphs in total). Based on these graphs, the following lane-level thresholds were established:
    - Allen Expressway Weekdays: 4000 vehicles per lane
    - Allen Expressway Weekends: 3000 vehicles per lane
    - Don Valley Parkway Weekdays: 15000 vehicles per lane
    - Don Valley Parkway Weekends: 10000 vehicles per lane
    - Gardiner Expressway Weekdays: 10000 vehicles per lane
    - Gardiner Expressway Weekends: 10000 vehicles per lane
    - Lakeshore Boulevard Weekdays: 2000 vehicles per lane
    - Lakeshore Bouldevard Weekends: 2000 vehicles per lane

2. [s2_rescu_dayvol_stats_21.sql](s2_rescu_dayvol_stats.sql) calculates the average and median volumes for weekdays and weekends for all non-ramp detectors. The volume counts include data for all lanes in total (not individual lanes).
3. [s3_rescu_enuf_vol.sql](s3_rescu_enuf_vol.sql) uses the statistics calculated in [s2_rescu_dayvol_stats_21.sql](s2_rescu_dayvol_stats.sql) and the thresholds determined based on the statistics calculated in [s2_rescu_dayvol_stats_21.sql](s2_rescu_dayvol_stats.sql) to determine which detectors meet the thresholds on which days.
4. [s4_rescu_good_vol.sql](s4_rescu_good_vol.sql) creates a table that stores 15 minute volume data from RESCU detectors that recorded volumes that met or exceeded the volume thresholds as identified in [s3_rescu_enuf_vol.sql](s3_rescu_enuf_vol.sql).
5. [s5_rescu_det_art.sql](s5_rescu_det_art.sql) creates a table of detectors with missing `arterycodes` or `centreline_ids`
6. [s6_rescu_miss_cids.sql](s6_rescu_miss_cids.sql) fills in the missing `arterycodes` or `centreline_ids` based on a manual process
7. [s7_rescu_data_21.sql](s7_rescu_data.sql) produces a table of RESCU data in 15 minute bins from detectors that met daily volume thresholds, with complete `arterycode` and `centreline_id` data.

## Checks

Here is a description of each of the 'xD_' sql files:
- [x1_rescu_good_vol_days.sql](x1_rescu_good_vol_days.sql) allows you to check how many good days of data each detector recorded. A "good day of data" is defined as a day when the daily volume threshold was met.
- [x2_rescu_gaps.sql](x2_rescu_gaps.sql) allows you to check for gaps in time bins (a missing time bin means there is no data for that 15-minute period). It uses a "gaps and islands" approach to calculate the length of the gaps.


