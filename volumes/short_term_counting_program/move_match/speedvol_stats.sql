/*
This code replicates statistics from the speed volume Speed Percentile Report:
https://move.intra.prod-toronto.ca/view/location/s1:Ab_DAA/POINTS/reports/ATR_SPEED_VOLUME
Note that am and pm peak hours as presented in this report are based on clock face hours
and not 15 minute bins.
*/

-- Grab speed volume study data
WITH spd_vol AS (
    SELECT
        arterydata.arterycode,
        countinfo.count_info_id,
        arterydata.apprdir AS direction,
        arterydata.location,
        cnt_det.timecount AS datetime_bin,
        speed_classes.speed_kph,
        cnt_det.count AS volume_15min
    FROM traffic.arterydata
    JOIN traffic.countinfo USING (arterycode)
    JOIN traffic.cnt_det USING (count_info_id)
    JOIN prj_volume.speed_classes USING (speed_class)
    WHERE
        --filter studies here.
        countinfo.count_date >= '2023-01-01'
        AND countinfo.count_date < '2023-01-15' -- short time period for testing purposes
),

-- Calculate volume percentages and cumulative percentages by speed bin
spd_bin_vol AS (
    SELECT
        spd_vol.arterycode,
        spd_vol.count_info_id,
        spd_vol.direction,
        spd_vol.location,
        spd_vol.datetime_bin::date AS dt,
        spd_vol.speed_kph,
        SUM(spd_vol.volume_15min) AS spd_vol, -- aggregate 15 minute bins into a daily count
        -- SUM(SUM()) explained below
        SUM(SUM(spd_vol.volume_15min)) OVER cumulative
        / (SUM(SUM(spd_vol.volume_15min)) OVER _all + 0.00000001) AS cum_pct,
        SUM(spd_vol.volume_15min) / (SUM(SUM(spd_vol.volume_15min)) OVER _all + 0.00000001) AS pct
    FROM spd_vol
    GROUP BY
        spd_vol.arterycode,
        spd_vol.count_info_id,
        spd_vol.direction,
        spd_vol.location,
        spd_vol.datetime_bin::date,
        spd_vol.speed_kph
    HAVING SUM(spd_vol.volume_15min) > 0
    WINDOW
        cumulative AS (
            PARTITION BY spd_vol.count_info_id
            ORDER BY spd_vol.speed_kph RANGE UNBOUNDED PRECEDING
        ),
        _all AS (PARTITION BY spd_vol.count_info_id)
),

/*
SUM(SUM()) Explained
There are two lines in the CTE above that use a nested aggregate function within a WINDOW function.
A WINDOW function allows calculations to be performed on subsets of data (or WINDOWs of data)
without collapsing the data into aggregated forms (like GROUP BY).
In this CTE, two WINDOWs are used:
    cumulative: creates windows based on count_info_id and speed bin
    _all: creates windows based on count_info_id
Here is an explanation of the first SUM(SUM()) line:
    Part 1 - SUM(SUM(spd_vol.volume_15min)) OVER cumulative: 
        The inside SUM() adds up 15 minute volume data (resulting in daily volumes by speed bin).
        The outside SUM() adds up the bins over the cumulative window.
        In plain language: sum daily totals by speed bin, then calculate a cumulative sum for
        ascending speed bins.
    Part 2 - / (SUM(SUM(spd_vol.volume_15min)) OVER _all + 0.00000001):
        The inside SUM() adds up 15 minute volume data (resulting daily volumes by speed bin).
        The outside SUM() adds up all of the speed bin volumes (resulting in daily volumes).
        In plain language: calculate daily volumes.
    Since Part 1 is divided by Part 2, you end up with cumulative proportions of daily
    volume by speed bin.
Here is an explanation of the second SUM(SUM()) line:
    SUM(spd_vol.volume_15min) / (SUM(SUM(spd_vol.volume_15min)) OVER _all + 0.00000001) AS pct:
    Calculate daily volumes by speed bin and divide it by...
    Daily total volumes (15 minute volume data are aggregated to speed bins in the inside sum, then they
    are totalled in the outside sum).
    You end up with the proportion of daily volume in each speed bin.
*/

-- Calculate 15th percentile speed
per_15 AS (
    SELECT DISTINCT ON (spd_bin_vol.count_info_id)
        spd_bin_vol.count_info_id,
        spd_bin_vol.arterycode,
        spd_bin_vol.dt,
        spd_bin_vol.cum_pct,
        spd_bin_vol.pct,
        LOWER(spd_bin_vol.speed_kph) -- lower speed bound of bin
        + (UPPER(spd_bin_vol.speed_kph) - LOWER(spd_bin_vol.speed_kph)) --range
        * (
            0.15 -- multiplied by the desired percentile (for which you want speed)
            - spd_bin_vol.cum_pct -- the cumulative % for that bin
            + spd_bin_vol.pct -- munis the non-cumulative % from that bin
        )
            -- all dividded by the bin volume % plus that decimal to avoid a divide by zero error
        / spd_bin_vol.pct AS pctile_speed_15
    FROM spd_bin_vol
    WHERE
        spd_bin_vol.cum_pct >= 0.15
        AND pct <> 0
    ORDER BY
        spd_bin_vol.count_info_id,
        spd_bin_vol.cum_pct
),

-- Calculate 50th percentile speed (see comments for 15th percentile
-- speed for an explanation of the calculation)
per_50 AS (
    SELECT DISTINCT ON (spd_bin_vol.count_info_id)
        spd_bin_vol.count_info_id,
        spd_bin_vol.arterycode,
        spd_bin_vol.dt,
        spd_bin_vol.cum_pct,
        spd_bin_vol.pct,
        LOWER(spd_bin_vol.speed_kph)
        + (UPPER(spd_bin_vol.speed_kph) - LOWER(spd_bin_vol.speed_kph))
        * (0.50 - spd_bin_vol.cum_pct + spd_bin_vol.pct)
        / spd_bin_vol.pct AS pctile_speed_50
    FROM spd_bin_vol
    WHERE
        spd_bin_vol.cum_pct >= 0.50
        AND pct <> 0
    ORDER BY
        spd_bin_vol.count_info_id,
        spd_bin_vol.cum_pct
),

-- Calculate 85th percentile speed (see comments for 15th percentile
-- speed for an explanation of the calculation)
per_85 AS (
    SELECT DISTINCT ON (spd_bin_vol.count_info_id)
        spd_bin_vol.count_info_id,
        spd_bin_vol.arterycode,
        spd_bin_vol.dt,
        spd_bin_vol.cum_pct,
        spd_bin_vol.pct,
        LOWER(spd_bin_vol.speed_kph)
        + (UPPER(spd_bin_vol.speed_kph) - LOWER(spd_bin_vol.speed_kph))
        * (0.85 - spd_bin_vol.cum_pct + spd_bin_vol.pct) 
        / spd_bin_vol.pct AS pctile_speed_85
    FROM spd_bin_vol
    WHERE
        spd_bin_vol.cum_pct >= 0.85
        AND pct <> 0
    ORDER BY
        spd_bin_vol.count_info_id,
        spd_bin_vol.cum_pct
),

-- Calculate 95th percentile speed (see comments for 15th percentile
-- speed for an explanation of the calculation)
per_95 AS (
    SELECT DISTINCT ON (spd_bin_vol.count_info_id)
        spd_bin_vol.count_info_id,
        spd_bin_vol.arterycode,
        spd_bin_vol.dt,
        spd_bin_vol.cum_pct,
        spd_bin_vol.pct,
        LOWER(spd_bin_vol.speed_kph)
        + (UPPER(spd_bin_vol.speed_kph) - LOWER(spd_bin_vol.speed_kph))
        * (0.95 - spd_bin_vol.cum_pct + spd_bin_vol.pct)
        / spd_bin_vol.pct AS pctile_speed_95
    FROM spd_bin_vol
    WHERE
        spd_bin_vol.cum_pct >= 0.95
        AND pct <> 0
    ORDER BY
        spd_bin_vol.count_info_id,
        spd_bin_vol.cum_pct
),

-- Calculate daily volumes
daily_vol AS (
    SELECT
        spd_bin_vol.count_info_id,
        spd_bin_vol.arterycode,
        spd_bin_vol.dt,
        SUM(spd_bin_vol.spd_vol) AS dt_vol
    FROM spd_bin_vol AS spd_bin_vol
    GROUP BY
        spd_bin_vol.count_info_id,
        spd_bin_vol.arterycode,
        spd_bin_vol.dt
),

-- Calculate daily volumes by bin and find the midpoint of the speed bins
ave_calcs AS (
    SELECT
        spd_bin_vol.count_info_id,
        spd_bin_vol.arterycode,
        spd_bin_vol.dt,
        spd_bin_vol.speed_kph,
        SUM(spd_bin_vol.spd_vol) AS bin_vol,
        (UPPER(spd_bin_vol.speed_kph) + LOWER(spd_bin_vol.speed_kph)) / 2 AS mid_bin
    FROM spd_bin_vol
    GROUP BY
        spd_bin_vol.count_info_id,
        spd_bin_vol.arterycode,
        spd_bin_vol.dt,
        spd_bin_vol.speed_kph
),

-- Calculate average daily mean speed
ave_vol AS (
    SELECT
        ave_calcs.count_info_id,
        ave_calcs.arterycode,
        ave_calcs.dt,
        ROUND(SUM(ave_calcs.bin_vol * ave_calcs.mid_bin)
        / SUM(ave_calcs.bin_vol), 1) AS mean_spd
    FROM ave_calcs
    GROUP BY
        ave_calcs.count_info_id,
        ave_calcs.arterycode,
        ave_calcs.dt
    HAVING SUM(ave_calcs.bin_vol) > 0
),

-- Calculate hourly sums
hr_sums AS (
    SELECT
        spd_vol.count_info_id,
        spd_vol.arterycode,
        DATE_TRUNC('HOUR', spd_vol.datetime_bin) AS hr,
        SUM(spd_vol.volume_15min) AS hr_vol
    FROM spd_vol
    GROUP BY
        spd_vol.count_info_id,
        spd_vol.arterycode,
        DATE_TRUNC('HOUR', spd_vol.datetime_bin)
),

-- Find AM Peak
am_peak AS (
    SELECT DISTINCT
        spd_bin_vol.count_info_id,
        spd_bin_vol.arterycode,
        spd_bin_vol.dt,
        FIRST_VALUE(hr_sums.hr) OVER cumulative AS am_peak_hr,
        FIRST_VALUE(hr_sums.hr_vol) OVER cumulative AS am_peak_vol
    FROM spd_bin_vol
    LEFT JOIN hr_sums USING (count_info_id)
    WHERE date_part('hour', hr_sums.hr) < 12
    WINDOW cumulative AS (PARTITION BY spd_bin_vol.count_info_id ORDER BY hr_sums.hr_vol DESC)
),

-- Find PM Peak
pm_peak AS (
    SELECT DISTINCT
        spd_bin_vol.count_info_id,
        spd_bin_vol.arterycode,
        spd_bin_vol.dt,
        FIRST_VALUE(hr_sums.hr) OVER cumulative AS pm_peak_hr,
        FIRST_VALUE(hr_sums.hr_vol) OVER cumulative AS pm_peak_vol
    FROM spd_bin_vol
    LEFT JOIN hr_sums USING (count_info_id)
    WHERE date_part('hour', hr_sums.hr) >= 12
    WINDOW cumulative AS (PARTITION BY spd_bin_vol.count_info_id ORDER BY hr_sums.hr_vol DESC)
)

-- Put all the stats together in one big happy table!!!
SELECT DISTINCT
    spd_vol.count_info_id,
    ave_vol.arterycode,
    spd_vol.direction,
    spd_vol.location,
    ave_vol.dt,
    daily_vol.dt_vol,
    am_peak.am_peak_hr,
    am_peak.am_peak_vol,
    pm_peak.pm_peak_hr,
    pm_peak.pm_peak_vol,
    ave_vol.mean_spd,
    ROUND(per_15.pctile_speed_15, 1) AS pctile_speed_15,
    ROUND(per_50.pctile_speed_50, 1) AS pctile_speed_50,
    ROUND(per_85.pctile_speed_85, 1) AS pctile_speed_85,
    ROUND(per_95.pctile_speed_95, 1) AS pctile_speed_95
FROM spd_vol
LEFT JOIN ave_vol USING (count_info_id)
LEFT JOIN per_15 USING (count_info_id)
LEFT JOIN per_50 USING (count_info_id)
LEFT JOIN per_85 USING (count_info_id)
LEFT JOIN per_95 USING (count_info_id)
LEFT JOIN daily_vol USING (count_info_id)
LEFT JOIN am_peak USING (count_info_id)
LEFT JOIN pm_peak USING (count_info_id)
ORDER BY
    ave_vol.arterycode,
    ave_vol.dt;