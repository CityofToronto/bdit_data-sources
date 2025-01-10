This is a readme to describe the complex query [here](./select-congestion_raw_segments.sql).  
Samples from each of the CTEs are shown for one segment/time_grp. Not all columns are shown from each CTE result.  

### time_bins
Contains hourly and period definitions, known as `time_grp`s. These define the extents within which to evaluate dynamic bin options. A dynamic bin must be fully within the time_grp. 

```sql
WITH time_bins AS (
    SELECT
        start_time,
        start_time + '1 hour'::interval AS end_time,
        tsrange(start_time, start_time + '1 hour'::interval, '[)') AS time_grp
    FROM generate_series(
        '2025-01-04'::date,
        '2025-01-04'::date + interval '23 hours',
        '1 hour'::interval
    ) AS hours(start_time)
    UNION
    SELECT
        start_time + '2025-01-04'::date,
        end_time + '2025-01-04'::date,
        tsrange(start_time + '2025-01-04'::date, end_time + '2025-01-04'::date, '[)')     
    FROM (
        VALUES
            ('07:00'::time, '10:00'::time),
            ('10:00', '16:00'),
            ('16:00', '19:00')
        ) AS time_periods(start_time, end_time)
    ORDER BY start_time
),
```

| "start_time"          | "end_time"            | "time_grp"                                          |
|-----------------------|-----------------------|-----------------------------------------------------|
| "2025-01-04 00:00:00" | "2025-01-04 01:00:00" | "[""2025-01-04 00:00:00"",""2025-01-04 01:00:00"")" |
| "2025-01-04 01:00:00" | "2025-01-04 02:00:00" | "[""2025-01-04 01:00:00"",""2025-01-04 02:00:00"")" |
| "2025-01-04 02:00:00" | "2025-01-04 03:00:00" | "[""2025-01-04 02:00:00"",""2025-01-04 03:00:00"")" |
| "2025-01-04 03:00:00" | "2025-01-04 04:00:00" | "[""2025-01-04 03:00:00"",""2025-01-04 04:00:00"")" |
| "2025-01-04 04:00:00" | "2025-01-04 05:00:00" | "[""2025-01-04 04:00:00"",""2025-01-04 05:00:00"")" |
| "2025-01-04 05:00:00" | "2025-01-04 06:00:00" | "[""2025-01-04 05:00:00"",""2025-01-04 06:00:00"")" |
| "2025-01-04 06:00:00" | "2025-01-04 07:00:00" | "[""2025-01-04 06:00:00"",""2025-01-04 07:00:00"")" |
| "2025-01-04 07:00:00" | "2025-01-04 08:00:00" | "[""2025-01-04 07:00:00"",""2025-01-04 08:00:00"")" |
| "2025-01-04 07:00:00" | "2025-01-04 10:00:00" | "[""2025-01-04 07:00:00"",""2025-01-04 10:00:00"")" |
| "2025-01-04 08:00:00" | "2025-01-04 09:00:00" | "[""2025-01-04 08:00:00"",""2025-01-04 09:00:00"")" |

### segment_5min_bins
In this step we pull the relevant data from `here.ta_path` for each segment / time_grp. We save the disaggregate travel time data by link in 3 arrays (link_dirs, tts, lengths), so that in future steps we can reaggregate average segment travel time and distinct length over different ranges without referring back to the here.ta_path table. The time bins (`tx`) are also ranked to make it easier to enumerate possible bin extents using generate_series in the next step. 

```sql
segment_5min_bins AS (
    SELECT
        segments.segment_id,
        tb.time_grp,
        ta.tx,
        RANK() OVER w AS bin_rank,
        segments.total_length,
        SUM(links.length) / segments.total_length AS sum_length,
        SUM(links.length) AS length_w_data,
        SUM(links.length / ta.mean * 3.6) AS unadjusted_tt,
        SUM(sample_size) AS num_obs,
        ARRAY_AGG(ta.link_dir ORDER BY link_dir) AS link_dirs,
        ARRAY_AGG(links.length / ta.mean * 3.6 ORDER BY link_dir) AS tts,
        ARRAY_AGG(links.length ORDER BY link_dir) AS lengths
    FROM here.ta_path AS ta
    JOIN time_bins AS tb ON ta.tx >= tb.start_time AND ta.tx < tb.end_time
    JOIN congestion.network_links_23_4_geom AS links USING (link_dir)
    JOIN congestion.network_segments_23_4_geom AS segments USING (segment_id)
    WHERE ta.dt = '2025-01-04'
        --AND tx < '2025-01-04 01:00:00'
        AND segment_id = 29 AND date_trunc('hour', ta.tx) = '2025-01-04 00:00:00'
    GROUP BY
        segments.segment_id,
        tb.time_grp,
        ta.tx,
        segments.total_length
   WINDOW w AS (
        PARTITION BY segments.segment_id, tb.time_grp
        ORDER BY ta.tx
   )
),
```

`SELECT bin_rank, tx, round(sum_length, 2) AS sum_length, link_dirs, tts FROM segment_5min_bins;`

| "bin_rank" | "tx"                  | "sum_length" | "link_dirs"                                                                                               | "tts"                                                                                                                                                                                                         |
|------------|-----------------------|--------------|----------------------------------------------------------------------------|--------------------------------------------------------------------------------------------------------------------------------------------------|
| 1          | "2025-01-04 00:00:00" | 1.01         | "{1000589822T,1000589823T,1280577167T,792343539T, 792343541T, 836248875T,836248876T,845737718T,845737719T}" | {4.59624489795918360,1.274693877551020408164, 4.96575000000000000,6.68329411764705876, 1.101306122448979591836, 1.526693877551020408164,1.196816326530612244884,4.79172413793103452,9.12626086956521724}         |
| 2          | "2025-01-04 00:05:00" | 0.12         | "{845737718T}"                                                                                            | {19.85142857142857148}                                                                                                                                                                                        |
| 3          | "2025-01-04 00:15:00" | 0.07         | "{1280577167T}"                                                                                           | {2.787789473684210526300}                                                                                                                                                                                     |
| 4          | "2025-01-04 00:50:00" | 0.39         | "{845737718T,845737719T}"                                                                                 | {34.74000000000000000,28.62327272727272724}                                                                                                                                                                   |
| 5          | "2025-01-04 00:55:00" | 1.01         | "{1000589822T,1000589823T,1280577167T,792343539T,792343541T, 836248875T,836248876T,845737718T,845737719T}" | {5.17737931034482764,1.435862068965517241376,1.826482758620689655172,3.91779310344827580,1.240551724137931034496,1.719724137931034482752, 1.348137931034482758612,2.459469026548672566372,6.42563265306122460} |

### dynamic_bin_options
Here we enumerate all the possible dynamic bin options for each starting point. The number of combinations are cut down significantly with the `CASE` statements inside the `generate_series`: 
- Don't enumerate options for 5min bins with sufficient length.
- Only look forward until the next 5min bin with sufficient lenght.

```sql
dynamic_bin_options AS (
    --within each segment/hour, generate all possible forward looking bin combinations
    --don't generate options for bins with sufficient length
    --also don't generate options past the next bin with 80% length
    SELECT
        tx,
        time_grp,
        segment_id,
        bin_rank AS start_bin,
        --generate all the options for the end bin within the group.
        generate_series(
            CASE
                WHEN sum_length >= 0.8 THEN bin_rank
                --if length is insufficient, need at least 1 more bin
                ELSE LEAST(bin_rank + 1, MAX(bin_rank) OVER w)
            END,
            CASE
                --dont need to generate options when start segment is already sufficient
                WHEN sum_length >= 0.8 THEN bin_rank
                --generate options until 1 bin has sufficient length, otherwise until last bin in group
                ELSE COALESCE(MIN(bin_rank) FILTER (WHERE sum_length >= 0.8) OVER w, MAX(bin_rank) OVER w)
            END,
            1
        ) AS end_bin
    FROM segment_5min_bins
    WINDOW w AS (
        PARTITION BY time_grp, segment_id
        ORDER BY tx
        --look only forward for end_bin options
        RANGE BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING
    )
),
```

In this case we find 8 dynamic bin options with the pruning conditions, down from max of 5+4+3+2+1 = 15.

| "tx"                  | "time_grp"                                          | "segment_id" | "start_bin" | "end_bin" |
|-----------------------|-----------------------------------------------------|--------------|-------------|-----------|
| "2025-01-04 00:00:00" | "[""2025-01-04 00:00:00"",""2025-01-04 01:00:00"")" | 29           | 1           | 1         |
| "2025-01-04 00:05:00" | "[""2025-01-04 00:00:00"",""2025-01-04 01:00:00"")" | 29           | 2           | 3         |
| "2025-01-04 00:05:00" | "[""2025-01-04 00:00:00"",""2025-01-04 01:00:00"")" | 29           | 2           | 4         |
| "2025-01-04 00:05:00" | "[""2025-01-04 00:00:00"",""2025-01-04 01:00:00"")" | 29           | 2           | 5         |
| "2025-01-04 00:15:00" | "[""2025-01-04 00:00:00"",""2025-01-04 01:00:00"")" | 29           | 3           | 4         |
| "2025-01-04 00:15:00" | "[""2025-01-04 00:00:00"",""2025-01-04 01:00:00"")" | 29           | 3           | 5         |
| "2025-01-04 00:50:00" | "[""2025-01-04 00:00:00"",""2025-01-04 01:00:00"")" | 29           | 4           | 5         |
| "2025-01-04 00:55:00" | "[""2025-01-04 00:00:00"",""2025-01-04 01:00:00"")" | 29           | 5           | 5         |

### unnested_db_options
Combining the previous two steps, we have enumerated all the possible bin start/end ranges (`dynamic_bin_options`), now we can unnest the disaggregate data (`segment_5min_bins`) and evaluate them. 
Note the multiple arrays unnested at once into rows, see `unnest ( anyarray, anyarray [, ... ] ) ` [here](https://www.postgresql.org/docs/current/functions-array.html#id-1.5.8.25.6.2.2.19.1.1.1). 
We then group the results by bin / link_dir so we only have the unique length within each bin. 

```sql
unnested_db_options AS (
    SELECT
        dbo.time_grp,
        dbo.segment_id,
        s5b.total_length,
        dbo.tx AS dt_start,
        --exclusive end bin
        MAX(s5b.tx) + interval '5 minutes' AS dt_end,
        unnested.link_dir,
        unnested.len,
        AVG(unnested.tt) AS tt, --avg TT for each link_dir
        SUM(num_obs) AS num_obs --sum of here.ta_path sample_size for each link_dir
    FROM dynamic_bin_options AS dbo
    LEFT JOIN segment_5min_bins AS s5b
        ON s5b.time_grp = dbo.time_grp
        AND s5b.segment_id = dbo.segment_id
        AND s5b.bin_rank >= dbo.start_bin
        AND s5b.bin_rank <= dbo.end_bin,
    --unnest all the observations from individual link_dirs to reaggregate them within new dynamic bin
    UNNEST(s5b.link_dirs, s5b.lengths, s5b.tts) AS unnested(link_dir, len, tt)
    --we need to use nested data to determine length for these multi-period bins
    WHERE dbo.start_bin != dbo.end_bin
    GROUP BY
        dbo.time_grp,
        dbo.segment_id,
        s5b.total_length,
        dbo.tx,
        dbo.end_bin,
        unnested.link_dir,
        unnested.len
)
```

`SELECT dt_start, dt_end, link_dir, len, tt, num_obs FROM unnested_db_options WHERE dt_start = '2025-01-04 00:05:00' AND dt_end = '2025-01-04 01:00:00'`

| "dt_start"            | "dt_end"              | "link_dir"    | "len" | "tt"                     | "num_obs" |
|-----------------------|-----------------------|---------------|-------|--------------------------|-----------|
| "2025-01-04 00:05:00" | "2025-01-04 01:00:00" | "1000589822T" | 62.56 | 5.17737931034482764      | 18        |
| "2025-01-04 00:05:00" | "2025-01-04 01:00:00" | "1000589823T" | 17.35 | 1.435862068965517241376  | 18        |
| "2025-01-04 00:05:00" | "2025-01-04 01:00:00" | "1280577167T" | 22.07 | 2.307136116152450090736  | 20        |
| "2025-01-04 00:05:00" | "2025-01-04 01:00:00" | "792343539T"  | 47.34 | 3.91779310344827580      | 18        |
| "2025-01-04 00:05:00" | "2025-01-04 01:00:00" | "792343541T"  | 14.99 | 1.240551724137931034496  | 18        |
| "2025-01-04 00:05:00" | "2025-01-04 01:00:00" | "836248875T"  | 20.78 | 1.719724137931034482752  | 18        |
| "2025-01-04 00:05:00" | "2025-01-04 01:00:00" | "836248876T"  | 16.29 | 1.348137931034482758612  | 18        |
| "2025-01-04 00:05:00" | "2025-01-04 01:00:00" | "845737718T"  | 38.60 | 19.016965865992414682124 | 21        |
| "2025-01-04 00:05:00" | "2025-01-04 01:00:00" | "845737719T"  | 87.46 | 17.52445269016697592     | 20        |

### Insert statement
Here we find bins with sufficient length, for the two cases: 
- Multiple 5min bins assembled: need to check sufficient length from last step. 
- An original 5min bin, no group by needed to check length. 

```sql
INSERT INTO gwolofs.congestion_raw_segments (
    time_grp, segment_id, dt_start, dt_end, bin_range, tt,
    unadjusted_tt, total_length, length_w_data, num_obs
)
--this query contains overlapping values which get eliminated
--via on conflict with the exclusion constraint on congestion_raw_segments table.
SELECT DISTINCT ON (time_grp, segment_id, dt_start)
    time_grp,
    segment_id,
    dt_start,
    dt_end,
    tsrange(dt_start, dt_end, '[)') AS bin_range,
    total_length / SUM(len) * SUM(tt) AS tt,
    SUM(tt) AS unadjusted_tt,
    total_length,
    SUM(len) AS length_w_data,
    SUM(num_obs) AS num_obs --sum of here.ta_path sample_size for each segment
FROM unnested_db_options AS udbo
GROUP BY
    time_grp,
    segment_id,
    dt_start,
    dt_end,
    total_length
HAVING SUM(len) >= 0.8 * total_length
UNION
--these 5 minute bins already have sufficient length
--don't need to use nested data to validate.
SELECT
    time_grp,
    segment_id,
    tx AS dt_start,
    tx + interval '5 minutes' AS dt_end,
    tsrange(tx, tx + interval '5 minutes', '[)') AS bin_range,
    total_length / length_w_data * unadjusted_tt AS tt,
    unadjusted_tt,
    total_length,
    length_w_data,
    num_obs --sum of here.ta_path sample_size for each segment
FROM segment_5min_bins
--we do not need to use nested data to determine length here.
WHERE sum_length >= 0.8
ORDER BY
    time_grp,
    segment_id,
    dt_start,
    dt_end
--exclusion constraint + ordered insert to prevent overlapping bins
ON CONFLICT ON CONSTRAINT dynamic_bins_unique
DO NOTHING;
```

`SELECT segment_id, bin_range, round(tt, 2) AS tt, total_length, length_w_data FROM inserted;` 

| "segment_id" | "bin_range"                                         | "tt"  | "total_length" | "length_w_data" |
|--------------|-----------------------------------------------------|-------|----------------|-----------------|
| 29           | "[""2025-01-04 00:00:00"",""2025-01-04 00:05:00"")" | 34.93 | 324.33         | 327.44          |
| 29           | "[""2025-01-04 00:05:00"",""2025-01-04 01:00:00"")" | 53.18 | 324.33         | 327.44          |
| 29           | "[""2025-01-04 00:15:00"",""2025-01-04 01:00:00"")" | 52.76 | 324.33         | 327.44          |
| 29           | "[""2025-01-04 00:50:00"",""2025-01-04 01:00:00"")" | 52.29 | 324.33         | 327.44          |
| 29           | "[""2025-01-04 00:55:00"",""2025-01-04 01:00:00"")" | 25.31 | 324.33         | 327.44          |

After insert against exclusion constraint, only 2 remain, since records 3,4,5 overlap with record 2 above. 
`SELECT segment_id, bin_range, round(tt, 2) AS tt, total_length, length_w_data FROM gwolofs.congestion_raw_segments WHERE segment_id = 29 AND time_grp = '["2025-01-04 00:00:00","2025-01-04 01:00:00")'::tsrange`

Constraint: 
```sql
    CONSTRAINT dynamic_bins_unique EXCLUDE USING gist (
        segment_id WITH =,
        bin_range WITH &&,
        time_grp WITH =
    )
```

| "segment_id" | "bin_range"                                         | "tt"  | "total_length" | "length_w_data" |
|--------------|-----------------------------------------------------|-------|----------------|-----------------|
| 29           | "[""2025-01-04 00:00:00"",""2025-01-04 00:05:00"")" | 34.93 | 324.33         | 327.44          |
| 29           | "[""2025-01-04 00:05:00"",""2025-01-04 01:00:00"")" | 53.18 | 324.33         | 327.44          |