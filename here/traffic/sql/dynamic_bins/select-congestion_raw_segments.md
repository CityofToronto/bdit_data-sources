This is a readme to describe the complex query [here](./function-congestion_network_segment_agg.sql).  
Samples from each of the CTEs are shown for one segment/time_grp. Not all columns are shown from each CTE result.  

### segments

Identifies the links that make up each segment, along with total segment length from `congestion.network_links_*` table.

```sql
WITH segments AS (
    SELECT
        segment_id,
        link_dir,
        length,
        SUM(length) OVER (PARTITION BY segment_id) AS total_length
    FROM congestion.%2$I --eg. congestion.network_links_23_4_geom
)
```

### segment_5min_bins
In this step we pull the relevant data from `here.ta_path` for each segment / time_grp (gwolofs.congestion_time_grps). We save the disaggregate travel time data by link in 3 arrays (link_dirs, tts, lengths), so that in future steps we can reaggregate average segment travel time and distinct length over different ranges without referring back to the here.ta_path table. The time bins (`tx`) are also ranked to make it easier to enumerate possible bin extents using `generate_series` in the next step. 

```sql
segment_5min_bins AS (
    SELECT
        links.segment_id,
        timerange(tg.start_tod, tg.end_tod, '[)') AS time_grp,
        ta.tx,
        RANK() OVER w AS bin_rank,
        links.total_length,
        SUM(links.length) / links.total_length AS sum_length,
        SUM(links.length) AS length_w_data,
        SUM(links.length / ta.mean * 3.6) AS unadjusted_tt,
        SUM(sample_size) AS num_obs,
        ARRAY_AGG(ta.link_dir ORDER BY link_dir) AS link_dirs,
        ARRAY_AGG(links.length / ta.mean * 3.6 ORDER BY link_dir) AS tts,
        ARRAY_AGG(links.length ORDER BY link_dir) AS lengths
    FROM here.ta_path AS ta
    JOIN gwolofs.congestion_time_grps AS tg ON
        ta.tx >= %1$L::date + tg.start_tod
        AND ta.tx < %1$L::date + tg.end_tod
    JOIN segments AS links USING (link_dir)
    WHERE
        ta.dt >= %1$L::date
        AND ta.dt < %1$L::date + interval '1 day'
    GROUP BY
        links.segment_id,
        tg.start_tod,
        tg.end_tod,
        ta.tx,
        links.total_length
    WINDOW w AS (
        PARTITION BY links.segment_id, tg.start_tod, tg.end_tod
        ORDER BY ta.tx
    )
),
```

`SELECT * FROM gwolofs.congestion_raw_segments WHERE segment_id = 1 AND dt = '2025-01-10' AND time_grp = '[00:00:00,01:00:00)';`

| segment_id | time_grp            | tx                      | bin_rank | total_length | sum_length             | length_w_data | unadjusted_tt            | num_obs | link_dirs                                                     | tts                                                                                           | lengths                        |
|------------|---------------------|-------------------------|----------|--------------|------------------------|---------------|--------------------------|---------|---------------------------------------------------------------|-----------------------------------------------------------------------------------------------|--------------------------------|
|          1 | [00:00:00,06:00:00) | 2025-01-10 00:20:00.000 |        1 |       374.22 | 1.00000000000000000000 |        374.22 | 29.200422445479049559580 |       5 | {1328374158F,1328374159F,1328374160F,1328374165F,1328374166F} | {3.739245283018868,4.845056603773585,1.8298867924528301,3.109090909090909,15.677142857142858} | {55.05,71.33,26.94,38.0,182.9} |
|          1 | [00:00:00,06:00:00) | 2025-01-10 00:25:00.000 |        2 |       374.22 | 0.48874993319437763882 |        182.90 |    131.68800000000000000 |       1 | {1328374166F}                                                 | {131.688}                                                                                     | {182.9}                        |
|          1 | [00:00:00,06:00:00) | 2025-01-10 00:35:00.000 |        3 |       374.22 | 1.00000000000000000000 |        374.22 | 76.657011086474501198040 |       5 | {1328374158F,1328374159F,1328374160F,1328374165F,1328374166F} | {4.833658536585366,6.263121951219512,2.365463414634146,3.3365853658536584,59.85818181818182}  | {55.05,71.33,26.94,38.0,182.9} |
|          1 | [00:00:00,06:00:00) | 2025-01-10 05:00:00.000 |        4 |       374.22 | 0.19060980172091283202 |         71.33 |      6.26312195121951216 |       1 | {1328374159F}                                                 | {6.263121951219512}                                                                           | {71.33}                        |
|          1 | [00:00:00,06:00:00) | 2025-01-10 05:05:00.000 |        5 |       374.22 | 1.00000000000000000000 |        374.22 | 35.452421052631578833688 |       5 | {1328374158F,1328374159F,1328374160F,1328374165F,1328374166F} | {5.215263157894737,6.757578947368421,2.5522105263157893,3.6,17.327368421052633}               | {55.05,71.33,26.94,38.0,182.9} |
|          1 | [00:00:00,06:00:00) | 2025-01-10 05:15:00.000 |        6 |       374.22 | 1.00000000000000000000 |        374.22 | 48.013722580645161104508 |       5 | {1328374158F,1328374159F,1328374160F,1328374165F,1328374166F} | {6.392903225806451,12.8394,3.128516129032258,4.412903225806452,21.24}                         | {55.05,71.33,26.94,38.0,182.9} |
|          1 | [00:00:00,06:00:00) | 2025-01-10 05:20:00.000 |        7 |       374.22 | 0.48874993319437763882 |        182.90 |     13.16880000000000000 |       1 | {1328374166F}                                                 | {13.1688}                                                                                     | {182.9}                        |

### dynamic_bin_options
Here we enumerate all the possible dynamic bin options for each starting point. The number of combinations are cut down significantly with the `CASE` statements inside the `generate_series`: 
- Don't enumerate options for 5min bins with sufficient length.
- Only look forward until the next 5min bin with sufficient lenght.

```sql
dynamic_bin_options AS (
    --within each segment/hour, generate all possible forward looking bin combinations
    --don't generate options for bins with sufficient length
    --also don't generate options past the next bin with 80%% length
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

In this case we find 13 dynamic bin options with the pruning conditions, down from max of 10+9+8+7+6+5+4+3+2+1 = 55.

| tx                      | time_grp            | segment_id | start_bin | end_bin |
|-------------------------|---------------------|------------|-----------|---------|
| 2025-01-10 00:20:00.000 | [00:00:00,06:00:00) |          1 |         1 |       1 |
| 2025-01-10 00:25:00.000 | [00:00:00,06:00:00) |          1 |         2 |       3 |
| 2025-01-10 00:35:00.000 | [00:00:00,06:00:00) |          1 |         3 |       3 |
| 2025-01-10 05:00:00.000 | [00:00:00,06:00:00) |          1 |         4 |       5 |
| 2025-01-10 05:05:00.000 | [00:00:00,06:00:00) |          1 |         5 |       5 |
| 2025-01-10 05:15:00.000 | [00:00:00,06:00:00) |          1 |         6 |       6 |
| 2025-01-10 05:20:00.000 | [00:00:00,06:00:00) |          1 |         7 |       8 |
| 2025-01-10 05:20:00.000 | [00:00:00,06:00:00) |          1 |         7 |       9 |
| 2025-01-10 05:20:00.000 | [00:00:00,06:00:00) |          1 |         7 |      10 |
| 2025-01-10 05:25:00.000 | [00:00:00,06:00:00) |          1 |         8 |       9 |
| 2025-01-10 05:25:00.000 | [00:00:00,06:00:00) |          1 |         8 |      10 |
| 2025-01-10 05:50:00.000 | [00:00:00,06:00:00) |          1 |         9 |      10 |
| 2025-01-10 05:55:00.000 | [00:00:00,06:00:00) |          1 |        10 |      10 |

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
        s5b_end.tx + interval '5 minutes' AS dt_end,
        unnested.link_dir,
        unnested.len,
        AVG(unnested.tt) AS tt, --avg TT for each link_dir
        SUM(s5b.num_obs) AS num_obs --sum of here.ta_path sample_size for each link_dir
    FROM dynamic_bin_options AS dbo
    LEFT JOIN segment_5min_bins AS s5b
        ON s5b.time_grp = dbo.time_grp
        AND s5b.segment_id = dbo.segment_id
        AND s5b.bin_rank >= dbo.start_bin
        AND s5b.bin_rank <= dbo.end_bin
    --this join is used to get the tx info about the last bin only
    LEFT JOIN segment_5min_bins AS s5b_end
        ON s5b_end.time_grp = dbo.time_grp
        AND s5b_end.segment_id = dbo.segment_id
        AND s5b_end.bin_rank = dbo.end_bin,
    --unnest all the observations from individual link_dirs to reaggregate them within new dynamic bin
    UNNEST(s5b.link_dirs, s5b.lengths, s5b.tts) AS unnested(link_dir, len, tt)
    --dynamic bins should not exceed one hour (dt_end <= dt_start + 1 hr)
    WHERE s5b_end.tx + interval '5 minutes' <= dbo.tx + interval '1 hour'
    GROUP BY
        dbo.time_grp,
        dbo.segment_id,
        s5b.total_length,
        dbo.tx, --stard_bin
        s5b_end.tx, --end_bin
        unnested.link_dir,
        unnested.len
)
```

`SELECT * FROM unnested_db_options WHERE time_grp = '[00:00:00,06:00:00)' LIMIT 10`

| time_grp            | segment_id | total_length | dt_start                | dt_end                  | link_dir    | len    | tt                      | num_obs |
|---------------------|------------|--------------|-------------------------|-------------------------|-------------|--------|-------------------------|---------|
| [00:00:00,06:00:00) |          1 |       374.22 | 2025-01-10 00:20:00.000 | 2025-01-10 00:25:00.000 | 1328374158F |  55.05 |     3.739 |       5 |
| [00:00:00,06:00:00) |          1 |       374.22 | 2025-01-10 00:20:00.000 | 2025-01-10 00:25:00.000 | 1328374159F |  71.33 |     4.845 |       5 |
| [00:00:00,06:00:00) |          1 |       374.22 | 2025-01-10 00:20:00.000 | 2025-01-10 00:25:00.000 | 1328374160F |  26.94 | 1.829 |       5 |
| [00:00:00,06:00:00) |          1 |       374.22 | 2025-01-10 00:20:00.000 | 2025-01-10 00:25:00.000 | 1328374165F |  38.00 | 3.109 |       5 |
| [00:00:00,06:00:00) |          1 |       374.22 | 2025-01-10 00:20:00.000 | 2025-01-10 00:25:00.000 | 1328374166F | 182.90 |    15.677 |       5 |
| [00:00:00,06:00:00) |          1 |       374.22 | 2025-01-10 00:25:00.000 | 2025-01-10 00:40:00.000 | 1328374158F |  55.05 |     4.833 |       5 |
| [00:00:00,06:00:00) |          1 |       374.22 | 2025-01-10 00:25:00.000 | 2025-01-10 00:40:00.000 | 1328374159F |  71.33 |     6.263 |       5 |
| [00:00:00,06:00:00) |          1 |       374.22 | 2025-01-10 00:25:00.000 | 2025-01-10 00:40:00.000 | 1328374160F |  26.94 | 2.365 |       5 |
| [00:00:00,06:00:00) |          1 |       374.22 | 2025-01-10 00:25:00.000 | 2025-01-10 00:40:00.000 | 1328374165F |  38.00 | 3.336 |       5 |
| [00:00:00,06:00:00) |          1 |       374.22 | 2025-01-10 00:25:00.000 | 2025-01-10 00:40:00.000 | 1328374166F | 182.90 |    95.773 |       6 |

### Insert statement
Here we find bins with sufficient length, for the two cases: 
- Multiple 5min bins assembled: need to check sufficient length from last step. 
- An original 5min bin, no group by needed to check length. 

```sql
    --this query contains overlapping values which get eliminated
    --via on conflict with the exclusion constraint on congestion_raw_segments table.
    INSERT INTO gwolofs.congestion_raw_segments (
        dt, time_grp, segment_id, bin_range, tt, num_obs
    )
    --distinct on ensures only the shortest option gets proposed for insert
    SELECT DISTINCT ON (time_grp, segment_id, dt_start)
        dt_start::date AS dt,
        time_grp,
        segment_id,
        tsrange(dt_start, dt_end, '[)') AS bin_range,
        total_length / SUM(len) * SUM(tt) AS tt,
        SUM(num_obs) AS num_obs --sum of here.ta_path sample_size for each segment
    FROM unnested_db_options
    GROUP BY
        time_grp,
        segment_id,
        dt_start,
        dt_end,
        total_length
    HAVING SUM(len) >= 0.8 * total_length
    ORDER BY
        time_grp,
        segment_id,
        dt_start,
        dt_end --uses the option that ends first
    --exclusion constraint + ordered insert to prevent overlapping bins
    ON CONFLICT ON CONSTRAINT congestion_raw_segments_exclude
    DO NOTHING;
```

`SELECT dt, time_grp, segment_id, bin_range, round(tt, 2), num_obs FROM inserted WHERE time_grp = '[00:00:00,06:00:00)';` 

| dt         | time_grp            | segment_id | bin_range                                     | round  | num_obs |
|------------|---------------------|------------|-----------------------------------------------|--------|---------|
| 2025-01-10 | [00:00:00,06:00:00) |          1 | ["2025-01-10 00:20:00","2025-01-10 00:25:00") |  29.20 |      25 |
| 2025-01-10 | [00:00:00,06:00:00) |          1 | ["2025-01-10 00:25:00","2025-01-10 00:40:00") | 112.57 |      26 |
| 2025-01-10 | [00:00:00,06:00:00) |          1 | ["2025-01-10 00:35:00","2025-01-10 00:40:00") |  76.66 |      25 |
| 2025-01-10 | [00:00:00,06:00:00) |          1 | ["2025-01-10 05:00:00","2025-01-10 05:10:00") |  35.21 |      26 |
| 2025-01-10 | [00:00:00,06:00:00) |          1 | ["2025-01-10 05:05:00","2025-01-10 05:10:00") |  35.45 |      25 |
| 2025-01-10 | [00:00:00,06:00:00) |          1 | ["2025-01-10 05:15:00","2025-01-10 05:20:00") |  48.01 |      25 |
| 2025-01-10 | [00:00:00,06:00:00) |          1 | ["2025-01-10 05:20:00","2025-01-10 05:55:00") |  51.34 |      14 |
| 2025-01-10 | [00:00:00,06:00:00) |          1 | ["2025-01-10 05:25:00","2025-01-10 05:55:00") |  69.59 |      13 |
| 2025-01-10 | [00:00:00,06:00:00) |          1 | ["2025-01-10 05:50:00","2025-01-10 06:00:00") |  53.28 |      62 |
| 2025-01-10 | [00:00:00,06:00:00) |          1 | ["2025-01-10 05:55:00","2025-01-10 06:00:00") |  39.36 |      50 |

After insert against exclusion constraint, only 6 remain (of 10 above), since rows #3,5,8,9 overlap other records. 
`SELECT segment_id, bin_range, round(tt, 2) AS tt, total_length, length_w_data FROM gwolofs.congestion_raw_segments WHERE segment_id = 29 AND time_grp = '["2025-01-04 00:00:00","2025-01-04 06:00:00")'::tsrange`

Constraint: 
```sql
    CONSTRAINT congestion_raw_segments_exclude EXCLUDE USING gist (
        bin_range WITH &&,
        segment_id WITH =,
        time_grp WITH =,
        dt WITH =
    )
```

| segment_id | bin_range                                     | tt                | num_obs | dt         | time_grp            |
|------------|-----------------------------------------------|-------------------|---------|------------|---------------------|
|          1 | ["2025-01-10 00:20:00","2025-01-10 00:25:00") |  29.2004224454790 |      25 | 2025-01-10 | [00:00:00,06:00:00) |
|          1 | ["2025-01-10 00:25:00","2025-01-10 00:40:00") | 112.5719201773835 |      26 | 2025-01-10 | [00:00:00,06:00:00) |
|          1 | ["2025-01-10 05:00:00","2025-01-10 05:10:00") |  35.2051925545571 |      26 | 2025-01-10 | [00:00:00,06:00:00) |
|          1 | ["2025-01-10 05:15:00","2025-01-10 05:20:00") |  48.0137225806451 |      25 | 2025-01-10 | [00:00:00,06:00:00) |
|          1 | ["2025-01-10 05:20:00","2025-01-10 05:55:00") |          51.34214 |      14 | 2025-01-10 | [00:00:00,06:00:00) |
|          1 | ["2025-01-10 05:55:00","2025-01-10 06:00:00") |  39.3552705888070 |      50 | 2025-01-10 | [00:00:00,06:00:00) |