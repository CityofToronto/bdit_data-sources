# Example Queries

## Corridor Aggregation

Aggregate corridor dynamic bins and calculate confidence intervals with bootstrapping. 

```sql
SELECT *
FROM data_requests.i1105_tt_corridors,
generate_series('2025-05-16', '2025-05-18', '1 day'::interval)
CROSS JOIN LATERAL (
    SELECT *
    FROM here_agg.cache_tt_results_daily(
        generate_series::date,
        node_start,
        node_end
    )
) AS q;

SELECT corridor_bootstrap.*
FROM here_agg.corridors
JOIN data_requests.i1105_tt_corridors USING (node_start, node_end, map_version),
here_agg.corridor_bootstrap(
    start_date := '2026-05-16'::date,
    end_date := '2026-05-18'::date,
    corridor_id := corridors.corridor_id,
    n_resamples := 300::int,
    isodows := '{1,2,3,4,5,6,7}'::smallint[],
    include_holidays := True::boolean,
    hr_starts := 15::smallint,
    hr_ends := 17::smallint
);
```