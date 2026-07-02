1. Cache Corridors

```sql
CREATE TABLE gwolofs.bathurst_corridors AS
SELECT
    cache_corridor.*,
    val.*
FROM 
(VALUES
    --found using tt app
    (30363068, 30363474, 'Bathurst St', 'Northbound', 'College St', 'Bloor St W'),
    (30363474, 30363068, 'Bathurst St', 'Southbound', 'Bloor St W', 'College St')
) AS val(start_node, end_node, routeStreets, direction, startCrossStreets, endCrossStreets),
here_agg.cache_corridor(
    node_start := val.start_node::bigint,
    node_end := val.end_node::bigint,
    map_version := '25_1'::text
);
```


2. Cache dynamic bins for the corridors into here_agg.raw_corridors

```sql
--can also use `here_agg.cache_tt_results` for more fine grained control over which hours to cache.
SELECT here_agg.cache_tt_results_daily(
    start_date := gs.dt::date,
    node_start := bath.start_node,
    node_end := bath.end_node 
)
FROM gwolofs.bathurst_corridors AS bath,
generate_series('2026-05-01', '2026-05-30', '1 day'::interval) AS gs(dt)
```

3. Calculate bootstrapped confidence intervals

```sql
SELECT wkdy_grps.*, lat.*
FROM gwolofs.bathurst_corridors AS bath,
--cross join day of week groups
(VALUES
    ('Weekend/Holiday', ARRAY[6, 7], True),
    ('Mon-Fri', ARRAY[1, 2, 3, 4, 5], False)
) AS wkdy_grps(dow_group, isodows, include_holidays),
--cross join hour groups
(VALUES
    (7,10),
    (16,19)
) AS hrs(hr_start, hr_end),
LATERAL (
    SELECT * FROM here_agg.corridor_bootstrap(
        start_date := '2026-05-01'::date,
        end_date := '2026-05-30'::date,
        corridor_id := bath.corridor_id::bigint,
        n_resamples := 300::int,
        hr_starts := hrs.hr_start::smallint,
        hr_ends := hrs.hr_end::smallint,
        isodows := wkdy_grps.isodows::smallint[],
        include_holidays := wkdy_grps.include_holidays
    )
) AS lat
```