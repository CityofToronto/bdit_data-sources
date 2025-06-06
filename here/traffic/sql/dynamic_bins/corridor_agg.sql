--test: 35 projects, 1 day = 47s
SELECT gwolofs.congestion_cache_tt_results_daily(
    node_start := congestion_corridors.node_start,
    node_end := congestion_corridors.node_end,
    start_date := dates.dt::date
)
FROM gwolofs.congestion_corridors
JOIN gwolofs.congestion_projects USING (project_id),
generate_series('2025-02-01', '2025-02-28', '1 day'::interval) AS dates(dt)
WHERE
    congestion_projects.description IN (
        'bluetooth_corridors', 'scrutinized-cycleway-corridors'
    )
    AND map_version = '23_4';