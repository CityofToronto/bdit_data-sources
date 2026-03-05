--test: 35 projects, 1 day = 47s
SELECT
    here_agg.cache_tt_results_daily(
        node_start := congestion_corridors.node_start,
        node_end := congestion_corridors.node_end,
        start_date := dates.dt::date
    )
FROM here_agg.corridors
JOIN here_agg.projects USING (project_id),
    generate_series('2025-01-01', '2025-02-28', '1 day'::interval) AS dates (dt)
WHERE
    congestion_projects.description IN (
        'Avenue Road cycleway installation',
        'bluetooth_corridors',
        'scrutinized-cycleway-corridors'
    )
    AND corridor_id NOT IN (
        SELECT DISTINCT corridor_id
        FROM here_agg.raw_corridors
        WHERE dt >= '2025-01-01' AND dt < '2025-02-28'
    )
    AND map_version = '23_4';