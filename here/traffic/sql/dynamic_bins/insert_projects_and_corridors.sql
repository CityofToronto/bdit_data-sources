--for naming corridor_streets.
--need help with corridor_start and corridor_end locations - not sure how to turn here nodes into names. Intersection conflation?
WITH named_corridors AS (
    SELECT
        corridor_id,
        string_agg(DISTINCT initcap(st_name), ' / ') AS corridor_streets
    FROM here_agg.corridors,
        UNNEST(congestion_corridors.link_dirs) AS unnested (link_dir)
    LEFT JOIN here_gis.traffic_streets_24_4 ON link_id = trim(TRAILING 'T|F' FROM link_dir)::int
    WHERE map_version = '24_4'
    GROUP BY corridor_id
    ORDER BY corridor_id DESC
)

UPDATE here_agg.corridors AS cc
SET corridor_streets = nc.corridor_streets
FROM named_corridors AS nc
WHERE nc.corridor_id = cc.corridor_id;

--look at bluetooth corridors
REFRESH MATERIALIZED VIEW bluetooth.here_cn_23_4_lookup;

--cache project
WITH project AS (
    INSERT INTO here_agg.projects (description)
    VALUES ('bluetooth_corridors')
    RETURNING project_id
),

--cache corridors, repeat with multiple map versions
corridors AS (
    SELECT corridor_id
    FROM bluetooth.here_cn_23_4_lookup AS bt,
        here_agg.cache_corridor(bt.here_fnode, bt.here_tnode, '24_4')
)

--add project_id to corridors
UPDATE here_agg.corridors
SET project_id = (SELECT project_id FROM project)
WHERE corridor_id IN (SELECT corridor_id FROM corridors)
RETURNING corridor_id;

--examine the projects
SELECT congestion_corridors.*
FROM here_agg.corridors
JOIN here_agg.projects USING (project_id)
WHERE congestion_projects.description IN ('bluetooth_corridors', 'scrutinized-cycleway-corridors')

