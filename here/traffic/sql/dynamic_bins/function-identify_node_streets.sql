CREATE OR REPLACE FUNCTION gwolofs.identify_node_streets(
    node bigint,
    map_version text,
    exclude_streets text [],
    OUT streets text
)
RETURNS text
LANGUAGE plpgsql
COST 100
VOLATILE PARALLEL SAFE
AS $BODY$

DECLARE
    routing_nodes_table text := 'routing_nodes_' || map_version;
    traffic_streets_table text := 'traffic_streets_' || map_version;

BEGIN
EXECUTE format (
    $$
    SELECT string_agg(DISTINCT initcap(streets.st_name), ' / ' ORDER BY initcap(streets.st_name))
    FROM here.%1$I AS node
    LEFT JOIN here_gis.%2$I AS streets
        ON node.link_id = streets.link_id
        AND NOT(initcap(streets.st_name) = ANY(%3$L))
        AND streets.st_name IS NOT NULL
    WHERE node.node_id = %4$L
    $$,
    routing_nodes_table,
    traffic_streets_table,
    identify_node_streets.exclude_streets,
    identify_node_streets.node
) INTO streets;

RETURN;
END;
$BODY$;

ALTER FUNCTION gwolofs.identify_node_streets(bigint, text, text [])
OWNER TO gwolofs;

COMMENT ON FUNCTION gwolofs.identify_node_streets IS
'Identifies the streets intersecting with a HERE node_id, given a node, map_version,
and a list of streets to exclude (generally those which form the corridor).';
