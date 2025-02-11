-- FUNCTION: gwolofs.congestion_cache_corridor(bigint, bigint, text)

-- DROP FUNCTION IF EXISTS gwolofs.congestion_cache_corridor(bigint, bigint, text);

CREATE OR REPLACE FUNCTION gwolofs.congestion_cache_corridor(
	node_start bigint,
	node_end bigint,
	map_version text,
	OUT uid smallint,
	OUT link_dirs text[],
	OUT lengths numeric[],
	OUT total_length numeric)
    RETURNS record
    LANGUAGE 'plpgsql'
    COST 100
    VOLATILE PARALLEL SAFE 
AS $BODY$

DECLARE
    routing_function text := 'get_links_btwn_nodes_' || map_version;
    street_geoms_table text := 'routing_streets_' || map_version;

BEGIN

    --check if the node pair and map_version have already been routed
    --and if so, return values
    SELECT
        tt.uid,
        tt.link_dirs,
        tt.lengths,
        tt.total_length
        INTO uid, link_dirs, lengths, total_length
    FROM gwolofs.congestion_corridors AS tt
    WHERE
        tt.node_start = cache_tt_segment.node_start
        AND tt.node_end = cache_tt_segment.node_end
        AND tt.map_version = cache_tt_segment.map_version;
    IF FOUND THEN
        RETURN;
    END IF;
    
EXECUTE format (
    $$
        WITH routed_links AS (
            SELECT link_dir, seq
            FROM here_gis.%1$I(%2$L, %3$L),
            UNNEST (links) WITH ORDINALITY AS unnested (link_dir, seq)
        )
        
        INSERT INTO gwolofs.congestion_corridors (
            node_start, node_end, map_version, link_dirs, lengths, geom, total_length
        )
        SELECT
            %2$L AS node_start,
            %3$L AS node_end,
            %4$L AS map_version,
            ARRAY_AGG(rl.link_dir ORDER BY rl.seq) AS link_dirs,
            --lengths in m
            ARRAY_AGG(ST_Length(ST_Transform(streets.geom,2952)) ORDER BY rl.seq) AS lengths,
            st_union(st_linemerge(streets.geom)) AS geom,
            SUM(ST_Length(ST_Transform(streets.geom,2952))) AS total_length
        FROM routed_links AS rl
        JOIN here.%5$I AS streets USING (link_dir)
        --conflict would occur because of null values
        ON CONFLICT (node_start, node_end, map_version)
        DO UPDATE
        SET
            link_dirs = excluded.link_dirs,
            lengths = excluded.lengths,
            total_length = excluded.total_length
        RETURNING uid, link_dirs, lengths, total_length
    $$,
    routing_function, node_start, node_end, -- For routed_links
    map_version,      -- For INSERT SELECT values
    street_geoms_table                      -- For JOIN table
) INTO uid, link_dirs, lengths, total_length;
RETURN;
END;
$BODY$;

ALTER FUNCTION gwolofs.congestion_cache_corridor(bigint, bigint, text)
    OWNER TO gwolofs;
