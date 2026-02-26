/*
A mid-block SVC/ATR count is assigned two of the four cardinal directions.
These will be opposites (N/S or E/W), but a given centreline segment isn't
necessarily defined as N/S or E/W per se. A diagonal segment could be
assigned either pair while still being clear about which direction on the
segment is meant.

This view assigns directons, relative to the centreline segment, to each
of the four cardinal directions. Some of these, like N/S on Bloor are
extremely unlikely to show up in the data, and are excluded, but many other
unlikely combinations are kept just in case.

As such, this view should be joined on the actual directions assigned to SVCs
rather than used on its own. Such a join *should* filter out most silly values.
*/

CREATE TABLE traffic.svc_centreline_directions (
    centreline_id integer,
    from_node integer,
    to_node integer,
    direction text,
    centreline_geom_directed geometry,
    absolute_angular_distance real
);

COMMENT ON TABLE traffic.svc_centreline_directions
IS 'Maps the four cardinal directions (NB, SB, EB, & WB) referenced by SVCs onto '
'specific directions of travel along edges of the `gis_core.centreline_latest` network. '
'Refreshed automatically by `gcc_layers_pull_bigdata` DAG after inserts into '
'`gis_core.centreline_latest`.';

CREATE UNIQUE INDEX ON traffic.svc_centreline_directions (centreline_id, direction);

ALTER TABLE traffic.svc_centreline_directions OWNER TO gis_admins;

COMMENT ON COLUMN traffic.svc_centreline_directions.geom_directed
IS 'centreline segment geom drawn in the direction of `direction`';

COMMENT ON COLUMN traffic.svc_centreline_directions.absolute_angular_distance
IS 'Minimum absolute angular distance in degrees between centreline as drawn in `centreline_geom_directed` and the ideal of the stated direction. To be used as a measure of confidence for the match.';
