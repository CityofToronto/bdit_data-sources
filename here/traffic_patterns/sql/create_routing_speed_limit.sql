CREATE TABLE here.routing_streets_yy_q_speed_limit AS

SELECT
    routing_streets.link_dir,
    COALESCE(
        CASE
            WHEN RIGHT(routing_streets.link_dir, 1) = 'T' AND att.to_spd_lim > 0 THEN att.to_spd_lim
            WHEN RIGHT(routing_streets.link_dir, 1) = 'F' AND att.fr_spd_lim > 0 THEN att.fr_spd_lim
            ELSE NULL
        END, 50
    ) AS spd_lim
FROM here.routing_streets_yy_q AS routing_streets
LEFT JOIN here_gis.streets_att_yy_q AS att on LEFT(routing_streets.link_dir, -1)::int = att.link_id;