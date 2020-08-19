SET client_min_messages = warning; 
-- only show warning messages that I would like to know
CREATE TABLE gis.bylaws_routing AS
SELECT law.*, results.*
FROM jchew.bylaws_to_update law, --where deleted = false
LATERAL gis.text_to_centreline(
law.id,
law.highway,
law.between,
NULL
) as results