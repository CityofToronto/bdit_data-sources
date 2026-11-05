# Curb geometry automation


## Overview

For each polygon in `gis.edge_of_roadway`, the script:

1. Dissolve polygons into points, use azimuth to identify "turning points"
2. Use "turning points" to reconstruct distinct lines for each polygon, e.g. a rectangle will have 4 line output
3. Identify "curved" lines by calculating the ratio between line length and point to point distance, large differences will indicate a curver line
4. Find lines that are touching curve line and extend them using the last 10m of the line
5. Find the intersecting point of the extended line and cut the line 
6.


## Input

- `gis.edge_of_roadway`: polygon geometries with an `_id` and a `SUBTYPE_DESC` column.

## Output



## Caveats
