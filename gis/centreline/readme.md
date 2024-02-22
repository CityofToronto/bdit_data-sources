# City Centreline Data

The centreline data is one of the City's core GIS products. It delineates street centrelines as well as some other entity types like waterways or political boundaries. For the most part, our unit will need to filter out certain classes of these other entity types which are not directy related to transportation.

The centreline data are used by many other groups in the City and it's often important to be able to relate various data products or infrastructure back to the centreline entities they describe or are located on.

## How It's Structured

The centreline data is structured as an undirected graph with edges and nodes. Both edges and nodes have a `centreline_id` identifier. A given `centreline_id` will refer to either an edge or a node. All edges have _from_ and _to_ nodes, though this should not be taken to indicate that edges are directed. 

A **two-way street** will be represented by a single edge and the _from_ and _to_ identifiers may be arbitrarily swapped.

For **one-way streets** however, there is a column called `oneway_dir_code` that distinguishes whether its a two-way street and whether its being drawn with the digitization or against. This indicates only the direction for motor vehicle traffic and would not account for the presence of a contraflow bike lane. 

# Where It's Stored

Centreline data are stored in the `gis_core` schema in the `bigdata` database. Nodes are stored separately from edges. There may be older copy of the various centreline tables in the `gis` schema but they are not updated and their use is deprecated.

## Edges

Edges are stored in the partitioned table `gis_core.centreline`. This is not the complete centreline dataset - it has had some pre-filtering done to it to remove things like administrative boundaries.

However not all transport related edges are included. Things like alleyways and footpaths have been filtered out.

## Nodes

Nodes are stored in either of two tables, each of which is copied from a separate GCC layer which in turn are maintained by different groups. The differences between these tables is not completely understood.

* `gis_core.intersection` (copied from [here](https://insideto-gis.toronto.ca/arcgis/rest/services/cot_geospatial12/FeatureServer/42))
* `gis_core.centreline_intersection_point` (copied from [here](https://insideto-gis.toronto.ca/arcgis/rest/services/cot_geospatial12/FeatureServer/19))

Despite the term "intersection", nodes do not neccessarily indicate intersections in the transport sense, but rather are simply the intersection (points) of linear geometries. However some of them however _are_ "intersection"s in both senses of the word.
