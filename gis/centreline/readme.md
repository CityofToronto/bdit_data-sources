# City Centreline Data

The centreline data is one of the City's core GIS products. It delineates street centrelines as well as some other entity types like waterways or political boundaries. For the most part, our unit will need to filter out certain classes of these other entity types which are not directy related to transportation.

The centreline data are used by many other groups in the City and it's often important to be able to relate various data products or infrastucture back to the centreline entities they describe or are located on.

## How It's Structured

The centreline data is structured as an undirected graph with edges and nodes. Both edges and nodes have a `centreline_id` identifier. A given `centreline_id` will refer to either an edge or a node. All edges have _from_ and _to_ nodes, though this should not be taken to indicate that edges are directed. A two-way street will be represented by a single edge and the _from_ and _to_ identifiers may be arbitrarily swapped.

# Where It's Stored

Centreline data are stored in the `gis_core` schema in the `bigdata` database. Nodes are stored separately from edges. 

* edges are in the partitioned table `centreline`
* nodes are in the partitioned table `intersection` (copied from [here](https://insideto-gis.toronto.ca/arcgis/rest/services/cot_geospatial12/FeatureServer/42))
* OR nodes are in the partioned table `centreline_intersection_point`???

Despite the term "intersection", nodes do not neccessarily indicate intersections in the transport sense, but rather are simply the intersection (points) of linear geometries. However Some of them however _are_ "intersection"s in both senses of the word.

There is an older copy of these tables in the `gis` schema but it is not updated and its use is deprecated.

