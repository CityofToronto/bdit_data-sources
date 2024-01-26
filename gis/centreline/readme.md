# City Centreline Data

The centreline data is one of the City's coe GIS products. It delineates street centrelines as well as some other entity types like certain political boundaries. For the most part, our unit will need to filter out certain classes of these other entity types which are not directy related to transportation. 

The centreline data are used by many othr groups in the City and it's often important to be able to relate various data products or infrastucture back to the centreline entities they describe or are located on. 

# Where It's Stored

Centreline data are stored in the `gis_core` schema in the `bigdata` database.

There is an older copy of these tables in the `gis` schema but it is not maintained and its use is deprecated.

## How It's Structured

The centreline data is structured as a graph with edges and nodes. Both edges and nodes have a `centreline_id` identifier. A given `centreline_id` will refer to either an edge or a node. 