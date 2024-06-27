# City Centreline Data <!-- omit in toc -->

The centreline data is one of the City's core GIS products. It delineates street centrelines as well as other entity types like waterways or political boundaries. Our unit filters out certain classes of these other entity types which are not directy related to transportation.

The centreline data are used by many other groups in the City and it's often important to be able to relate various data products or infrastructure back to the centreline entities they describe or are located on.

- [How It's Structured](#how-its-structured)
- [Where It's Stored](#where-its-stored)
  - [Centreline Segments (edges)](#centreline-segments-edges)
    - [Directionality](#directionality)
    - [Lineage](#lineage)
  - [Intersections (nodes)](#intersections-nodes)


## How It's Structured

The centreline data is structured as an undirected graph with edges and nodes. Both edges and nodes have a `centreline_id` identifier. A given `centreline_id` will refer to either an edge or a node. All edges have _from_ and _to_ nodes, though this should not be taken to indicate that edges are directed. For a directed centreline layer, checkout `gis_core.routing_centreline_directional`. 

## Where It's Stored

Centreline data are stored in the `gis_core` schema in the `bigdata` database. Both the interection and centreline segment layer are stored in partitioned tables, where we pull in a new version of these layer quarterly from GCCview through an automatic [airflow process](/dags/gcc_layers_pull.py). Other centreline layers not stored in the `gis_core` schema (for example: `gis`) has been deprecated. 

### Centreline Segments (edges)

Segments are stored in the partitioned table `gis_core.centreline`. The latest version of centreline can be access through this materialized view `gis_core.centreline_latest`. 

Currently we are including only the following types:

* 'Expressway'
* 'Expressway Ramp'
* 'Major Arterial'
* 'Major Arterial Ramp'
* 'Minor Arterial'
* 'Minor Arterial Ramp'
* 'Collector'
* 'Collector Ramp'
* 'Local'
* 'Pending'
* 'Other' (version >= `2024-02-19`)

#### Directionality

Directionality of streets can be identified with the column `oneway_dir_code_desc`, distinguishing whether the segment is a one-way street. A two way street will be represented by a single segment. `oneway_dir_code` can be used to identify whether a segment is being drawn with the digitization or against, indicating vehicular traffic direction.

#### Lineage

Centreline gets updated regularly by the GCC, the changes are logged in `gis_core.centreline_lineage`. It is currently pulling from GCC's oracle database in a live table manually, regular pulling have not been set up yet. 

| column                | description                                |
|-----------------------|--------------------------------------------|
| centreline_lineage_id | Unique identifier of the lineage           |
| date_effective        | Effective date for the new centreline_id   |
| centreline_id_old     | the old centreline_id                      |
| centreline_id_new     | the new centreline_id replacing the old id |
| trans_id_create       | the transaction id that created the new id |

### Intersections (nodes)

Intersections are stored in either of two tables, each of which is copied from a separate GCC layer which in turn are maintained by different groups. Each intersections represents the intersecting point of two or more centreline segments. For both of these layers you may want to filter `WHERE classification_desc IN ('Major-Multi Level', 'Major-Single Level', 'Minor-Multi Level', 'Minor-Single Level')` which excludes things like pseudo intersections. 

* `gis_core.centreline_intersection_point` (pulled from [here](https://insideto-gis.toronto.ca/arcgis/rest/services/cot_geospatial/FeatureServer/19))
    - **Almost** unique on `intersection_id`: **generally the preferred intersection layer**.
    - contains additional boundary information such as ward, and municpality
    - include trails and ferry routes
* `gis_core.intersection` (pulled from [here](https://insideto-gis.toronto.ca/arcgis/rest/services/cot_geospatial12/FeatureServer/42))
    - **Not unique** on `intersection_id`: appears to be 1 row to describe every relationship between edges at a node.  
    - contains additional elevation information such as elevation level, elevation unit, height restriction, etc
    - does not include cul-de-sacs, overpass/underpass