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

* The `gis_core.centreline_latest` Materialized View contains the latest set of lines for road classes that are relevant to transportation. It includes all road classification but **excludes** `Trail` and `Busway`.
* The `gis_core.centreline_latest_all_feature` Materialized View contains the latest set of lines, **including all features**.
* The `centreline_intersection_point_latest` Materialized View contains the latest set of unique intersections with unique id `intersection_id`. These are any location where two lines intersect, not strictly intersections in the transportation sense ([see more](#intersections-nodes))

## Where It's Stored

Centreline data are stored in the `gis_core` schema in the `bigdata` database. Both the interection and centreline segment layer are stored in partitioned tables, where we pull in a new version of these layer quarterly from GCCview through an automatic [airflow process](/dags/gcc_layers_pull.py). Other centreline layers not stored in the `gis_core` schema (for example: `gis`) has been deprecated. 

### Centreline Segments (edges)

Segments are stored in the partitioned table `gis_core.centreline`. These lines are undirected. All edges have _from_ and _to_ nodes, though this should not be taken to indicate that edges are directed. For a directed centreline layer, check out `gis_core.routing_centreline_directional` ([see more](#centreline-segments-edges)) which has the necessary schema to be used in pg_routing.

Currently we are including only the following types:

> [!IMPORTANT]
> **2025-02-24**: Added `Busway`, `Trail`, `Access Road`, `Other Ramp`, and `Laneway`, in order to ensure consistency with MOVE.
> 
> **2024-02-19**: Added `Other`. 

| Feature Type         | Included in `centreline_latest` | Included in `centreline_latest_all_feature` |
|----------------------|--------------------------------|----------------------------------|
| Expressway          | ✅ | ✅ |
| Expressway Ramp     | ✅ | ✅ |
| Major Arterial      | ✅ | ✅ |
| Major Arterial Ramp | ✅ | ✅ |
| Minor Arterial      | ✅ | ✅ |
| Minor Arterial Ramp | ✅ | ✅ |
| Collector           | ✅ | ✅ |
| Collector Ramp      | ✅ | ✅ |
| Local               | ✅ | ✅ |
| Pending             | ✅ | ✅ |
| Other (added `2024-02-19`) | ✅ | ✅ |
| Busway (added `2025-02-24`) | ❌ | ✅ |
| Access Road (added `2025-02-24`) | ✅ | ✅ |
| Trail (added `2025-02-24`) | ❌ | ✅ |
| Other Ramp (added `2025-02-24`) | ✅ | ✅ |
| Laneway (added `2025-02-24`) | ✅ | ✅ |

#### Directionality

Directionality of streets can be identified with the column `oneway_dir_code_desc`, distinguishing whether the segment is a one-way street. A two way street will be represented by a single segment (`oneway_dir_code = 0`). `oneway_dir_code` can be used to identify whether a segment is being drawn with the digitization (1) or against (-1), indicating vehicular traffic direction.

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
    - contains additional boundary information such as ward, and municipality
    - include trails and ferry routes
* `gis_core.intersection` (pulled from [here](https://insideto-gis.toronto.ca/arcgis/rest/services/cot_geospatial12/FeatureServer/42))
    - **Not unique** on `intersection_id`: appears to be 1 row to describe every *physical* (do the lines connect in 3D) relationship between crossing edges at a node with the `connected` column. Crossing means this layer doesn't include any pairs of centrelines with the same name. Nor does it account for bylawed turn restrictions or one-way directions (any manner of illegal turns or possibly U-turns will be described as `connected = 'Y'`).
    - contains additional elevation information such as elevation level (they are all zero), elevation unit, height restriction, etc
    - does not include cul-de-sacs, overpass/underpass
* `gis_core.intersection_classification`
    - A view that provides information on intersection's related road classes, road names, and connectivity degree.
    
    | Column Name                 | Description  |
    |-----------------------------|--------------|
    | `intersection_id`           | Unique identifier for each intersection. |
    | `intersection_desc`         | Intersection Name. |
    | `distinct_feature_desc_list`| Disintct list of unique road class descriptions associated with the intersection. |
    | `highest_order_feature`     | The highest-order road class associated with the intersection. |
    | `all_feature_code_list`     | Full list of all road class descriptions (including duplicates). |
    |`classification_desc`        | Intersection Classification Description. |
    |`elevation_feature_code_desc`| Elevation classification Description. |
    | `road_names`                | List of distinct road names connected at the intersection. |
    | `degree`                    | Number of connected centreline segments. |
    | `centreline_ids`            | Array of `centreline_id`s connected to the intersection. |
    | `geom`                      | Intersection geometry. |
    | `cent_geom`                 | Combined geometry of all associated centreline segments. |

    - Known Caveats:
 
    -  Boundary Intersections: Intersections along the city's boundary (e.g., Steeles Avenue) may connect to roads outside the city's jurisdiction. These may be classified as pseudo intersections and get filtered out in this view.
 
    -  Intersections where centrelines intersect with themselves (e.g. North Hills Terrace) are not included, since only the number of unique centreline_ids is considered for the degree and we filter where degree is below or equal 2.

### Segments with reference to intersections

The materialized view `gis_core.centreline_leg_directions` contains an automated determination of the cardinal direction ("north", "east", "south", or "west") of segments with reference to a 3- or 4-legged intersection. This may be useful where other datasets such as TMCs provide data for e.g. the "North approach", but don't actually specify which centreline edge this is.

The orientation of some intersections makes this mapping non-trivial and it's possible and even likely that some datasets classify these cardinal directions differently or inconsistently. Please report any issues or inconsistencies you may find [here](https://github.com/CityofToronto/bdit_data-sources/issues/1190).

## MOVE Centreline

The tables described here reference the centreline that underpins the MOVE Exports product. It is a simplified view of the centreline that merges midblock segments into continuous segments between nodes that is more representative of how we view the road/transportation network. The intersections are then filtered to only contain the nodes that match the endpoints of the merged edges.

Additionally, these tables are what you would use to join to the studies included in the `traffic` schema as they're conflated to this version of the centreline. Studies can also be joined using their `lat` and `lng` coordinates via a spatial join if using a different version of the centreline.

If you would like to join to a non-simplified centreline, then you need to `unnest` the `centreline_id_array` column and then perform a join using that.

### `traffic.centreline2_midblocks`

| Column Name                 | Description  |
|-----------------------------|--------------|
| `midblock_id`               | Unique identifier for the segment. It's the lowest id of the merged segments. |
| `midblock_name`             | A name to reresent the midblock segment. Generated in MOVE ETL. Regex rules are applied to clean up the naming.|
| `centreline_type`           | Denotes if intersection or midblock. |
| `feature_code`              | Specifies the road class of the segment. |
| `linear_name_id`            | Identifies the road that the segment belongs to. |
| `linear_name_full`          | The legal name of the road the segment belongs to. |
| `properties`                | A JSON object containing a spatial join of nearby hospitals, schools, pxo's or traffic signals that are along or nearby the segment. |
| `from_intersection_id`      | `intersection_id` of the node at one of the endpoints. |
| `from_intersection_name`    | Non-regexed name for the intersection, please join to `traffic.centreline2_intersections` for the clean name. |
| `to_intersection_id`        | `intersection_id` of the node at one of the endpoints.|
| `to_intersection_name`      | Non-regexed name for the intersection, please join to `traffic.centreline2_intersections` for the clean name.|
| `lat`                       | Latitude of the midpoint of the segment. |
| `lng`                       | Longitude of the midpoint of the segment. |
| `geom`                      | Geometry of the segment. |
| `centreline_id_array`       | An array containing the list of `midblock_id`'s that are represented by the simplified segment. |
| `shape_length`              | The length of the shape according to GCC. However there is an existing issue relating to the accuracy of the length. |

### `traffic.centreline2_intersections`

| Column Name                 | Description  |
|-----------------------------|--------------|
| `intersection_id`           | Unique identifier of the intersection. |
| `centreline_type`           | Denotes whether `midblock` or `intersection`. |
| `intersection_name`         | The cleaned up name of the intersection. This field is unique. |
| `classification`            | The classification of the intersection (Major, minor etc..) |
| `feature_code`              | The feature code of the specific "level" of the intersection. |
| `properties`                | A JSON object containing a spatial join of nearby hospitals, schools, pxo's or traffic signals that are along or nearby the segment. |
| `lat`                       | Latitude of the intersection. |
| `lng`                       | Longitude of the intersction. |
| `geom`                      | The geometry of the intersection. |
| `date_effective`            | The date the intersection was updated by GCC. |






