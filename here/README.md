# HERE Data

## Traffic Data
Historical data acquired through the Traffic Analytics download portal. Data goes back to 2012-01-01 and is aggregated in 5-minute bins. In our database the data is stored in partitioned tables under `here.ta`. Have a look at the [procedure for loading new data](traffic#loading-new-data) for that including using [`data_utils`](../data_utils/), which has been extended to partition (add check constraints) and index this data.

### Data Schema

|column|type|notes|
|------|----|-----|
|link_dir|text| Unique link id |
|tx|timestamp| Timestamp of obersevation (5-min bin)|
|epoch_min|integer| Minutes of the day|
|length|integer| Link length (m)|
|mean|numeric| Observed mean speed|
|stddev|numeric| Observed speed standard deviation|
|min_spd|integer| Observed min speed|
|max_spd|integer| Observed max speed|
|confidence|integer| [70-100] degree to which observation depends on historical data|
|pct_x|integer| Speed at the x percentile in 5% bins|

For an exploratory description of the data check out [this notebook](https://github.com/CityofToronto/bdit_team_wiki/blob/here_evaluation/here_evaluation/Descriptive_eval.ipynb)

#### Getting link attributes
The Traffic Analytics `link_dir` is a concatenation of the `streets` layer `link_id` and a `travel direction` character (F,T). `F` and `T` represent "From" and "To" relative to each link's reference node, which is *always* the node with the lowest latitude. In the case of two nodes with equal latitude, the node with the lowest longitude is the reference node. To join `ta` data to the `here_gis.streets_att` table use the following  
```sql
JOIN here_gis.streets_att_16_1 gis ON gis.link_id = LEFT(ta.link_dir, -1)::numeric
```

#### Functional Class 5
This bucket contains a little bit of everything that doesn't fall into the other classes. Currently exclude

`"paved"  = 'Y' AND "poiaccess" =  'N' AND "ar_auto" = 'Y' AND "ar_traff" = 'Y'`

~~If we also want to exclude dead-ends add the following filter:~~  
~~`st_typ_aft NOT IN ('ACRS', 'ALY')`~~

## GIS Data
A lot of map layers provided by HERE, see the [README](gis/README.md) in the [gis](gis/) folder for more info.