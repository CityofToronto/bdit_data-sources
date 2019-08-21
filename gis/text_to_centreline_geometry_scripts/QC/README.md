# Text to Centreline QC

It is important to find efficient ways to conduct QC so we can verify that the streets that were matched to the large number of bylaw locations are correct. The QC can involve a lot of manual checks to the final dataset. The checks that do include:
- manually checked all the bylaw locations with final geoms over 2 km that occured between unfamiliar intersections (or intersections between which I was uncertain that the distance was over 2 km)
- looked at the centreline segments matched to bylaws that have a low confidence value
- looked at final lines that were of type `ST_MultiLineString` (this means that the lines were not continuous, or the line was a circular shape, or that there was a fork in the line)
- looked at final bylaw lines that overlapped with a different bylaw's line
- look at bylaws with more than one value in the `street_name_arr` output arry
- look at outputs with a ratio that is greater than 1

I investigated some of these checks. I included the queries that I used in `QC_script.sql`. One of the tables I used were called `crosic."posted_speed_limit_xml_open_data_withQC"`. This table was the output of the script that takes an Open Data XML file of the speed limit bylaws in the city as input and matches its contents to geometries that represent the locations where these bylaws are in effect. Find it [here](../text_to_centreline_script_xml_opendata.py). `posted_speed_limit_xml_open_data_original` is a table with the original `XML` file from Open Data.

I documented comments on the output from each of these queries in the `CSV` files in this folder. I will explain some interesting things I found while creating the CSV files below. A lot of what I will write is about changes that should be made to the algortihm to make the matches more accurate.

## Outputs with intersecting geometries

Query:
```
-- this query finds all of the streets that have intersecting geometry
SELECT ST_Transform(ST_SetSRID(p1.geom::geometry, 26917), 4326), ST_Transform(ST_SetSRID(p2.geom::geometry, 26917), 4326),
p1.id_orig AS id, p1.street_name, p1.extents, p1.speed_limit, p2.id_orig AS id, p2.street_name, p2.extents, p2.speed_limit
FROM (SELECT DISTINCT ON (street_name, extents) p.*, o."Speed_Limit_km_per_hr" speed_limit, o."ID" id_orig FROM crosic."posted_speed_limit_xml_open_data_withQC" p JOIN posted_speed_limit_xml_open_data_original o ON p.street_name = o."Highway" AND p.extents = o."Between") AS p1,
(SELECT DISTINCT ON (street_name, extents) p.*, o."Speed_Limit_km_per_hr" speed_limit, o."ID" id_orig FROM crosic."posted_speed_limit_xml_open_data_withQC" p JOIN posted_speed_limit_xml_open_data_original o ON p.street_name = o."Highway" AND p.extents = o."Between") AS p2
where p1.geom is not null and p2.geom is not null and st_overlaps(p1.geom::geometry, p2.geom::geometry)
and p1.index > p2.index
and st_geometrytype(p1.geom::geometry) <> 'ST_MultiLineString' and st_geometrytype(p2.geom::geometry) <> 'ST_MultiLineString';
```

`csv` file to look at: `overlapping_geometry.csv`

## Outputs with at least one street with a similar name close to it

See the documentation on the [street_name_arr](../README.md) variable (assigned in Step 3 section) to understand it.

Query:
```
select ST_Transform(ST_SetSRID(geom::geometry, 26917), 4326), *
from crosic."posted_speed_limit_xml_open_data_withQC"
where street_name_arr LIKE '%,%'
```

`csv` file to look at: `more_than_one_street_with_small_lev_dist.csv`

### Interesting Cases

There were a lot of cases of streets that are totally spearate from each other having similar names. i.e. `The East Mall` and `The West Mall`, I'm not including those here.

#### William R Allen Road and Highway 27

`William R Allen Road` and `Highway 27` are the few streets in the `gis.centreline` table that have different line segments for their northbound direction and their southbound direction. This is an issue because the algorithm will only match the bylaw to a street with one name (see information on the [street_name](../README.md) variable assigned in Step 3 in the process). So when given a bylaw where the street name that the bylaw occurs on is `William R Allen Rd`, it chooses to macth either `William R Allen Rd N` or `William R Allen Rd S` at random.

Right now, the algorithm disregards any text between brackets, so the description `William R. Allen Road (northbound)` would be changed to `William R Allen Rd`.

Some cases need to be added to the function to make these specific streets match correctly.

#### Bylaws that occurs between an intersection that has the same name as another intersection

This case is not a result of the fact that two streets were in the `street_arr` array, its just something I noticed that could be fixed in the future.

Its hard to descibe this case, so I'll show an example:

`George Street South`	from `Henry Lane Terrace (south intersection) and Front Street East`

This is a screenshot of what got matched:
![](jpg/QC_george_street.jpg)

It should be matching the southern intersection from `Henry Lane Terrace`, but instead it is matching the northern one. This is because the `(south intersection)` part of the description is disregarded.

## Outputs with a low confidence value

Query:
```
SELECT ST_Transform(ST_SetSRID(geom::geometry, 26917), 4326) geom_transformed, *
FROM crosic."posted_speed_limit_xml_open_data_withQC"
WHERE confidence LIKE '%Low%6%' OR confidence LIKE '%Low%7%' and "ratio" is not NULL
```

`csv` file to look at: `lowest_confidence_matches.csv`

### Interesting Cases

#### CPR Underpass

`Howland Avenue` from	`Dupont Street and CPR underpass`

confidence: `Low (6 character difference)`

Some bylaws have weird descriptions, the CPR underpass description is one that I've noticed a lot. Not sure how one could fix this though since I'm not sure if this would even be in the centreline.

#### Bylaws that occur on very short segments of street

`Dunn Avenue`	from `A point 13 metres south of Springhurst Avenue and Springhurst Avenue`
confidence: `Low (6 character difference)`

I thought this was interesting because it occurs for only 13 metres. There's a clause somewhere that filters out bylaw line geometries that are under 11 metres. I'm not sure if this clause should have a number less than 11 or if it should exist in general.
