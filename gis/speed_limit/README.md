# Updating the Posted Speed Limit Layer 


## Table of Contents
- [Description of Project Problem](#description-of-project-problem)
- [Methodology](#Methodology)
  - [How Well Does This Work](#How-Well-Does-This-Work)
  - [How to Create the Speed Limit Layer from Bylaws](#How-to-Create-the-Speed-Limit-Layer-from-Bylaws)
  - [How to Measure Success Rates](#How-to-Measure-Success-Rates)
- [Where did the bylaws fail](#Where-did-the-bylaws-fail)
3. [Information on Subfolders](#Information-on-Subfolders)

## Description of Project Problem

We were given a file of approximately 7000 descriptions of locations on Toronto streets (about 2000 of them are repealed), 
and each location represented a change to the posted speed limit on a certain stretch or road. 
Speed limit changes are enacted through bylaws in the City of Toronto, so each record was from a bylaw. Each record also contains and ID (the ID of the location/text description), bylaw number, 
a date for when the bylaw was passed, the original speed limit of the location, and the updated speed. 

Our job is to use this file to update the Toronto centreline file so it contains speed limit data. 

The final speed limit layer is in `gis.bylaws_speed_limit_layer`, however, it does not include accurate speed limit on highways. For speed limit on highways, please see table `gis.bylaws_speed_limit_layer_hwy`. Note the limitation that the speed limits on ramps are assumed to be the same as the expressways.


## Methodology 

See the [text to centreline](https://github.com/CityofToronto/bdit_data-sources/tree/master/gis/text_to_centreline) documentation for information on our algorithm that matches bylaw text descriptions to centreline street segment geometries. 

The function was mainly created to process the City of Toronto's [transportation bylaw data](https://open.toronto.ca/dataset/traffic-and-parking-by-law-schedules/). We have already used previous versions of this process for [posted speed limits](https://github.com/CityofToronto/bdit_data-sources/tree/master/gis/speed_limit) on streets in the City, locations of [community safety zones](https://github.com/CityofToronto/bdit_vz_programs/tree/master/safety_zones/commuity_safety_zone_bylaws), [turn restrictions](https://github.com/CityofToronto/bdit_vz_programs/blob/master/notebooks/Turn%20Restrictions.ipynb), etc. The function can handle most bylaws, even more advanced cases. Any limitations that we are currently aware of will be discussed in the [Outstanding Work](#Outstanding-Work) area of the document. It should also be noted that some bylaws are incorrect (for many reasons, such as a described intersection not existing), and our function cannot correctly match a lot of these bylaws, since the data is incorrect. Sometimes the function will match the bylaws incorrectly and other times it will return an error.

The folder named `manual` includes past work on transforming bylaws into centreline segments, including python code to transform the xml bylaws on Open Data into Postgresql to process. More on that can be found at this [README](manual/README.md).


### How Well Does This Work

Out of 5163 bylaws that are not repealed as of January 2020, 4956 of them got converted successfully. That means that we have an overall success rate of 96%! The number of bylaws fall into each case type and the percentage of successfully matched is shown in the table below. Those that failed are discussed in ["Where did the bylaws fail"](#Where-did-the-bylaws-fail) section below. The following subsection shows how each case type is found. 

|case type | number of bylaws | number of bylaws matched | % successfully matched|
|--|--|--|--|
|entire length|21|20|95%|
|normal (two intersections without any offset)|4815|4684|97%|
|case 1 (one intersection and one offset)|68|55|81%|
|case 2 (two intersections and at least one offset)|259|197|76%|


### How to Create the Speed Limit Layer from Bylaws

The text to centreline function was created to read bylaws and return the centrelines involved, be it partial or complete. The following steps were done in order to prepare the final bylaws speed limit layer. 

#### Run text_to_centreline() to generate geometries
 
Using the function `gis.text_to_centreline`, convert all bylaws text into centrelines and put the results into a table named `gis.bylaws_routing`. The query used is as shown below and can also be found [here](sql/table-bylaws_routing.sql). **Note** that there were a couple of them that raised a warning message and you can find out more in [Outstanding Work - Tackle Cases with Known Geom Error](#Tackle-cases-with-known-geom-error).

```sql
SET client_min_messages = warning; 
--only show warning messages that I would like to know
CREATE TABLE gis.bylaws_routing AS
SELECT 
    law.*, 
    results.*
FROM gis.bylaws_to_update law, --bylaws where deleted = false
LATERAL gis.text_to_centreline(
    law.id,
    law.highway,
    law.between,
    NULL) as results
```

However, the function does not include the `date_added` and `date_repealed`. We will need to join to `gis.bylaws_added_repealed_dates` to retrieve date information.

```sql
CREATE TABLE gis.bylaws_routing_dates AS
SELECT result.*, dates.date_added, dates.date_repealed FROM gis.bylaws_routing result
LEFT JOIN gis.bylaws_added_repealed_dates dates
USING (id)
```

#### Incorporate Centrelines without Bylaws and Cut Centrelines

The previous step only converts all bylaws into centrelines and do not include centrelines that are not stated in the bylaws. This [mat view query](sql/mat-view-bylaws_speed_limit_layer.sql) categorizes bylaws into different parts and incorporates that into the centreline layer into a mat view named `gis.bylaws_speed_limit_layer`. **This is the final bylaws speed limit layer if one is not interested in the expressways speed limit.** We check if the centrelines are involved in any bylaws, if they are not, set the speed limit to 50km/h. If they are just partially included in the bylaws, we check if there's another bylaw that governs that centreline. If there is, apply the next bylaw; If there is none, set the speed limit to 50km/h. For a centreline that is included partially in more than one bylaws, it falls into the part_two category. The function categorizes centrelines based on how the processed bylaw geometries intersect with the original geometry (not all, entirely, partially, or multiple bylaws). The picture below communicates that idea with the orange lines being the centrelines, highlighted in yellow being where bylaws applied, and the names above the lines being each category which corresponds to the CTE name.

![](jpg/centrelines_categorized.jpg)

This query may seem long but it is technically just handling the bylaws in a few parts.

1. no_bylaw -> No bylaw matched, therefore default of 50 km/hr for urban street applies

2. whole_added -> centrelines involved in bylaws, be it fully or partially

3. part_one_without_bylaw -> parts of centrelines not involved in bylaws if there isn't a next applicable bylaw

4. part_two -> for the partial centrelines, include the next bylaw that applies to it if exists

5. part_two_without_bylaw -> for partial centrelines where next bylaw has been applied to it, the remaining part of the centreline not involved in the bylaws

Some explanation on the long code:

i) [L25](sql/mat-view-bylaws_centreline_categorized.sql#L25): `AND ST_AsText(bylaws.line_geom) != 'GEOMETRYCOLLECTION EMPTY'` is used here as some geom produced from the function returns "unreadable" geom as the centreline is not involved in the bylaws but is found between the two given intersections. It normally happens for bylaws that are in case 1 or case 2.

ii) [L65](sql/mat-view-bylaws_centreline_categorized.sql#L65): ` (centreline.fcode_desc::text = ANY (ARRAY['Collector'::character varying, ...` is used here to only include relevant centrelines from `gis.centreline`.

iii) [L84](sql/mat-view-bylaws_centreline_categorized.sql#L84) `WHERE whole_added.section IS NOT NULL AND whole_added.section <> '[0,1]'::numrange` is used to find centrelines where bylaws are only applied to a part of it.

iv) [L123](sql/mat-view-bylaws_centreline_categorized.sql#L123) `WHERE bylaws.geo_id = one.geo_id AND (bylaws.date_added < one.date_added OR bylaws.id < one.bylaw_id)` is used to find the previous bylaws according to the `date_added`. Since not all bylaws have `date_added`, the `id` is used instead with larger values representing more recently applied bylaws.

v) [L135](sql/mat-view-bylaws_centreline_categorized.sql#L135) `st_difference(next_bylaw.geom, st_buffer(part_one.geom, 0.00001::double precision)) AS geom,` is used to find the difference between the current and previous bylaw centrelines for part two cases. Note that st_difference needs st_buffer (a really small buffer) here to work properly or else st_difference will not return any results. This may be due to the fact that the geometry might not be exactly the same after the unioning and trimming.

vi) The following few lines which occurred at the UNION part for the CTE next_bylaw, part_two, part_one_without_bylaw, part_two_without_bylaw is there to ensure that the geom is exactly the same as the geom from `gis.centreline`. This geom is more accurate as it does not involve any st_buffer, st_difference, st_union etc.

```sql
CASE WHEN bylaw.section IS NOT NULL 
THEN st_linesubstring(cl.geom, lower(bylaw.section)::double precision, upper(bylaw.section)::double precision)
ELSE cl.geom
END AS geom,
```

The final output table will look like this
|bylaw_id|lf_name|geo_id|speed_limit|int1|int2|con|note|geom|section|oid1_geom|oid1_geom_translated|oid2_geom|oid2_geom_translated|date_added|date_repealed|
|--|--|--|--|--|--|--|--|--|--|--|--|--|--|--|--|
|1805|	Harvie Ave	|2350209|	40	|13461402|	NULL|	Very High (100% match)|	highway2:...| ...	|(0.896704452511841,0.89770788980652]	|...	|...|	NULL|	NULL	|NULL	|NULL|
|1806|	Harvie Ave|	2350209|	30|	NULL|	NULL|	High (1 character difference)|	highway2:...|...|	[0,0.896704452511841]	|...|	NULL|	...|	...|	NULL|	NULL|
|NULL	|Harvie Ave|	2350209|	50|	NULL|	NULL|	NULL|	NULL|	...|	(0.89770788980652,1]|	NULL	|NULL|	NULL|	NULL	|NULL	|NULL|
|3762|	Traymore Cres	|1146129|	40	|13467350	|NULL|	Very High (100% match)|	highway2:...|...|	\[0,0.372174765194242)	|...|	...|	NULL	|NULL	|NULL|	NULL|
|3763	|Traymore Cres|	1146129	|40|	13467350|	13467108|	Very High (100% match)|	highway2:...| ...|	[0.372174765194242,1]	|...|	...|	...|	NULL|	NULL	|NULL|
|6583|	Glenvale Blvd|	127|	30|	13455526|	13455120	|Very High (100% match)|	highway2:...| ...|	NULL|	...|NULL|		...|	NULL|	01/15/2019	|NULL|	
|NULL|Broadway Ave|	129|	50	|	NULL|NULL|NULL|NULL|...|NULL|||||NULL|NULL|								

Look at 
- Harvie Ave which is categorized as or part_two from [here](#Incorporate-Centrelines-without-Bylaws-and-Cut-Centrelines), the latest bylaw is applied to the centreline partially and the other part of the centreline is either filled with the previous bylaws or the speed limit is just set to 50 if there isn't any previous bylaws applied to that part of the centreline. 
- Traymore Cres is categorized as part_one from [here](#Incorporate-Centrelines-without-Bylaws-and-Cut-Centrelines) where the centreline is partially governed by a bylaw and the other part is governed by an older bylaw. 
- Glencale Blvd is categorized whole_added from [here](#Incorporate-Centrelines-without-Bylaws-and-Cut-Centrelines) where the whole centreline is related to a bylaw.
- Broadway Ave is categorized as no_bylaw from [here](#Incorporate-Centrelines-without-Bylaws-and-Cut-Centrelines) as there is no bylaw governing that centreline.

#### Final Clean Up for Expressway

##### *`gis.bylaws_speed_limit_layer_hwy`*

Bylaw we recevied do not include expressways, and their speed limit is greater than 50kmh (the default for all street). Speed limit for highways in the table `gis.bylaws_speed_limit_layer_hwy` are manually updated using information found online for each highway. See (sql/create-table-bylaws_speed_limit_layer_hwy.sql) for steps. 

*Note that there is still uncertainty of ramp speeds and they are now assumed to have the same speed limit as the expressways.*

### How to Measure Success Rates

The steps below summarize how we find the case type for all bylaws from the table `gis.bylaws_2020` which is the bylaws information gotten directly via email. Some cleaning needs to be done to only include bylaws that are not deleted aka need to be processed as well as removing those that are weird (those that got repealed but not deleted & those that were not cleaned nicely.) The first query below is to clean up the bylaws to find out the components/variables from the bylaws text which are to be used in later process and to be used to categorize them into different case type. The second query then only include all bylaws that has to be processed to centrelines. The third query shows how I find out the bylaws that fall into different categories. In short, I will list out the tables used in sequence right here.

i) `gis.bylaws_2020` - bylaws table provided

ii) `gis.bylaws_2020_cleaned` - table that only contains the cleaned text and variables to be used in other process and does not contain other information besides `id`, `highway` and `between` from `gis.bylaws_2020`

iii) `gis.bylaws_to_update` - table that only contains bylaws that have to be updated aka not repealed

iv) Finding bylaws in different case types.

Table `gis.bylaws_2020` looks like this.
|id|city|deleted|bylaw_no|chapter|schedule|schedule_name|highway|between|speed_limit_km_per_h|
|--|--|--|--|--|--|--|--|--|--|
|5878||false|\[Added 2018-01-16 by By-law 68-2018]|950|35|Speed Limits on Public Highways|Windsor Street|Front Street West and Wellington Street West|30|

```sql
CREATE TABLE gis.bylaws_2020_cleaned AS 
SELECT clean_bylaws.* 
FROM gis.bylaws_2020, LATERAL gis.clean_bylaws_text(id, highway, between, NULL) AS clean_bylaws
```

Table `gis.bylaws_2020_cleaned` looks like this.
|bylaw_id|highway2|btwn1|direction_btwn1|metres_btwn1|btwn2|direction_btwn2|metres_btwn2|btwn2_orig|btwn2_check
|--|--|--|--|--|--|--|--|--|--|
|5878|Windsor St|Front St W|||Wellington St W|||Wellington St W|Wellington St W|

```sql
CREATE TABLE gis.bylaws_to_update AS
SELECT law.*
FROM gis.bylaws_2020 law
WHERE law.deleted = false 
AND (law.bylaw_no NOT LIKE '%Repealed%' OR law.bylaw_no IS NULL) 
--to exclude those that got repealed but not deleted (6 of them which are bylaw_id = 4571, 6350, 6477, 6512, 6565, 6566)
AND law.id IN (SELECT bylaw_id FROM gis.bylaws_2020_cleaned)
--to exclude those not cleaned nicely (4 of them which are bylaw_id = 2207,2208,2830?,6326)
```

Table `gis.bylaws_to_update` has the same format and columns as `gis.bylaws_2020`.

```sql
WITH entire AS (
SELECT * FROM gis.bylaws_2020_cleaned
WHERE bylaw_id IN (SELECT id FROM gis.bylaws_to_update)
AND TRIM(btwn1) ILIKE '%entire length%' 
AND btwn2 IS NULL
--ENIRE LENGTH (21 ROWS)
),
normal AS (
SELECT * FROM gis.bylaws_2020_cleaned
WHERE bylaw_id IN (SELECT id FROM gis.bylaws_to_update)
AND COALESCE(metres_btwn1, metres_btwn2) IS NULL 
AND TRIM(btwn1) NOT ILIKE '%entire length%'
--NORMAL CASES (4815 ROWS)
),
case1 AS (
SELECT * FROM gis.bylaws_2020_cleaned
WHERE bylaw_id IN (SELECT id FROM gis.bylaws_to_update)
AND btwn1 = btwn2 AND COALESCE(metres_btwn1, metres_btwn2) IS NOT NULL 
--AN INTERXN AND AN OFFSET(68 ROWS)	
)
SELECT * FROM gis.bylaws_2020_cleaned
WHERE bylaw_id IN (SELECT id FROM gis.bylaws_to_update)
AND bylaw_id NOT IN (SELECT bylaw_id FROM entire)
AND bylaw_id NOT IN (SELECT bylaw_id FROM normal)
AND bylaw_id NOT IN (SELECT bylaw_id FROM case1)
--ELSE AKA TWO INTERXN AND AT LEAST ONE OFFSET (259 ROWS)
```

In order to produce the results from the table in [How Well Does This Work](#How-Well-Does-This-Work), the query used is exactly the same as above except that this one line is added to the end of each CTE: `AND bylaw_id NOT IN (SELECT DISTINCT id FROM gis.bylaws_routing)` .

## Where did the bylaws fail

A check was done to find out at which stage the bylaws failed. Using the function `gis.text_to_centreline`, there are still 207 bylaws out of 5163 cleaned bylaws (or 211 bylaws out of 5167 bylaws that are not repealed and not cleaned) that need to be processed that fail to be converted into centrelines. If one would like to find the differences of the results found between using the old and new text_to_centreline process, go to this [issue #293](https://github.com/CityofToronto/bdit_data-sources/issues/293) but note that the number there might not reflect the latest results. Whereas more details about how the failed bylaws look like and why exactly do they fail, go to this [issue #298](https://github.com/CityofToronto/bdit_data-sources/issues/298). 

Below shows a table on exactly how many bylaws failed at different stage. Please refer to the flow chart [here](#how-the-function-works) for a better picture. The csv dump [here](csv/failed_bylaws.csv) (also in table `gis.failed_bylaws`) contains all bylaws that failed to be converted into centrelines and also the reason behind. There are, in total, 211 bylaws that do not get processed (note that these are all bylaws where deleted = false).

|failed_reason|function|stage |# of bylaws that failed here|
|--|--|--|--|
|1|`gis._clean_bylaws_text`|bylaws text could not be cleaned due to the way they are phrased |3|
|2|`gis._get_entire_length`|bylaws categorized as entire length case did not return any result|1|
|3|`gis._get_intersection_geom`|no int_id is not found for the second intersection only|83|
|4|`gis._get_intersection_geom`|no int_id is not found for both intersections|67|
|5|`gis._get_lines_btwn_intexn`|the intersections could not be routed|15| 
|6|`gis._centreline_case1`|bylaws categorized as case 1 did not return any result|0|
|7|`gis._centreline_case2`|no results for this case2 due to 1st argument isnt a line|8|
|8|`gis._centreline_case2`|no results for this case2 due to 2nd argument isnt within \[0,1]|10|
|9|`gis._centreline_case2`|no results for this case2 due to 3rd argument isnt within \[0,1]|12|
|10|`gis.text_to_centreline`|bylaws failed at the final stage|1|

**Note (Reasons of failing):** 
- The fact that failed_reason = 6 has 0 failed bylaws does not mean that no case1 failed. It just means that case1 failed at other stage before even reaching the function `gis._centreline_case1()`.
- For failed_reason = 1 or 2, it is due to the way the bylaws text was written.
- For failed_reason = 3 or 4, the 150 bylaws in total that failed at that stage are due to [Direction stated on bylaws is not taken into account](#Direction-stated-on-bylaws-is-not-taken-into-account), [Levenshtein distance can fail for streets that have E / W](#Levenshtein-distance-can-fail-for-streets-that-have-E--W), [Duplicate street name and former municipality element ignored](#Duplicate-street-name-and-former-municipality-element-ignored) and [Tackle Cases with "an intersection and two offsets"](#Tackle-Cases-with-an-intersection-and-two-offsets) .
- For failed_reason = 5, [pgRouting returns the shortest path but street name different from highway](#pgRouting-returns-the-shortest-path-but-street-name-different-from-highway) is the main reason that they are failing.
- For failed_reason = 6 or 7 or 8 or 9 or 10, look at the messages returned in the [section Tackle cases with known geom error](#Tackle-cases-with-known-geom-error) to find out more. They are mainly due to an error at `line_locate_point` or `line_interpolate_point`.

Query used to get the above results are
```sql
--failed_reason = 1
SELECT * FROM gis.bylaws_2020
WHERE id NOT IN (SELECT bylaw_id FROM gis.bylaws_2020_cleaned)
AND deleted = FALSE

--failed_reason = 2
SELECT * FROM gis.bylaws_2020_cleaned
WHERE bylaw_id IN (SELECT id FROM gis.bylaws_to_update)
AND TRIM(btwn1) ILIKE '%entire length%' 
AND btwn2 IS NULL
AND bylaw_id NOT IN (SELECT id FROM gis.bylaws_routing)

--failed_reason = 3
SELECT * FROM gis.bylaws_found_id
WHERE int2 IS NULL
AND int1 IS NOT NULL

--failed_reason = 4
SELECT * FROM gis.bylaws_found_id
WHERE int1 IS NULL
AND int2 IS NULL

--failed_reason = 5
--HOWEVER, the following query returns 81 rows, though 55 of them are actually routed but just do not have int1 or int2 aka they are case1
--THEREFORE, 81 - 55 = 26 cases that failed to be routed
SELECT * FROM gis.bylaws_found_id
WHERE int1 IS NOT NULL
AND int2 IS NOT NULL
AND id NOT IN (SELECT bylaw_id FROM gis.bylaws_found_routes)
```

In order to know exactly at which stage did they fail, I have to separate out the steps into different parts, namely finding int_id, finding routes, trimming centrelines and see where they fail.

Using tables continued from [section Usage - How to Measure Success Rates](#how-to-measure-success-rates),

i) `gis.bylaws_to_route` - Table with bylaws that need to be routed.

ii) `gis.bylaws_found_id` - Tables with found id using function [`gis.bylaws_get_id_to_route()`](sql/helper_functions/function-bylaws_get_id_to_route.sql) 

iii) `gis.bylaws_found_routes` - Tables with found routes using function [`gis.bylaws_route_id()`](sql/helper_functions/function-bylaws_route_id.sql)

*Note that the code in gis.bylaws_get_id_to_route() and gis.bylaws_route_id() are pretty much the same as that in gis._get_intersection_geom and gis._get_lines_btwn_interxn except that the output is slightly modified to enable further investigation on the failed bylaws.

```sql
CREATE TABLE gis.bylaws_to_route AS
WITH entire AS (
SELECT * FROM gis.bylaws_2020_cleaned
WHERE bylaw_id IN (SELECT id FROM gis.bylaws_to_update)
AND TRIM(btwn1) ILIKE '%entire length%' 
AND btwn2 IS NULL
)
SELECT * FROM gis.bylaws_2020_cleaned
WHERE bylaw_id IN (SELECT id FROM gis.bylaws_to_update)
AND bylaw_id NOT IN (SELECT bylaw_id FROM entire)
```

Table `gis.bylaws_to_route` contains 5142 rows of bylaws and looks like this. 
|bylaw_id|highway2|btwn1|direction_btwn1|metres_btwn1|btwn2|direction_btwn2|metres_btwn2|btwn2_orig|btwn2_check|
|--|--|--|--|--|--|--|--|--|--|
|6872|Warden Ave|Mack Ave|north|305|St. Clair Ave E|||St. Clair Ave E|St. Clair Ave E|

```sql
CREATE TABLE gis.bylaws_found_id AS
WITH selection AS (
SELECT * FROM gis.bylaws_to_update
WHERE id IN (SELECT bylaw_id FROM gis.bylaws_to_route)
)
SELECT law.*, results.*
FROM selection law,
LATERAL gis.bylaws_get_id_to_route(
law.id,
law.highway,
law.between,
NULL
) as results
```

Table `gis.bylaws_found_id` contains 5142 rows and looks like this. 
|id|city|deleted|bylaw_no|chapter|schedule|schedule_name|highway|between|speed_limit_km_per_h|bylaw_id|note|highway2|int1|int2|oid1_geom|oid1_geom_translated|oid2_geom|oid2_geom_translated|lev_sum1|lev_sum2|
|--|--|--|--|--|--|--|--|--|--|--|--|--|--|--|--|--|--|--|--|--|
|6872|TO|false|\[Added 2019-07-18 by By-law 1201-2019]|950|35|Speed Limits on Public Highways|Warden Avenue|A point 305 metres north of Mack Avenue and St. Clair Avenue East|60|6872|highway2: Warden Ave btwn1:  Mack Ave btwn2: St. Clair Ave E metres_btwn1: 305 metres_btwn2:  direction_btwn1: north direction_btwn2: |Warden Ave|13457981|13455952|...|...|...||0|1|

```sql
CREATE TABLE gis.bylaws_found_routes AS 
WITH select_id AS (
SELECT * FROM gis.bylaws_found_id
WHERE int1 IS NOT NULL
AND int2 IS NOT NULL
)
SELECT ids.highway, ids.between, ids.note, rout.*
FROM select_id ids,
LATERAL gis.bylaws_route_id(
ids.id,
ids.highway2,
ids.int1,
ids.int2
) AS rout
```

Table `gis.bylaws_found_routes` contains 26646 rows (as a bylaw can return multiple centrelines) and looks like this. 
|highway|between|note|bylaw_id|int_start|int_end|line_geom|seq|geo_id|lf_name|objectid|fcode|fode_desc|
|--|--|--|--|--|--|--|--|--|--|--|--|--|
|Warden Avenue|A point 305 metres north of Mack Avenue and St. Clair Avenue East|highway2: Warden Ave btwn1:  Mack Ave btwn2: St. Clair Ave E metres_btwn1: 305 metres_btwn2:  direction_btwn1: north direction_btwn2:|6872|13457981|13455952|...|1|112744|Warden Ave|21501|201300|Minor Arterial|


## Information on Subfolders

This folder contains 3 subfolders: `automated`, `manual`, and `original_documentation_summer_2018`. 

The `original_documentation_summer_2018` folder contains the documentation for our original process for creating an updated speed limit layer. It contains a `README` with a lot of code. This `README` was created before the `text_to_centreline` function existed, and it is basically goes through the process for how to match bylaw text to geometries, with not code that is not the most efficient. This speed limit layer that was created from the code in this repo was used in the [Vision Zero Challenge](https://www.toronto.ca/services-payments/streets-parking-transportation/road-safety/vision-zero/educational-campaigns/vision-zero-challenge/) during the Summer of 2018, and the location of the output layer can be found [here](https://github.com/CityofToronto/vz_challenge/tree/master/transportation/posted_speed_limits). 

The `manual` folder contains an attempt to get a perfect posted speed limit layer of the City, which was worked on mainly in `April 2019`. A lot of maunal work was done in this case because there are many bylaws that are written very incorrectly. In the end, there were still around 150/8000 bylaw text fields that were unmatched and the matched were not `QC`'d

The `automated` folder is the most recently updated folder. It has work from the most recent update of the speed limit file (August 2019). The confidence in the matching of this file is fairly high. However, no manual changes were made to the matched/unmatched geometries and we know for a fact there are issues with some of the matches. These general issues were explored in the [QC subfolder](./automated/QC). There are also over 200 bylaws that were not matched to centreline segments. 