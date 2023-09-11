
# Text Description to Centreline Geometry Automation

- [Intro](#Intro)
- [Usage](#Usage)
  - [Function text_to_centreline](#Function-text_to_centreline)
    - [Inputs](#Inputs)
    - [Outputs](#Outputs)
    - [Example Usage](#Example-Usage)
  - [Function text_to_centreline_geom](#Function-text_to_centreline_geom)
    - [Inputs](#Inputs)
    - [Outputs](#Outputs)
    - [Example Usage](#Example-Usage)
- [How the Function Works](#How-the-Function-Works)
  - [Step 1: Clean the data](#Step-1-Clean-the-data)
  - [Step 2: Separate into different cases](#Step-2-Separate-into-different-cases)
    - [2a) Entire Length](#2a-Entire-Length)
    - [2b) Normal Cases - Two Intersections](#2b-Normal-Cases---Two-Intersections)
    - [2c) Special Case 1 - An Intersection and An Offset](#2c-Special-Case-1---An-Intersection-and-An-Offset) 
    - [2d) Special Case 2 - Two Intersections and At Least One Offset](#2d-Special-Case-2---Two-Intersections-and-At-Least-One-Offset)
      - [The logic behind dealing with geom to be trimmed](#The-logic-behind-dealing-with-geom-to-be-trimmed)
  - [Confidence output](#Confidence-output)
- [Future Enhancement](#Future-Enhancement)
  - [Wrapper function for `clean_bylaws_text`](#Wrapper-function-for-clean_bylaws_text)
  - [Include `date_added` and `date_reapealed`](#Include-date_added-and-date_repealed)
  - [Rename `highway` and `btwn`](#Rename-highway-and-btwn)
  - [Reset ramps speed limit](#Reset-ramps-speed-limit)
  - [Use directional centrelines](#Use-directional-centrelines)
  - [Improve sensitivity in `section`](#Improve-sensitivity-in-section)
- [Outstanding Work](#Outstanding-Work) 
  - [Tackle Cases with Known Geom Error](#tackle-cases-with-known-geom-error)
  - [pgRouting returns the shortest path but street name different from `highway`](#pgrouting-returns-the-shortest-path-but-street-name-different-from-highway)
  - [Direction stated on bylaws is not taken into account](#direction-stated-on-bylaws-is-not-taken-into-account)
  - [Levenshtein distance can fail for streets that have E / W](#levenshtein-distance-can-fail-for-streets-that-have-e--w)
  - [Duplicate street name and former municipality element ignored ](#duplicate-street-name-and-former-municipality-element-ignored)
  - [Tackle Cases with "an intersection and two offsets"](#tackle-cases-with-an-intersection-and-two-offsets)
  - [Modify `con` (confidence level) definition to better reflect actual situation](#modify-con-confidence-level-definition-to-better-reflect-actual-situation)

## Intro

This is a `README` for the text to centreline function, which is written in `postgresql`. The general purpose of this function is to take an input text description of a street location in the City of Toronto, and return an output of centreline segments that match this description. This function was originally written for [automating bylaw's geometries](https://github.com/CityofToronto/bdit_data-sources/tree/master/gis/speed_limit), thus most example and variable name are based on bylaw. 
The input typically takes 1) the street, and 2) the two intersection where the street begins and ends. For example, you could use the function to get the centreline segments of Bloor Street from Royal York Road to St George Street. 

Depending on your use case, there are two functions you could use:
- [`gis.text_to_centreline`](sql/function-text_to_centreline.sql)

    - If you are only interested in *all* centreline information, including geo_id, road class, street name, intersection id, etc

- [`gis.text_to_centreline_geom`](sql/function-text_to_centreline_geom.sql)

    -  If you are only interested in finding the geometry

## Usage

### Function text_to_centreline

#### Inputs
The function takes four inputs:
- `_bylaw_id`: Unique ID  
- `highway`: Street
- `frm`: From cross street
- `to`: To cross street

*This function was created mainly for automating bylaw's geometries, these inputs were named after bylaw's column names.* 

If you want the centreline segment on Bloor from Royal York Road and St George Street:

|\_bylaw_id|highway|from|to|
|----------|--------|---|---|
|1| Bloor Street |Royal York Road | St George Street |

In this case you would input:
- `1` (unique id) as `_bylaw_id`
- `Bloor Street` as `highway`
- `Royal York Road` as `frm`
- `St George Street` as `to`

However if the street you are automating is in another format:

|\_bylaw_id|highway|between|
|----------|--------|---|
|1| Bloor Street|Between Royal York Road and St George Street |

In this case you would input:
- `1` as `_bylaw_id`
- `Bloor Street` as `highway`
- `Between Royal York Road and St George Street` as `frm`
- `NULL` as `to`.

This function can handle text descriptions of four different categories, all of which are differences in the `between`:

| case   type                                          | text description in between                                                                                                                   | Example                                                                                                                                                        |
|------------------------------------------------------|-----------------------------------------------------------------------------------------------------------------------------------------------|----------------------------------------------------------------------------------------------------------------------------------------------------------------|
| entire   length                                      | Entire length                                                                                                                                 | gis.text_to_centreline(1, 'Richland Crescent', 'Entire   Length', NULL)                                                                                        |
| normal   (two intersections without any offset)      | [intersection1 street] and   [intersection2 street]                                                                                           | gis.text_to_centreline(2, 'Foxridge Drive', 'Birchmount Road and Kennedy Road', NULL)                                                                          |
| case   1 (one intersection and one offset)           | [intersection1 street] and a   point [xx] meters [cardinal direction ie North]                                                                | gis.text_to_centreline(3, 'Ravenwood   Place', 'Ferris Road   and a point 64.11 metres southwest', NULL)                                                       |
| case   2 (two intersections and at least one offset) | A point [xx] meters [cardinal   direction] of [intersection1 street] and a point [xx] meters [cardinal   direction] of [intersection2 street] | gis.text_to_centreline(4, 'Brimorton Drive', 'A point 101   metres west of Amberjack Boulevard and a point 103 metres east of Dolly   Varden Boulevard', NULL) |

#### Outputs
The function will then returns a table as shown below. Note the below is only showing a single row each from the results of the above sample queries. The `con` value represents how close the names of the input values are to the intersections they were matched to. More about that can be found at the [confidence output](#Confidence-output) section.

|int1|	int2|	geo_id|	lf_name|	con|	note|	line_geom|	section|	oid1_geom|	oid1_geom_translated|	oid2_geom|	oid2_geom_translated|	objectid|	fcode	|fcode_desc|
|--|--|--|--|--|--|--|--|--|--|--|--|--|--|--|
|13443051|	13442736|	104679	|Placentia Blvd|	Very High (100% match)|	highway2:...| ...|NULL|...||...||		43183	|201400|	Collector|
|13457875|	NULL|	3835696|	Druid Crt|	Very High (100% match)|	highway2: ...|...|	[0,1]|	...|	...|	NULL|	NULL|	21243	|201500|	Local|

#### Example Usage

The above-mentioned query is only for a single use case. However, we normally use the function to run through a table and so the following will show how we can use the function on a table instead. The function takes in `_bylaw_id` as a variable, but really that is just an id for one to identify which row went wrong for troubleshooting. Say if one does not have any id associated with the rows in a table, simply put `NULL` as the variable.

```sql
SELECT * FROM gis.text_to_centreline
(2958, 'Placentia Boulevard', 'Kenhatch Boulevard and Sandhurst Circle', NULL);

SELECT * FROM gis.text_to_centreline
(NULL, 'Druid Court', 'Ferris Road and a point 78.19 metres north', NULL) ;
```

### Function text_to_centreline_geom

There is also a wrapper function named [`gis.text_to_centreline_geom`](sql/function-text_to_centreline_geom.sql) which takes in 3 variable, namely `_street`, `_from_loc`, `_to_loc` and only returns geometry (`_return_geom`) found from the function `gis.text_to_centreline`. If one is only interested in finding the geometry from two intersections given in text-based descriptions, this is a really useful function.

#### Inputs
The function takes four inputs:
- `_street`: Street 
- `_from_loc`: From cross street
- `_to_loc`: To cross street

#### Outputs

- `text_to_centerline_geom`: A single line geometry of the street

#### Example Usage

```sql
SELECT gis.text_to_centreline_geom('Bloor Street', 'Yonge Street', 'Parliament Street')
```
Returns one single line of geometry on Bloor Street from Yonge to Parliament.

![image](https://user-images.githubusercontent.com/46324452/229189145-c5a0dc84-367e-4d22-9941-556b13f27691.png)

# How the Function Works

The main steps for every type of input (complex or not so complex) are:

1. Cleaning the data
2. Match location text data to intersections
3. Find lines (centreline segments) between matched intersections using pgRouting

The process will be explained in further detail below. The functions called/variables assigned by these functions can be visualized with the following flow chart ![](jpg/text_to_centreline.jpeg)

## Step 1: Clean the data

The first step is to clean the location description data so it can easily be matched to intersections in the `gis.centreline_intersection` table.

We clean the data mainly using the [`gis.abbr_street`](sql/helper_functions/function-abbr_street.sql) function. The intersection table writes roads with appreviations. For example the word `street` is written as `St` and `road` is `Rd`, etc.

We want to be able to clean the following input variables:

1. `highway2` (`TEXT`): the street name
2. `btwn1` and `btwn2` (`TEXT`): the start and end street intersection name
3. `metres_btwn1` and `metres_btwn2` (`NUMERIC`): The number of metres away from the intersections. For example on `Bloor Street` between `100` metres east of `Royal York Road` and `300` metres west of `St George Street`, then `metres_btwn1` would be `100` and `metres_btwn2` would be `300`. These fields are `NULL` for everything except for [special case 1](#2c-Special-Case-1---An-Intersection-and-An-Offset)  and [special case 2](#2d-Special-Case-2---Two-Intersections-and-At-Least-One-Offset)
4. `direction_btwn1` and `direction_btwn2` (`TEXT`): Direction away from the intersections. will be null for everything except for [special case 1](#2c-Special-Case-1---An-Intersection-and-An-Offset)  and [special case 2](#2d-Special-Case-2---Two-Intersections-and-At-Least-One-Offset).. These are directions for points away from the intersections. For example on `Bloor Street` between `100` metres east of `Royal York Road` and `300` metres west of `St George Street`, then `direction_btwn1` would be `east` and `direction_btwn2` would be `west`.

There are different cases for how the data is input ([see Usage](#Usage)), so both of those cases should have to be cleaned differently, hence there is a lot of code like: `CASE WHEN t IS NULL THEN ` .... `ELSE`. An idea to make this cleaner in the future could be to make 2 different functions for cleaning inputs.

The cleaning text function currently in the main function is [`gis._clean_bylaws_text()`](sql/function-clean_bylaws_text.sql). The big chunk of cleaning function is now separated and since the function returns composite types and there are many columns involved, it's easier to return them as a table type. More explanation [here at 36.4.7. SQL Functions as Table Sources](https://www.postgresql.org/docs/9.6/xfunc-sql.html).

It is also possible to return multiple variable types without the involvement of a table which is using the `OUT` term when creating the function as shown in [gis.\_clean_bylaws_text()](sql/function-clean_bylaws_text.sql).
More explanation [here at 41.3.1. Declaring Function Parameters](https://www.postgresql.org/docs/9.6/plpgsql-declarations.html)

Sample query to just clean up the text would look like this:
```sql
SELECT cleaned_table.* 
FROM gis.bylaws_2020 -- table you want to clean up
CROSS JOIN LATERAL gis._clean_bylaws_text(id, highway, between, NULL) AS cleaned_table
```

## Step 2: Separate into different cases

### 2a) Entire Length

**If `TRIM(_frm) ILIKE '%entire length%' AND _to IS NULL`, then it falls into this case.**

If the street segment occurs on the entire length of the street then call a special function named [`gis._get_entire_length()`](sql/function-get_entire_length.sql). This function selects all centreline segments in the City of Toronto with the exact name of `highway2`. In this case the street name has to be exact, and if the street name is misspelled then there will be no output geometry. There could potentially be an issue with how this code is written because some streets in the city have the same name but appear in different districts (i.e. Etobicoke and North York). To be solved, currently in issue [#281 Use the former municipality element of the "highway" field](https://github.com/CityofToronto/bdit_data-sources/issues/281)

For example,
```sql
SELECT * FROM gis.text_to_centreline(3128, 'Richland Crescent', 'Entire Length', NULL)
```
and the result looks like this

![](jpg/entire_length.JPG)


### 2b) Normal Cases - Two Intersections

**If `COALESCE(metres_btwn1, metres_btwn2) IS NULL`, then the bylaw is not a special case.**

The function [`gis._get_intersection_geom()`](sql/function-get_intersection_geom.sql) is the main function that is called to get the geometry of the intersections between which the bylaw is in effect. The function returns an array with the geometry of the intersection and the `objectid` (unique `ID`) of the intersection. If the `direction` and `metres` values that are inputted to the function are not `NULL`, then the function returns a translated intersection geometry (translated in the direction specified by the number of metres specified). The function takes a value `not_int_id` as an input. This is an intersection `int_id` (intersection `ID`) that we do not want the function to return. We use `int_id` instead of `objectid` since sometimes there are intersection points that are in the exact same location but have different `objectid` values. This is a parameter to this function because sometimes streets can intersect more than once, and we do not want the algorithm to match to the same intersection twice.

In  most cases, the function `gis._get_intersection_geom()` calls on another function named [`gis._get_intersection_id()`](sql/helper_functions/function-get_intersection_id.sql). This function returns the `objectid` and `intersection id` of the intersection, as well as how close the match was (where closeness is measured by levenshtein distance). The query in this function works by gathering all of the streets from the City of Toronto intersection streets table that have the same/very similar names to the streets that are described in the bylaw description provided as input to the `text_to_centreline` function (i.e. `btwn1`, `btwn2`, `highway2`). 

If there is more than one street with the same unique intersection ID in this subset, then this means the street from `highway2` and the first street from `between` (aka the street from `btwn1`) have been matched. We can use a `HAVING` clause (i.e. `HAVING COUNT(DISTINCT TRIM(intersections.street)) > 1`) to ensure that only the intersections that have been matched to both street names are chosen. The `gis.centreline_intersection_streets` view (that is called in this function) assigns a unique ID to each intersection in the City of Toronto (`gis.centreline_intersection`). Each row contains one street name from an intersection and the ID associated with the intersection.

If the names for `highway` and `btwn` are the same, the `gis._get_intersection_geom()` calls on the function named [`gis._get_intersection_id_highway_equals_btwn()`](sql/helper_functions/function-get_intersection_id_highway_equals_btwn.sql). This function is intended for cases where the intersection that we want is a cul de sac or a dead end or a pseudo intersection. In these cases the intersection would just be the name of the street. `not_int_id` is a parameter of this function as well since some streets both start and end with an intersection that is a cul de sac or pseudo intersection. The process to find the appropriate intersection is very similar to the `get_intersection_id` function, except it does not have the `HAVING COUNT(intersections.street) > 1`. This is because cul de sac or pseudo intersections intersection only have one entry in the `gis.centreline_intersection_streets` view (since there is only one road involved in the intersection).

The `oid1_geom` and `oid2_geom` values that are assigned in the `text_to_centreline` function and represented the (sometimes) translated geometry of the intersections. 

Once the id and geometry of the intersections are found, the lines between the two points are then found using pgRouting. The function [`gis._get_lines_btwn_interxn()`](sql/function-get_lines_btwn_interxn.sql) is used to find the centrelines between the two intersection points using the table `gis.centreline_routing_undirected`. More information about pgRouting can be found [here](https://github.com/CityofToronto/bdit_data_tools/tree/routing/routing). The one used here is undirected as we want to get the shortest route between the two points. Directionality is to be added into the future process, currently in issue [#276 Speed Limit Layer Enhancement (directional)](https://github.com/CityofToronto/bdit_data-sources/issues/276).

For example,
```sql
SELECT * FROM gis.text_to_centreline(6974, 'Foxridge Drive', 'Birchmount Road and Kennedy Road', NULL)
```
and the result looks like this

![](jpg/normal_case.JPG)


### 2c) Special Case 1 - An Intersection and An Offset

**If `clean_bylaws.btwn1 = clean_bylaws.btwn2`, then the bylaw is special case 1.**

There are some records that start and/or end at locations that are a certain amount of metres away from an intersection. We create a method to assign a geometry to the locations of these bylaws. These segments do not start at one intersection and end at another, they often start/end in the middle of centreline segments. The function used for this case is [`gis._centreline_case1`](sql/function-centreline_case1.sql)

`btwn2` is formatted like: "a point (insert number here) metres (direction - north/south/east/west)"

The point is located on the street in the `highway` column a certain number of metres away from the intersection identified in `btwn1`.

Example: `highway` = "Billingham Rd" `btwn` = "Dundas St and a point 100 metres north"

These records can be filtered with the WHERE clause: `btwn2 LIKE '%point%'`

**The workflow for this case (An Intersection and An Offset) is:**

i) Create a temp table `_wip` as some function may return multiple rows.

ii) Using [`gis._get_intersection_geom()`](sql/function-get_intersection_geom.sql) to get the intersection id & geometry of that one intersection as well as the translated geometry according to the bylaws. The function [`gis._translate_intersection_point()`](sql/helper_functions/function-translate_intersection_point.sql) is used to translate the intersection point and the point is rotated at an angle of 17.5 to make the city boundary an almost vertical or horizontal line.

iii) A `new_line`, connecting the intersection point to the translated point, is created using ST_MakeLine.

iv) Create a buffer of `3*metres_btwn2` around the `new_line` and find the centrelines within the buffer that have the same name as `highway2`. The centrelines found from the previous steps are then inserted into the temp table `_wip`.

v) After combining all centrelines using `ST_LineMerge(ST_Union(individual_centreline_geom))`, the line is cut according to the metres stated on bylaws and put into the column `line_geom_cut`. Note that it is important to find the start_point/end_point of how the line is drawn so that we know how much we should add or subtract from that one intersection we found. 

vi) `combined_section` specifies the section of the centrelines that was trimmed from the original combined centrelines line_geom named `whole_centreline`, whether it's from the start_point to a cut point ('[0, 0.5678]') or from a cut point to the end_point ('[0.1234, 1]').

vii) Make sure that the `ind_line_geom` (which is the original individual line geometry for each geo_id) and `line_geom_cut` (which is the complete line geometry that matches the text description and got trimmed if neccessary) has the same orientation ie drawn in the same direction and insert that into a new column named `line_geom_reversed`.

viii) Create a small buffer around `line_geom_reversed` and find out if the `ind_line_geom` is within the buffer or how much of the centreline is within the buffer. With that, `line_geom` which is the final individual centrelines (trimmed when needed) aka the wanted output of this function is found. Centrelines are now separated into their respective `geo_id` rows.

ix) Update the `section` column which specifies the section of the centrelines that was trimmed from the original individual centrelines line_geom named `ind_line_geom`, whether it's from the start_point to a cut point ('[0, 0.5678]') or from a cut point to the end_point ('[0.1234, 1]').

x) Return every single rows from the temp table with columns that we need.

For example,
```sql
SELECT * FROM gis.text_to_centreline(6440, 'Ravenwood Place', 'Ferris Road and a point 64.11 metres southwest', NULL)
```
and the result looks like this

![](jpg/case1.JPG)

### 2d) Special Case 2 - Two Intersections and At Least One Offset

**If the bylaws does not fall into any cases stated above, then it is special case 2.**

`btwn1` or `btwn2` formatted like: "(number) metres (direction) of (insert street name that is not btwn1 or highway)"

The point is located a certain number if metres away from the specified intersection. The intersection is an intersection between the street in the `highway` column and the (insert street name that is in `between`).

These records can be filtered with the WHERE clause: `btwn LIKE '%metres%of%`

Example: street = "Watson Avenue" btwn = "Between St Marks Road and 100 metres north of St Johns Road"

For this case, we need to find the intersections *St. Marks and Watson Avenue* and *St. Johns Road and Watson Avenue*. Then find a point that is 100 metres north of the intersection of *St. Johns Road and Watson Avenue*.

The work flow for this case is very similar to 2c except now that we have the two intersection points, we can use pgRouting to link the two points and then add/trim centrelines according to the bylaws. The function used for this case is [`gis._centreline_case2`](sql/function-centreline_case2.sql)

**The workflow for this case (Two Intersections and At Least One Offset) is:**

i) Create a temp table `_wip2` as some function may return multiple rows.

ii) Using `gis._get_intersection_geom()` to get the intersection id & geometry of that one intersection as well as the translated geometry according to the bylaws. The function [`gis._translate_intersection_point()`](sql/helper_functions/function-translate_intersection_point.sql) is used to translate the intersection point and the point is rotated at an angle of 17.5 to make the city boundary an almost vertical or horizontal line.

iii) Using `gis._get_lines_btwn_interxn()` pgRouting to find all relevant centrelines between the two intersections.

iv) `new_line1`, connecting the intersection point `oid1_geom` to the translated point `oid1_geom_translated`, and `new_line2`, connecting the intersection point `oid2_geom` to the translated point `oid2_geom_translated`, is created using ST_MakeLine.

v) Create a buffer of `3*metres` around `new_line1` and `new_line2` and find the centrelines within the buffer that have the same name as `highway2`. The centrelines found from the previous steps are then inserted into the temp table `_wip2`.

vi) Combined all centrelines found within the buffer as well as pgRouting into `whole_centreline`.

vii) Combine all centrelines found using pgRouting into `pgrout_centreline`, can be filetered by setting `seq` IS NOT NULL.

viii) Deal with the first intersection `oid1_geom`. Details can be found [below](#The-logic-behind-dealing-with-geom-to-be-trimmed).

ix) Then deal with the second intersection `oid2_geom`. The math logic is the same as the previous step which can be found [below](#The-logic-behind-dealing-with-geom-to-be-trimmed).

x) Make sure that the `ind_line_geom` and `line_geom_cut` has the same orientation ie drawn in the same direction and insert that into a new column named `line_geom_reversed`.

viii) Create a small buffer around `line_geom_reversed` and find out if the `ind_line_geom` is within the buffer or how much of the centreline is within the buffer. With that, `line_geom` which is the final individual centrelines (trimmed when needed) aka the wanted output of this function is found. Centrelines are now separated into their respective `geo_id` rows.

ix) Update the `section` column which specifies the section of the centrelines that was trimmed from the original individual centrelines line_geom named `ind_line_geom`, whether it's from the start_point to a cut point ('[0, 0.5678]') or from a cut point to the end_point ('[0.1234, 1]').

x) Return every single rows from the temp table with columns that we need.

For example,
```sql
SELECT * FROM gis.text_to_centreline
(6668, 'Brimorton Drive', 'A point 101 metres west of Amberjack Boulevard and a point 103 metres east of Dolly Varden Boulevard', NULL)
```
and the result looks like this

![](jpg/case2.JPG)

#### The logic behind dealing with geom to be trimmed
To explain some logic used in the function `gis._centreline_case2()`. The part of the code used particularly to match/trim geometry can be found [here](sql/function-centreline_case2.sql#L124-L175). `pgrout_centreline` is used as the baseline and then centreline is added or trimmed where applicable according to the bylaws. If addition of centrelines is needed, then `ST_Union` is used (see A1 & A2); if subtraction/trimming of centrelines is needed, then `ST_Difference` is used (see B1 & B2). The only difference between A1 and A2 is the draw direction and the same goes for B1 and B2.

The image below shows the four possible cases (A1, A2, B1 and B2)
![image](https://user-images.githubusercontent.com/54872846/77707649-284df380-6f9c-11ea-9509-b0db0cdf24c5.png)


## Confidence output

The function outputs a confidence level (`con`), which is defined after the `centreline_segments` variable. This value represents how close the names of the input values are to the intersections they were matched to. Specifically, this value is the sum of the levenshtien distance between the `highway2` value and the street name it matched to and the `btwn1`/`btwn2` value and the street name it matched to. It is the sum of this value for both intersections.

# Future Enhancement

The mega function `gis.text_to_centreline` currently works fine on its own. However, there are a few parts of it that can be done better for better clarity and for easier use.

## Wrapper function for `clean_bylaws_text`
This can also be found in [issue #319](https://github.com/CityofToronto/bdit_data-sources/issues/319). A wrapper function is defined as an endpoint to make a function more user friendly for a specific use case. The current function [`gis._clean_bylaws_text`](sql/function-clean_bylaws_text.sql) takes in 4 inputs although most of the time, our inputs are really just 3 inputs with the `btwn` text containing both `frm & to`. Therefore, a cleaner function can be done here. The slightly complicated part is that this function does not output a table or a variable but rather it outputs a record in a format of a pre-defined table.

## Include `date_added` and `date_repealed`
The function does not output these two information but these two columns are rather important in determining which bylaw is the latest one and which bylaw is the previous one that governs a particular street. This is essential in producing the final bylaws speed limit layer. These two information was found by extracting the information from the `bylaw_no` column from the table provided. The query can be found [here](sql/table-bylaws_added_repealed_dates.sql).

## Rename `highway` and `btwn`
These two column names are first used because that's how the table provided names them. I personally also find it really confusing as the term `highway` here simply means the street where the bylaw is applied to whereas the term `btwn` means the other two streets that intersect with the street where the bylaw is applied aka the start and end point of the street. Given that these two variables or even certain variation of them are used throughout the whole text_to_centreline function, it can be pretty taxing to rename all of them to sth more sensible.

## Reset ramps speed limit
The speed limit on the ramps is currently assumed to be the same as that on the expressways which is not realistic. The ramps can be found by filtering the category in the table `gis.centreline` whereas the speed limit can be found by Googling.

## Use directional centrelines
Current process uses routing with the network being `gis.centreline_routing_undirected_lfname` which contains undirectional centrelines. This can be a problem because for streets with median, only a single path is routed with the current process which is taking the shortest path that matches the street names. Note that by implementing this, solving this [outstanding issue](#direction-stated-on-bylaws-is-not-taken-into-account) will be substantially easier.

## Improve sensitivity in `section`
There is a comment made on the [part](#incorporate-centrelines-without-bylaws-and-cut-centrelines) about incorporating centrelines without bylaws and only assigning the latest bylaw if the same centreline is found in different bylaws. The problem occurs when a centreline is partially involved in bylaw_B and partially involved in bylaw_A (which is the earlier on bylaw) and partially not involved in any bylaw. As shown in the table for bylaw_id = 1805, the very first row has a `section` of '(0.896704452511841,0.89770788980652]' which is extremely minute and can be filtered. The point is that, normally the new bylaw is used to overwrite the old bylaw but due to the way the bylaws texts are phrased, the centrelines outputs might be slightly different and we want the process to be able to filter that out, particularly on the sensitivty of the column `section`.

# Outstanding Work

There is a [Github Issue](https://github.com/CityofToronto/bdit_data-sources/issues/188) on weird bylaws that haven't been matched from Chelsea's work. \
I summarised them into the 3 points below: \
i) The streets stated in bylaws are slightly off i.e. they do not intersect or are rather far apart. \
ii) Directionality matters. Northbound and Southbound route between two intersection are of different centrelines such as the Allen Rd. \
iii) The error message `line_locate_point: 1st arg isn't a line` returned for 49 distinct id's. This mostly occurs when trying to slice a multilinestring.

To be honest, since the new process is rather different from Chelsea's process, it is hard to say if those problems are solved. However, her outstanding problem is kind of related to some issues that have been encountered with the new processes but may not have been solved yet. The issues are listed below, followed by how they are found and how one could tackle them. **Note that a bylaw failing may be due to a combination of a couple of reasons and the list below is not exhaustive. Only the ones of greater significance are stated below.**

## Tackle Cases with Known Geom Error

This can be found at [issue #320](https://github.com/CityofToronto/bdit_data-sources/issues/320). When trying to run the mega function on all the bylaws, below are the warning messages I found which may lead us to how we can improve the current function. This problem is related to (iii) above under [Outstanding Work](#outstanding-work). For my case, I found 31 of them and the log is shown below where most of them are related to the street centrelines being not continuous. 

```sql
WARNING:  Internal error at case2 for highway2 = Aylesworth Ave , btwn1 = Midland Ave, btwn2 = Phillip Ave : 'line_interpolate_point: 2nd arg isn't within [0,1]' 
WARNING:  Internal error at case2 for highway2 = Cranbrooke Ave , btwn1 = Barse St, btwn2 = Grey Rd : 'line_interpolate_point: 2nd arg isn't within [0,1]' 
WARNING:  Internal error at case2 for highway2 = Firgrove Cres , btwn1 =  Petiole Rd, btwn2 = Jane St : 'line_locate_point: 1st arg isn't a line' 
WARNING:  Internal error at mega for bylaw_id = 1632 : 'could not open relation with OID 1406444995' 
WARNING:  Internal error at case2 for highway2 = Hillhurst Blvd , btwn1 = Avenue Rd, btwn2 = Proudfoot Ave : 'line_interpolate_point: 3rd arg isn't within [0,1]' 
WARNING:  Internal error at case2 for highway2 = Japonica Rd , btwn1 = Crosland Dr, btwn2 = White Abbey Pk : 'line_interpolate_point: 3rd arg isn't within [0,1]' 
WARNING:  Internal error at case2 for highway2 = Lake Shore Blvd E , btwn1 =  Lower Jarvis St, btwn2 = Coxwell Ave : 'line_locate_point: 1st arg isn't a line' 
WARNING:  Internal error at case2 for highway2 = Lake Shore Blvd W , btwn1 = Humber River, btwn2 = Spadina Ave : 'line_locate_point: 1st arg isn't a line' 
WARNING:  Internal error at case2 for highway2 = Lonsmount Dr , btwn1 = Bathurst St, btwn2 = Montclair Ave : 'line_interpolate_point: 2nd arg isn't within [0,1]' 
WARNING:  Internal error at case2 for highway2 = Meadowvale Dr , btwn1 = Kenway Rd, btwn2 = Durban Rd : 'line_interpolate_point: 2nd arg isn't within [0,1]' 
WARNING:  Internal error at case2 for highway2 = Moore Ave , btwn1 = Mount Pleasant Rd, btwn2 = Welland Ave : 'line_interpolate_point: 3rd arg isn't within [0,1]' 
WARNING:  Internal error at case2 for highway2 = Queen St E , btwn1 = Victoria Park Ave, btwn2 = Fallingbrook Rd : 'line_interpolate_point: 2nd arg isn't within [0,1]' 
WARNING:  Internal error at case2 for highway2 = Rathburn Rd , btwn1 =  The West Mall, btwn2 = Melbert Rd : 'line_interpolate_point: 3rd arg isn't within [0,1]' 
WARNING:  Internal error at case2 for highway2 = Rexdale Blvd , btwn1 = Highway 427, btwn2 = Islington Ave : 'line_locate_point: 1st arg isn't a line' 
WARNING:  Internal error at case2 for highway2 = Symes Rd , btwn1 =  Terry Dr, btwn2 = Orman Ave : 'line_locate_point: 1st arg isn't a line' 
WARNING:  Internal error at case2 for highway2 = Topcliff Ave , btwn1 =  Demaris Ave, btwn2 = Driftwood Ave : 'line_interpolate_point: 2nd arg isn't within [0,1]' 
WARNING:  Internal error at case2 for highway2 = Craven Rd , btwn1 = Gerrard St E, btwn2 = Fairford Ave : 'line_interpolate_point: 3rd arg isn't within [0,1]' 
WARNING:  Internal error at case2 for highway2 = Leslie St , btwn1 = Queen St E, btwn2 = Ivy Ave : 'line_interpolate_point: 3rd arg isn't within [0,1]' 
WARNING:  Internal error at case2 for highway2 = Rhodes Ave , btwn1 = Gerrard St E, btwn2 = Fairford Ave : 'line_interpolate_point: 3rd arg isn't within [0,1]' 
WARNING:  Internal error at case2 for highway2 = Ryerson Ave , btwn1 = Queen St W, btwn2 = Carr St : 'line_interpolate_point: 3rd arg isn't within [0,1]' 
WARNING:  Internal error at case2 for highway2 = Paton Rd , btwn1 = Symington Ave, btwn2 = Rankin Cres : 'line_interpolate_point: 2nd arg isn't within [0,1]' 
WARNING:  Internal error at case2 for highway2 = Roblocke Ave , btwn1 = Irene Ave, btwn2 = Leeds St : 'line_interpolate_point: 3rd arg isn't within [0,1]' 
WARNING:  Internal error at case2 for highway2 = Bond Ave , btwn1 = Leslie St, btwn2 = Scarsdale Rd : 'line_interpolate_point: 3rd arg isn't within [0,1]' 
WARNING:  Internal error at case2 for highway2 = James St , btwn1 = Queen St W, btwn2 = Albert St : 'line_interpolate_point: 3rd arg isn't within [0,1]' 
WARNING:  Internal error at case2 for highway2 = Hillside Dr , btwn1 =  Gamble Ave, btwn2 = Don Valley Dr : 'line_interpolate_point: 2nd arg isn't within [0,1]' 
WARNING:  Internal error at case2 for highway2 = Redway Rd , btwn2 = Millwood Rd : 'line_locate_point: 1st arg isn't a line' 
WARNING:  Internal error at case2 for highway2 = Trent Ave , btwn1 = Danforth Ave, btwn2 = Ice Cream Lane : 'line_interpolate_point: 2nd arg isn't within [0,1]' 
WARNING:  Internal error at case2 for highway2 = Meighen Ave , btwn1 =  Medhurst Rd, btwn2 = Victoria Park Ave : 'line_interpolate_point: 2nd arg isn't within [0,1]' 
WARNING:  Internal error at case2 for highway2 = Merritt Rd , btwn1 =  Valor Blvd, btwn2 = St. Clair Ave E : 'line_locate_point: 1st arg isn't a line' 
WARNING:  Internal error at case2 for highway2 = Topham Rd , btwn1 = Tiago Ave, btwn2 = Valor Blvd : 'line_locate_point: 1st arg isn't a line' 
WARNING:  Internal error at case2 for highway2 = The Queensway , btwn1 =  The West Mall, btwn2 = Kipling Ave : 'line_interpolate_point: 3rd arg isn't within [0,1]' 
SELECT 26747

Query returned successfully in 1 hr 13 min.
```

The table below summarizes the error messages above.

|Error|# of bylaws|bylaw_id involved|
|--|--|--|
|'line_locate_point: 1st arg isn't a line'|8|1400, 2204, 2205, 3118, 3659, 5925, 6429, 6451|
|'line_interpolate_point: 2nd arg isn't within \[0,1]'|10|180, 920, 2354, 2541, 3037, 3746, 5309, 5909, 6278, 6428|
|'line_interpolate_point: 3rd arg isn't within \[0,1]'|12|1898, 2050, 2633, 3077, 4773, 4802, 4821, 4941, 5375, 5485, 5674, 6869|
|Internal error at mega for bylaw_id = 1632 : 'could not open relation with OID 1406444995' |1|1632|

## pgRouting returns the shortest path but street name different from `highway`

This can be found at [issue #268](https://github.com/CityofToronto/bdit_data-sources/issues/268#issuecomment-595973836). 
This happened for `bylaw_id` = 6577 where `highway` = 'Garthdale Court' and `between` = 'Overbrook Place and Purdon Drive'. The two intersection ids found are 13448816 and 13448300 (marked as red cross below). The blue line is the result from pg_routing whereas the highlighted yellow path indicates the road segment from the bylaw. This problem is partially solved after including levenshtein distance as the cost in the routing process to prefer the highway matching the streetname. There were 84 bylaws that failed previously but after applying levenshtein distance as a cost, [only 15 bylaws fail](https://github.com/CityofToronto/bdit_data-sources/issues/268).

![image](https://user-images.githubusercontent.com/54872846/76124099-f7475800-5fc7-11ea-865e-963edefa43be.png)

## Direction stated on bylaws is not taken into account

Which is also somehow related to problem (ii) stated above. This can be found at [issue #276](https://github.com/CityofToronto/bdit_data-sources/issues/276). For example: `highway` = 'Black Creek Drive (southbound)' and `between` = 'Eglinton Avenue West and a point 200 metres north of Weston Road'. The direction of the `highway` is mentioned in the bylaw but we don't use that and that's sth we can do better. Currently, we just assume that all centrelines found have the specified speed limit for both ways even though it was stated clearly in the bylaws that only southbound traffic needs to conform to that speed limit. I found that there are about 20 bylaws where the direction was explicitly stated.

We might be able to solve it by separating that direction during the cleaning the data process and taking that into account when finding the right centrelines.

## Levenshtein distance can fail for streets that have E / W

This can be found at [issue #279](https://github.com/CityofToronto/bdit_data-sources/issues/279). For example, bylaw_id = 6667, highway = 'Welland Ave' and between = 'Lawrence Ave W and Douglas Avenue'. Since changing 'Lawrence Ave W' to 'Lawrence Ave E' only takes 1 step whereas changing 'Welland Ave' to 'Welland Rd' takes 3 steps, the intersection point with the less levenshtein distance is chosen instead, resulting in the segments of road that does not reflect the bylaws text.

I also found that when there are three street names in the intersec5 column and two of the street names are almost the same with the slight change of E to W or vice versa (which happen particularly frequently for Yonge St like Adelaide St E / Yonge St / Adelaide St W, King St E / Yonge St / King St W, Bloor St E / Yonge St / Bloor St W etc), the intersection points found is weird because of the reason above.

## Duplicate street name and former municipality element ignored 

This can be found at [issue #281 - Include former municipality element of the "highway" field](https://github.com/CityofToronto/bdit_data-sources/issues/281). When a street name is duplicated or triplicated across the city due to amalgamation, there will be a two-character code in the highway field to specify which former municipality is referenced, this should be used to ensure the correct intersections are getting matched. Examples of the municipality is shown below.
![image](https://user-images.githubusercontent.com/54872846/78183571-21106500-7436-11ea-8b34-017c73736b48.png)

This is a problem because for `bylaw_id` = 6645 where `highway` = "McGillivray Ave" and `between` = "The west end of McGillivray Ave and Kelso Ave", it's routed as shown below and that is not right. Blue line represents the routed centreline where highlighted in yellow is the road segment we want and the two black crosses are the two intersections mentioned in the bylaw text. This happen because there are two 'McGillivray Ave' in Toronto (one is highlighted in maroon and the other is the black cross on the left) and they both are cul de sac. 
![image](https://user-images.githubusercontent.com/54872846/75820184-e4374c80-5d69-11ea-8360-2e1725106ef9.png)

## Tackle Cases with "an intersection and two offsets"

This can be found at [issue #289](https://github.com/CityofToronto/bdit_data-sources/issues/289). There are about 9 cases of bylaws where there's only one intersection (btwn1 = btwn2) but two offsets (metres_btwn1 IS NOT NULL & metres_btwn2 IS NOT NULL). Examples as shown below.
![image](https://user-images.githubusercontent.com/54872846/77802945-60177280-7052-11ea-85de-aae1132d5786.png)

## Modify `con` (confidence level) definition to better reflect actual situation

This can be found as a part of [issue #270](https://github.com/CityofToronto/bdit_data-sources/issues/270). The `con` output may need to be modified in order to reflect the actual situation as they currently showing high confidence level for those where the street has E/W or streets that are in other former municipality. It might also be a good idea to indicate if the centrelines are found from the pgRouting process or from the bufferring and locating process.
