# GENERAL INFORMATION

| CENTRELINE Dataset                                                                                                                                                    |                 |                 |                 |                          |
|------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|-----------------|-----------------|-----------------|--------------------------|
| **Description**                                                                                                                                                              | **Source type** | **Keywords**    | **Last update** | **Location**             |
| The Toronto Centreline is a data set of linear features representing streets, walkways, rivers, railways, highways and administrative boundaries within the City of Toronto. | Infrastructure  | such as Source, type, location, short-time (long-term), program, team.  | 2022            | Toronto, Ontario, Canada |


<BR>

## More Information
 
||                 |
|----------------------------|---------------|
| **Contact**                      | example@info.ca |
| **Access link**                        | GCC link                |
| **Available format**            | SHP, Excel, GeoJson                |
| **Temporal coverage**           |recent (up to update date)                 |
| **Geographical coverage**       |Examples: `{(xmin,ymin), (xmax,ymax)}`, number of points (intersections, segments), regions, wards, `EPSG=XXXXX`, `CRS=WGS84`               |
| **What to use it for**          |As base map                 |
| **Date of data collection**     |                |
| **Collected by**                | Examples: division, unit, programs                |


<BR>



# Data 
 [centreline]

**What is included in the dataset**
|                                |            |
|--------------------------------|------------|
| **Number of features**         |     67769  |
| **Number of categorical data** |           |
| **Number of numerical data**   |          |
| **Geometry**                   |     Yes    |

<details>
  <summary>List of columns and definitions</summary>
  
  | column name | type                       | Example           | details                  |
|-------------|----------------------------|-------------------|--------------------------|
| gid         | integer  (Not Null)        | 63974             |                          |
| geo_id      | numeric (10)               | 30079678          | a unified id             |
| lfn_id      | numeric(10)                | 19155             | line feature name id     |
| lf_name     | character varying(110)     | Waterfront Trl    |                          |
| address_l   | character varying(20)      | NULL              |                          |
| address_r   | character varying(20)      | NULL              |                          |
| oe_flag_l   | character varying(2)       | N                 |                          |
| oe_flag_r   | character varying(2)       | N                 |                          |
| lonuml      | integer                    | 0                 |                          |
| hinuml      | integer                    | 0                 |                          |
| lonumr      | integer                    | 0                 |                          |
| hinumr      | integer                    | 0                 |                          |
| fnode       | numeric (10)               | 30079676          | from-node                |
| tnode       | numeric (10)               | 30079656          | to-node                  |
| fcode       | integer                    | 204001            | feature code             |
| fcode_desc  | character varying(100)     | Trail             | feature code description |
| juris_code  | character varying(20)      | CITY OF TORONTO   |                          |
| objectid    | numeric                    | 189008            |                          |
| geom        | geometry (MultilineString) | 0105000020E6100.. |`WGS84`                      |
</details>

<BR>

### **Any quality concern:**
> list code/symbol and definition

### **Information on variables value (Optional):**
<details>
  <summary>fcode_descriptions</summary>
</details>
<BR>
<BR>

# Relevant Datasets

| Related Data            | Related field      | Read More                                         |
|-------------------------|--------------------|-----------------------------------------------|
| Centreline_intersection | {`tnode`, `fnode`} | link to a readme of `centreline_intersection` |
