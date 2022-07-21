# [DATASET NAME] CENTRELINE Dataset
[DESCRIPTION] The Toronto Centreline is a data set of linear features representing streets, walkways, rivers, railways, highways and administrative boundaries within the City of Toronto.

<table class="tg">

  <tr>
    <td class="tg-fymr"><span style="font-weight:bold">Source type</span></td>
    <td class="tg-fymr"><span style="font-weight:bold">Keywords</span></td>
    <td class="tg-fymr"><span style="font-weight:bold">Last update</span></td>
    <td class="tg-fymr"><span style="font-weight:bold">Location</span></td>
    <td class="tg-0pky"><span style="font-weight:bold">Location</span></td>
  </tr>
  <tr>
    <td class="tg-0pky">Infrastructure</td>
    <td class="tg-0pky">such as Source, type, location, short-time (long-term), program, team.</td>
    <td class="tg-0pky">2022</td>
    <td class="tg-0pky">Toronto, Ontario, Canada</td>
    <td class="tg-0pky">Toronto, Ontario, Canada</td>
  </tr>
</tbody>
</table>


<BR>

## More Information
 
||                 |
|----------------------------|---------------|
| **Contact**                      | example@info.ca |
| **Access link**                        | GCC link                |
| **Available format**            |SHP, Excel, GeoJson                |
| **Temporal coverage**           |recent (up to update date)                 |
| **Geographical coverage**       |Examples: `{(xmin,ymin), (xmax,ymax)}`, number of points (intersections, segments), regions, wards, `EPSG=XXXXX`, `CRS=WGS84`               |
| **What to use it for**          |As base map                 |
| **Date of data collection**     | Data gets updates when some changes applied to real-world features                |
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

### **Quality Concerns:**
> list code/symbol and definition

### **Data Dictionary (Optional):**
<details>
  <summary>fcode_descriptions</summary>
</details>
<BR>
<BR>

# Relevant Datasets

| Related Data            | Related field      | Read More                                         |
|-------------------------|--------------------|-----------------------------------------------|
| Centreline_intersection | {`tnode`, `fnode`} | link to a readme of `centreline_intersection` |
