# Table of Contents
- [Traffic Patterns: Traffic Models](#traffic-patterns-traffic-models)
    - [Traffic Patterns: Data Model](#traffic-patterns-data-model)
    - [Converting Traffic Patterns](#converting-traffic-patterns)
- [Sad news](#sad-news)

# Traffic Patterns: Traffic Models

Just like the sun doesn't always shine, the streets of Toronto don't always produce vehicle probe speeds. In those cases, HERE provides us with "traffic patterns," a model for each street link by time of week. This dataset comes in a big honking `tar.gz`. Here are some handy notes for navigating and uploading this data.

`tar -tf traffic_patterns_18.tar.gz` lists the contents of the archive, revealing a `.zip` file and a PDF of documentation. `unzip -l RELATIONAL_NTP_NA_LINK_181H0.zip` lists the contents of the `.zip` file, which are more `.zip` files:

```bash
  Length      Date    Time    Name
---------  ---------- -----   ----
     2536  2019-01-24 09:17   NTP_NA_HOLIDAYAPPENDIX_181H0.zip
235350311  2019-01-24 09:18   NTP_REF_NA_LINK_FC1-4_181H0.zip
761010921  2019-01-24 09:22   NTP_REF_NA_LINK_FC5_181H0.zip
  1995095  2019-01-24 09:23   NTP_SPD_NA_181H0.zip
```

The two main large files are the reference tables which list the relationship
between each link and travel direction, and the model for that speed
(`NTP_REF_NA_LINK_*.zip`), the models themselves are orders of magnitude
smaller. You will also notice that these files are for all of North America, so
quite a bit of filtering is required not to bloat the data sent to the
database. You can specify which files to unzip from an archive. For example,
the below command unzips only the `.zip` file for functional classes 1-4:

```bash
unzip RELATIONAL_NTP_NA_LINK_181H0.zip NTP_REF_NA_LINK_FC1-4_181H0.zip
```

You must use the `-p` flag to unzip to a pipe, otherwise `unzip` will print
some information about the file being uncompressed to stdout. The below command
uses `csvgrep` & `csvcut` to filter the country code lookup table to only
canadian links and stores only the link column to be used to filter the link
ref data in the next command. This uses `grep` and the `-f FILE` flag to use a
a file as input for the filter list. Note that `-F` must also be used [since the
filter strings are literal strings](https://unix.stackexchange.com/questions/83260/reading-grep-patterns-from-a-file).

```bash
unzip -p NTP_REF_NA_LINK_FC1-4_181H0.zip NTP_COUNTRYLUT_NA_LINK_FC1-4_181H0.csv | csvgrep -c COUNTRY_CODE -m CAN | csvcut -c LINK_PVID > canadian_links.csv
unzip -p NTP_REF_NA_LINK_FC1-4_181H0.zip NTP_REF_NA_LINK_FC1-4_181H0.csv | grep -F -f canadian_links.csv - | csvcut -c LINK_PVID,TRAVEL_DIRECTION,U,M,T,W,R,F,S|  psql -h 10.160.12.47 -d bigdata -c "\COPY here.traffic_pattern_18_ref FROM STDIN WITH (FORMAT csv, HEADER TRUE);"
```

The same two steps can be repeated with the functional code 5 data.

The speed models are in `NTP_SPD_NA_181H0.zip`, the contents of which are
pretty straightforward and can be sent to `traffic_pattern_spd_15` & `traffic_pattern_spd_60`.

```bash
$ unzip RELATIONAL_NTP_NA_LINK_181H0.zip NTP_SPD_NA_181H0.zip
Archive:  NTP_SPD_NA_181H0.zip
  Length      Date    Time    Name
---------  ---------- -----   ----
  4522703  2019-01-24 09:17   NTP_SPD_NA_15MIN_KPH_181H0.csv
  4305198  2019-01-24 09:17   NTP_SPD_NA_15MIN_MPH_181H0.csv
  1199868  2019-01-24 09:17   NTP_SPD_NA_60MIN_KPH_181H0.csv
  1147077  2019-01-24 09:17   NTP_SPD_NA_60MIN_MPH_181H0.csv
---------                     -------

$ unzip -p NTP_SPD_NA_181H0.zip NTP_SPD_NA_15MIN_KPH_181H0.csv | psql -h 10.160.12.47 -d bigdata -c "\COPY here.traffic_pattern_spd_15 FROM STDIN WITH (FORMAT csv, HEADER TRUE);"
```

After these files are uploaded, the tables need to be converted to long format
using the [SQL below](#converting-traffic-patterns).

## Traffic Patterns: Data Model

`sql/create_traffic_patterns.sql` contains the SQL to create the tables to contain Traffic Patterns. There are 15-min and 60-min models, which have a basic wide structure of `{pattern_id, h00_00, h00_015, [...], h23_45}` where `hHH_MM` is the speed value for `pattern_id` for that time of day. Both tables share the same `pattern_ids`, which can be found in the lookup reference table `here.traffic_pattern_YY_ref` (where `YY` is the year of the model). This table is of the format `{link_pvid, travel_direction, u, m, t, w, r, f, s}` where each of those letter columns contains a `pattern_id` for that combination of `link_dir` and `day of the week` starting with sUnday.

These wide-format tables are converted to the more relational narrow format with the [sql referenced below](#converting-traffic-patterns) :point_down:.

`here.traffic_pattern_YY_ref_narrow`

|column | type | definition |
|-------|------|------------|
|link_dir |text | link direction |
|isodow |integer | ISO Day of Week|
|pattern_id |integer | id referring to the pattern for that link_dir, day of week combination|

`here.traffic_pattern_YY_spd_MM_narrow` (15 & 60 minute patterns have the same structure)

|column | type | definition |
|-------|------|------------|
|pattern_id| integer| ID for this pattern|
|trange| timerange| Time range for which this pattern applies|
|pattern_speed| integer|Speed in km/hr for that pattern_id & time range|

## Converting Traffic Patterns

[`sql/convert_traffic_patterns.sql`](sql/convert_traffic_patterns.sql) contains multiple queries to convert traffic patterns from their wide format into something that is easier to query. The queries all use [`json_build_object(VARIADIC "any")`](https://devdocs.io/postgresql~9.6/functions-json#json_build_object) to create a set of key-value pairs from the values in the columns.

For example, `json_build_object('isodow',0,'pattern_id', u)`, creates a row like `{'isodow':0, 'pattern_id': 8}`.

Next using [`json_build_array()`](https://devdocs.io/postgresql~9.6/functions-json#json_build_array) to create an array of those objects, example:

```sql
SELECT
    json_build_array(
        json_build_object('isodow', 0, 'pattern_id', u),
        json_build_object('isodow', 1, 'pattern_id', m)
    )
FROM here.traffic_pattern_18_ref
LIMIT 1
-- [{'isodow':0, 'pattern_id': 8},
--  {'isodow':1, 'pattern_id': 8}]
```

And finally [`json_to_recordset(json)`](https://devdocs.io/postgresql~9.6/functions-json#json_to_recordset) converts the array into a set of defined rows, for example, the below converts each row into 7 rows of `(isodow, pattern_id)` records, linked to the original `link_dir`

```sql
SELECT 
    link_pvid || travel_direction AS link_dir,
    isodow,
    pattern_id
INTO here.traffic_pattern_18_ref_narrow
FROM here.traffic_pattern_18_ref,
LATERAL ( 
    SELECT 
        isodow,
        pattern_id
    FROM json_to_recordset( json_build_array(
        json_build_object('isodow',0,'pattern_id', u),
        -- ...
        json_build_object('isodow',6,'pattern_id', s)
    ) )
);
```
## Generate Quarterly traffic patterns
For top secret contract reasons, we are no longer receiving traffic patterns data. The latest (and last) traffic patterns dataset that we received is from 2019. As time goes by, and the street network evolves, traffic patterns will cease to be useful. We wil be creating quaterly traffic patterns for each version for routing purposes. 

Depending on the date you are routing, you should use different quaters, years, and map version of traffic patterns. We will only include weekday (Tue - Thurs), and exclude holiday seasons to create a more [generalized traffic pattern layer](/sql/create_quaterly_traffic_patterns.sql).


