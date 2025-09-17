<!-- TOC ignore:true -->
# Miovision Intersection Update Resources  

This readme contains information and resources on how to add/remove Miovision intersections from our data pipeline.  
For the main Miovision readme, see [here](../readme.md). 

<!-- TOC -->

- [Miovision Intersection Update Resources](#miovision-intersection-update-resources)
- [Removing Intersections](#removing-intersections)
- [Adding Intersections](#adding-intersections)
	- [Update `miovision_api.intersections`:](#update-miovision_apiintersections)
	- [Update `miovision_api.intersection_movements`](#update-miovision_apiintersection_movements)
	- [Backfill/Aggregate new intersection data](#backfillaggregate-new-intersection-data)
	- [Adding many intersections (Archived)](#adding-many-intersections-archived)
	- [Alternate Method of finding `px` (Archived)](#alternate-method-of-finding-px-archived)

<!-- /TOC -->

# Removing Intersections
Once we are informed of the decommissioned date of a Miovision camera, we can carry out the following steps.

1) Update the column `date_decommissioned` on table [`miovision_api.intersections`](../readme.md#intersections) to include the decommissioned date. The `date_decommissioned` is the date of the *last timestamp from the location* (so if the last row has a `datetime_bin` of '2020-06-15 18:39', the `date_decommissioned` is '2020-06-15').

2) Remove aggregated data on the date the camera is decommissioned. Manually remove decommissioned machines' data from aggregate tables using [function-clear-volumes_15min.sql](../sql/function/function-clear-volumes_15min.sql), [function-clear-volumes_15min_mvt.sql](../sql/function/function-clear-volumes_15min_mvt.sql). You can also manually delete `volumes_daily` table. Dont worry about other tables that they are linked to since we have set up the ON DELETE CASCADE functionality. If the machine is taken down on 2020-06-15, we are not aggregating any of the data on 2020-06-15 as it may stop working at any time of the day on that day.

3) Done. Removing intersections is short and simple.

# Adding Intersections
Adding intersections is not as simple as removing an intersection. We will first have to find out some information before proceeding to aggregating the data. The steps are outlined below.

## Update `miovision_api.intersections`:

Look at the table [`miovision_api.intersections`](../readme.md#intersections) to see what information about the new intersections is needed to update the table. The steps needed to find details such as id, coordinates, px, int_id, geom, which leg_restricted etc are described below. This process is normally done by a member of `miovision_admins`; otherwise you will need to prepare queries for an admin to run. 

1. **Name and ID**  
	The new intersection's `api_name`, `id`, `lat` and `lng` are automatically added by the `miovision_pull` pipeline each day from the [Miovision API](https://api.miovision.com/intersections) /intersections endpoint. 

2. **Intersection Name**
   The `intersection_name` is an internal name following the convention `[E / W street name] / [N / S street name]`.
		
3. **date installed**  
    to update `date_installed`, the following script can be run, where the temp table contains the first date of which a recording was made for a specific intersection_uid (previously defined). The output is then joined on the intersections table:
	
	```sql
	-- Drop temp table if it exists
	DROP TABLE IF EXISTS temp_min_dates;

	-- Recreate temp table with restriction to specific intersection_uids
	CREATE TEMP TABLE temp_min_dates AS
	SELECT intersection_uid, MIN(datetime_bin)::date AS min_datetime
	FROM miovision_api.volumes
	WHERE intersection_uid IN ('intersection_uids you want to input')
	GROUP BY intersection_uid;

	-- Create index
	CREATE INDEX idx_temp_intersection_uid ON temp_min_dates(intersection_uid);

	-- Perform the update
	UPDATE miovision_api.intersections AS i
	SET date_installed = t.min_datetime
	FROM temp_min_dates AS t
	WHERE i.intersection_uid = t.intersection_uid;

	```

4.  **date_decommissioned**  
    `date_decommissioned` is described under [#removing-intersections](#removing-intersections). 
		
5. **px**  
    `px` is a uid used to identify signalized intersections. In most cases, `px` is easiest to find manually by searching the intersection name (location) in ITS Central (https://itscentral.corp.toronto.ca/) and finding the corresponding intersection id (PX####). `px` id can be used to look up the rest of the information (`street_main`, `street_cross`, `geom`, `lat`, `lng` and `int_id`) from table `gis.traffic_signal` as in the query below. Note that `px` is a zero padded text format in `gis.traffic_signal`, but stored as an integer in `miovision_api.intersections`. 

6. **Restricted legs**  
    In order to find out which leg of that intersection is restricted (**no cars approaching from that leg**), go to Google Map to find out the direction of traffic.

7. **Update Traffic Signal Info**  
    After identifying the Px number, you can grab some additional columns from `gis.traffic_signal`:

	```sql
	UPDATE miovision_api.intersections
	SET
		lat = ts.latitude,
		lng = ts.longitude,
		geom = ts.geom,
		street_main = ts.main_street,
		street_cross = ts.side1_street,
		int_id = ts.node_id,
		px = ts.px::integer
	FROM gis.traffic_signal AS ts
	WHERE ts.px = '0539' AND intersection_uid = 122
	```

8. **Update geojson**  
	Update the [geojson intersections file](../geojson/mio_intersections.geojson) using `ogr2ogr`. This geojson file is helpful as a publically accessible record of our Miovision intersections. You will have to make an issue and commit this change. 

```bash
cd ~/bdit_data-sources &&
rm -f volumes/miovision/geojson/mio_intersections.geojson &&
ogr2ogr -f "GeoJSON" volumes/miovision/geojson/mio_intersections.geojson PG:"host=trans-bdit-db-prod0-rds-smkrfjrhhbft.cpdcqisgj1fj.ca-central-1.rds.amazonaws.com dbname=bigdata" -sql "SELECT * FROM miovision_api.intersections ORDER BY date_installed" -nln miovision_installations
```

9. **Update `miovision_api.centreline_miovision`**

	[`miovision_api.centreline_miovision`](../sql/readme.md#centreline_miovision) links Miovision intersection legs to `gis_core.centreline` street segments. 

	Use [**this script**](../sql/updates/update-centreline_miovision.sql) to add new intersections to `centreline_miovision`. The script can automatically identify the correct direction and centreline segment for most Miovision intersections, but manual adjustments are needed for the following situations:
	- Segments are not aligned in a North-South or East-West direction (like Kingston Road)
	- Segments intersect at odd angles (like Kingston Road and Eglinton Avenue)
	- One or more "legs" is not a street segment (like the entrance to the shopping centre at Danforth and Jones)

	The script above also contains checks for duplicates and values missing from the table. 

## Update `miovision_api.intersection_movements`  

Now that the updated table of [`miovision_api.intersections`](../readme.md#intersections) is ready, we have to update the table [`miovision_api.intersection_movements`](../readme.md#intersection_movements). Intersection movements determines which movements should be aggregated, by classification, typically for reporting purposes. Yes, we can see all kinds of wacky behaviour out there, but analyzing that is rarer than reporting on the main movements, so this makes basic analysis a little bit easier.

We need to find out all valid movements for the new intersections from the data but we don't have that yet, so the following has to be done.

1. **~~Populate `miovision_api.volumes`~~**  
    `volumes` table is now automatically populated for new intersections since [#1214](https://github.com/CityofToronto/bdit_data-sources/pull/1214). 

2. **Insert into `intersection_movements`**  
    Now that there is data in `miovision_api.volumes`, run the SELECT query below and validate those new intersection movements. The line `HAVING COUNT(DISTINCT datetime_bin::time) >= 20` is there to make sure that the movement is actually legit and not just a single observation. `volume::numeric / classification_volume >= 0.005` is a suggested addition to make sure that for lower volume modes (bicycles), we don't filter out a small volume but large percentage (> 5 / 1000).  
	Next, INSERT INTO `intersection_movements` table which has all valid movements for intersections. These include decommissioned intersections, just in case we might need those in the future.

	```sql
	WITH counts AS (
		SELECT DISTINCT
			intersection_uid,
			classification_uid,
			leg,
			movement_uid,
			COUNT(DISTINCT datetime_bin::time) AS bins,
			SUM(volume) AS volume,
			SUM(SUM(volume)) OVER w AS classification_volume
		FROM miovision_api.volumes
		WHERE
			intersection_uid IN (67, 68) --only include new intersection_uid
			AND datetime_bin > 'now'::text::date - interval '10 days' -- or the date of data that you pulled
			AND classification_uid IN (1,2,6,10) --will include other modes after this
 			AND NOT (
 			    --exclude bike exits from aggregation (duplicate with entrance volumes)
			    classification_uid = 10 AND movement_uid = 8
 			)
		GROUP BY intersection_uid, classification_uid, leg, movement_uid
		WINDOW w AS (PARTITION BY intersection_uid, classification_uid)
	)

	-- Uncomment when you're ready to insert.
	-- INSERT INTO miovision_api.intersection_movements (intersection_uid, classification_uid, leg, movement_uid)
	SELECT
		intersection_uid,
		classification_uid,
		leg,
		movement_uid
	FROM counts
	WHERE
		bins >= 20 --consider omitting if using many days of data.
		OR volume::numeric / classification_volume >= 0.005
	```

	**Alternate method** - If you find you need to manually add movements to the above, download the output of the query into a CSV, manually edit the CSV, then
	append it to `miovision_api.intersection_movements` by modifying the below python snippet, (or use an SQL INSERT statement):

	```python
	import pandas as pd
	import psycopg2
	from psycopg2.extras import execute_values

	import configparser
	import pathlib

	# Insert code to read configuration settings.
	postgres_settings = {your_postgres_config}

	# Insert the name of your CSV file.
	df = pd.read_csv({your_file.csv})
	df_list = [list(row.values) for i, row in df.iterrows()]

	with psycopg2.connect(**postgres_settings) as conn:
		with conn.cursor() as cur:
			insert_data = """INSERT INTO miovision_api.intersection_movements(intersection_uid, classification_uid, leg, movement_uid) VALUES %s"""
			execute_values(cur, insert_data, df_list)
			if conn.notices != []:
				print(conn.notices)
	```


3. **Add additional modes to `intersection_movements`**  
    The step before only include valid intersection movements for
	`classification_uid IN (1,2,6,10)` which are light vehicles, cyclists and
	pedestrians. The reason is that the counts for other mode may not pass the
	mark of having 20 distinct datetime_bin. However, we know that if vehicles
	can make that turn, so can trucks, vans, buses and unclassified motorized
	vehicles, which are `classification_uid IN (3, 4, 5, 8, 9)`. Therefore, we
	will run the below query for all the classes not included in the previous
	steps, and all intersections under consideration.

	```sql
	-- Include all wanted classification_uids here.
	WITH wanted_veh(classification_uid) AS (
				VALUES (3), (4), (5), (8), (9)
	)
	INSERT INTO miovision_api.intersection_movements
		(intersection_uid, classification_uid, leg, movement_uid)
	SELECT
		a.intersection_uid,
		b.classification_uid,
		a.leg,
		a.movement_uid
	FROM miovision_api.intersection_movements AS a
	CROSS JOIN wanted_veh AS b
	-- Specify which intersection_uids to use.
	WHERE
		a.intersection_uid IN {INSERT_IDS_HERE}
		AND a.classification_uid = 1
	ORDER BY 1, 2, 3, 4
	```

4. **Review `intersection_movements`**  
    Once the above is finished, we have completed updating the table [`miovision_api.intersection_movements`](../readme.md#intersection_movements). **Though, the valid movements should be manually reviewed.**  
    Below is an example script + output you can use to aggregate movements into a more readable format for QC. In particular look for intersections with very short lists of valid movements, or no valid movements for certain classifications.  

    | intersection_uid | leg | movements                                                                                                                                                                                    |
    |------------------|-----|-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
    | 66               | E   | 1 - Through (1 - Light, 2 - Bicycle)<br>2 - Left (1 - Light)<br>3 - Right (1 - Light)<br>5 - Clockwise (6 - Pedestrian)<br>6 - Counter Clockwise (6 - Pedestrian)                             |
    | 66               | N   | 1 - Through (1 - Light, 2 - Bicycle)<br>2 - Left (1 - Light)<br>3 - Right (1 - Light, 2 - Bicycle)<br>5 - Clockwise (6 - Pedestrian)<br>6 - Counter Clockwise (6 - Pedestrian)                |
    | 66               | S   | 1 - Through (1 - Light)<br>2 - Left (1 - Light)<br>3 - Right (1 - Light)<br>5 - Clockwise (6 - Pedestrian)<br>6 - Counter Clockwise (6 - Pedestrian)                                          |
    | 66               | W   | 1 - Through (1 - Light)<br>2 - Left (1 - Light, 2 - Bicycle)<br>3 - Right (1 - Light)<br>4 - U-Turn (2 - Bicycle)<br>5 - Clockwise (6 - Pedestrian)<br>6 - Counter Clockwise (6 - Pedestrian) |

    ```sql
    WITH movements AS (
        SELECT
            intersection_uid, leg,
            movement_uid || ' - ' || movement_pretty_name || ' (' ||
                    string_agg(classification_uid || ' - ' || classification, ', '::text ORDER BY classification_uid) || ')'               
                AS mvmts
        FROM miovision_api.intersection_movements
        LEFT JOIN miovision_api.classifications USING (classification_uid)
        LEFT JOIN miovision_api.movements USING (movement_uid)
        WHERE intersection_uid IN (66) --adjust uid here
            AND classification_uid NOT IN (3, 4, 5, 8, 9) --since these just mirror lights
        GROUP BY
            intersection_uid,
            leg,
            movement_uid,
            movement_pretty_name
    )

    SELECT
        intersection_uid,
        leg,
        string_agg(mvmts, chr(10) ORDER BY mvmts) AS movements
    FROM movements
    GROUP BY
        intersection_uid,
        leg
    ORDER BY
        intersection_uid,
        leg
    ```

## Backfill/Aggregate new intersection data

Now that the intersection is configured and the raw volumes data is in the database, we have to finish aggregating the data.

1. **~~Backfill `miovision_api.volumes`~~**   

2. **Backfill additional tables**  
	Next use the [api script](../api/intersection_tmc.py) with `--agg` to backfill the aggregate tables between the date_installed and current date (exclusive). **Skip pulling data by omitting `--pull` flag.** See readme [here](../api/readme.md#how-to-run-the-api). 

3. **QC Aggregate Tables**  
    Check the data pulled for the new intersections to see if you find anything weird in the data. As a starting point, the following sample query can be used to check that the volumes correspond between `volumes`, `volumes_15min`, `volumes_15min_mvmt`, making sure to adjust all the datetime_bin filters and the intersection_uid filter. 

	```sql
	SELECT
		v.intersection_uid,
		v.classification_uid,
		SUM(v.volume) AS volume,
		v15_mvmt.volume AS volume_15_mvmt,
		CASE
			WHEN v.classification_uid IN (6, 10) THEN v15.volume
			ELSE ROUND(v15.volume/2, 0)
		END AS volume_15
	FROM miovision_api.volumes AS v
    --need to remove unacceptable similar to `miovision_api.aggregate_15_min_tmc`;
	LEFT JOIN miovision_api.unacceptable_gaps un
		ON un.intersection_uid = v.intersection_uid
		AND datetime_bin_ceil(v.datetime_bin, 15) - interval '15 minutes' = un.datetime_bin
    --identify volumes from miovision_api.volumes_15min_mvt
	LEFT JOIN LATERAL (
		SELECT
			intersection_uid,
			classification_uid,
			SUM(volume) AS volume
		FROM miovision_api.volumes_15min_mvt
		WHERE
            --adjust dates
			datetime_bin >= '2024-02-01 00:00:00'::timestamp - interval '1 hour'
			AND datetime_bin < '2024-02-02 00:00:00'::timestamp - interval '1 hour'
		GROUP BY
			intersection_uid,
			classification_uid
	) AS v15_mvmt ON
		v.intersection_uid = v15_mvmt.intersection_uid
		AND v.classification_uid = v15_mvmt.classification_uid
    --identify volumes from miovision_api.volumes_15min
	LEFT JOIN LATERAL (
		SELECT
			intersection_uid,
			classification_uid,
			SUM(volume) AS volume
		FROM miovision_api.volumes_15min
		WHERE
            --adjust dates
			datetime_bin >= '2024-02-01 00:00:00'::timestamp - interval '1 hour'
			AND datetime_bin < '2024-02-02 00:00:00'::timestamp - interval '1 hour'
		GROUP BY
			intersection_uid,
			classification_uid
	) AS v15 ON
		v.intersection_uid = v15.intersection_uid
		AND v.classification_uid = v15.classification_uid
	WHERE
        --adjust dates
		v.datetime_bin >= '2024-02-01 00:00:00'::timestamp - interval '1 hour'
		AND v.datetime_bin < '2024-02-02 00:00:00'::timestamp - interval '1 hour'
		AND v.intersection_uid >= 69 --adjust intersection here
		AND un.datetime_bin IS NULL
	GROUP BY
		v.intersection_uid,
		v.classification_uid,
		un.datetime_bin,
		v15.volume,
		v15_mvmt.volume
	```

4. **Done!**  
    From the next day onwards, the process will pull in both OLD and NEW intersections data via the automated Airflow process.

## Adding many intersections (Archived)  

Below is an optional method to import new intersections using an excel table and python. You may find it easier to use a simple SQL insert statement for one or two intersections. 

When adding multiple intersections, you can prepare updates to the table in an Excel
spreadsheet, read the spreadsheet into Python, and then append the spreadsheet
to `miovision_api.intersections`. First, create a spreadsheet with the same
columns in `miovision_api.intersections` - this can be done by exporting the
table in pgAdmin, and then deleting all the rows of data. Then insert new rows
of data representing the new intersections using the procedure above, keeping
`date_decommissioned` and `geom` blank (these will be filled in later). Finally,
run a script like the one below to get the new rows into `miovision_api.intersections`.

If you do use this method and the script below, **DO NOT INCLUDE ANY EXISTING
INTERSECTIONS IN YOUR EXCEL SPREADSHEET**.

```python
import pandas as pd
import psycopg2
from psycopg2.extras import execute_values

import configparser
import pathlib

# Read in Postgres credentials.
config = configparser.ConfigParser()
config.read(pathlib.Path.home().joinpath({YOUR_FILE}}).as_posix())
postgres_settings = config['POSTGRES']

# Process new intersections Excel file.
df = pd.read_excel({NEW_INTERSECTION_FILE})
# We'll deal with these later.
df.drop(columns=['date_decommissioned', 'geom'], inplace=True)
# psycopg2 translates None to NULL, so change any NULL in leg restricted column to None.
# If you have nulls in other columns you will need to handle them in the same way.
# https://stackoverflow.com/questions/4231491/how-to-insert-null-values-into-postgresql-database-using-python
for col in ('n_leg_restricted', 'e_leg_restricted',
            'w_leg_restricted', 's_leg_restricted'):
    df[col] = df[col].astype(object)
    df.loc[df[col].isna(), col] = None
df_list = [list(row.values) for i, row in df.iterrows()]

# Write Excel table row-by-row into miovision_api.intersections.
with psycopg2.connect(**postgres_settings) as conn:
    with conn.cursor() as cur:
        insert_data = """INSERT INTO miovision_api.intersections(intersection_uid, id, intersection_name,
                                                        date_installed, lat, lng,
                                                        street_main, street_cross, int_id, px,
                                                        n_leg_restricted, e_leg_restricted,
                                                        s_leg_restricted, w_leg_restricted, api_name) VALUES %s"""
        execute_values(cur, insert_data, df_list)
		update_geom = """UPDATE miovision_api.intersections a
							SET geom = ST_SetSRID(ST_MakePoint(b.lng, b.lat), 4326)
							FROM miovision_api.intersections b
							WHERE b.geom IS NULL
								AND a.id = b.id;"""
        cur.execute(update_geom)
        if conn.notices != []:
            print(conn.notices)
```

## Alternate Method of finding `px` (Archived)
For a large list of intersections you could convert to values and use `gis._get_intersection_id()` to identify the intersection_ids, px, and geom like so:  

```sql
WITH intersections(id, intersection_name_api) AS (
VALUES
	--note that suffixes had to be shortened to meet the threshold for matching `_get_intersection_id`
	('fe0550e0-ef27-49f2-a469-4e8511771e4a', 'Eglinton Ave E and Kennedy Rd'),
	('ff494e5c-628e-4d83-9cc3-13af52dbb88f', 'Bathurst St and Fort York Bl')
)

SELECT i.id, SPLIT_PART(i.intersection_name_api, ' and ', 1), SPLIT_PART(i.intersection_name_api, ' and ', 2), _get_intersection_id[3], ts.px::int, ts.geom
FROM intersections AS i,
LATERAL (
	SELECT * FROM gis._get_intersection_id(SPLIT_PART(i.intersection_name_api, ' and ', 1), SPLIT_PART(i.intersection_name_api, ' and ', 2), 0)
) AS agg
LEFT JOIN gis.traffic_signal AS ts ON ts.node_id = _get_intersection_id[3]
```
 
<p align="center">
	<img src="image-1.png" alt="Identifying miovision `px` using ITS Central" width="50%"/>
</p>
