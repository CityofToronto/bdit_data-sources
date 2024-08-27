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
- [New Intersection Activation Dates.py](#new-intersection-activation-datespy)
- [Adding many intersections](#adding-many-intersections)

<!-- /TOC -->

# Removing Intersections
Once we are informed of the decommissioned date of a Miovision camera, we can carry out the following steps.

1) Update the column `date_decommissioned` on table [`miovision_api.intersections`](../readme.md#intersections) to include the decommissioned date. The `date_decommissioned` is the date of the *last timestamp from the location* (so if the last row has a `datetime_bin` of '2020-06-15 18:39', the `date_decommissioned` is '2020-06-15').

2) Remove aggregated data on the date the camera is decommissioned. Manually remove decommissioned machines' data from aggregate tables using [function-clear-volumes_15min.sql](../sql/function/function-clear-volumes_15min.sql), [function-clear-volumes_15min_mvt.sql](../sql/function/function-clear-volumes_15min_mvt.sql). You can also manually delete `volumes_daily` table. Dont worry about other tables that they are linked to since we have set up the ON DELETE CASCADE functionality. If the machine is taken down on 2020-06-15, we are not aggregating any of the data on 2020-06-15 as it may stop working at any time of the day on that day.

3) Done. Removing intersections is short and simple.

# Adding Intersections
Adding intersections is not as simple as removing an intersection. We will first have to find out some information before proceeding to aggregating the data. The steps are outlined below.

## Update `miovision_api.intersections`:

Look at the table [`miovision_api.intersections`](../readme.md#intersections) to see what information about the new intersections is needed to update the table. The steps needed to find details such as id, coordinates, px, int_id, geom, which leg_restricted etc are described below. Once everything is done, have a member of `miovision_admins` do an INSERT INTO this table to include the new intersections.

1. **Name and ID**  
    The new intersection's `api_name`, `id`, can be found using the [Miovision API](https://api.miovision.com/intersections) /intersections endpoint. The key needed to authorize the API is the same one used by the Miovision Airflow user. The `intersection_name` is an internal name following the convention `[E / W street name] / [N / S street name]`.
		
2. **date installed**  
    `date_installed` is the *date of the first row of data from the location* (so if the first row has a `datetime_bin` of '2020-10-05 12:15', the `date_installed` is '2020-10-05'). `date_installed` can be found by by e-mailing Miovision, manually querying the Miovision API for the first available timestamp, or by running the [script](new_intersection_activation_dates.py) in this folder. 

3.  **date_decommissioned**  
    `date_decommissioned` is described under (#removing-intersections). 
		
4. **px**  
    `px` is a uid used to identify signalized intersections. For 1 or 2 `px` is is easiest to find manually by searching the intersection name (location) in ITS Central (https://itscentral.corp.toronto.ca/) and finding the corresponding intersection id (PX####). `px` id can be used to look up the rest of the information (`street_main`, `street_cross`, `geom`, `lat`, `lng` and `int_id`) from table `gis.traffic_signal` as in the query below. Note that `px` is a zero padded text format in `gis.traffic_signal`, but stored as an integer in `miovision_api.intersections`. 

	**Alternate method** - For a large list of intersections you could convert to values and use `gis._get_intersection_id()` to identify the intersection_ids, px, and geom like so: 
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

5. **Restricted legs**  
    In order to find out which leg of that intersection is restricted (no cars approaching from that leg), go to Google Map to find out the direction of traffic.

6. **Insert statement**  
    Prepare an insert statement for the new intersection(s). Alternatively [this](#adding-many-intersections) section contains a python snippet you can use to do the same via a spreadsheet, which may be helpful for adding a large number of intersections. 

	```sql
	INSERT INTO miovision_api.intersections(intersection_uid, id, intersection_name,
	date_installed, lat, lng, geom, street_main, street_cross, int_id, px, 
	n_leg_restricted, e_leg_restricted, s_leg_restricted, w_leg_restricted, api_name)

	WITH new_intersections (intersection_uid, id, intersection_name, date_installed, px,
							n_leg_restricted, e_leg_restricted, s_leg_restricted, w_leg_restricted, api_name) AS (
		VALUES
			(67, '11dcfdc5-2b37-45c0-ac79-3d6926553582', 'Sheppard / Keele', 
				'2021-06-16'::date, '0600', null, null, null, null, 'Sheppard Avenue West and Keele Street'),
			(68, '9ed9e7f3-9edc-4f58-ae5b-8c9add746886', 'Steeles / Jane', 
				'2021-05-12'::date, '0535', null, null, null, null, 'Steeles Avenue West and Jane Street')
	)

	SELECT
		ni.intersection_uid, --sequential 
		ni.id, --from api
		ni.intersection_name, --cleaned name
		ni.date_installed, --identify via communication or new_intersection_activation_dates.py
		ts.latitude,
		ts.longitude,
		ts.geom,
		ts.main_street AS street_main,
		ts.side1_street AS street_cross,
		ts.node_id AS int_id,
		ni.px::integer AS px, 
		ni.n_leg_restricted,
		ni.e_leg_restricted,
		ni.s_leg_restricted,
		ni.w_leg_restricted.
		api_name --from api 
	FROM new_intersections AS ni
	JOIN gis.traffic_signal AS ts USING (px)
	```

7. **Update geojson**  
	Update the [geojson intersections file](../geojson/mio_intersections.geojson) by exporting to file from QGIS with `GeoJSON` format. This geojson file is helpful as a publically accessible record of our Miovision intersections.   
	
<p align="center">
	<img src="geojson_export.png" alt="Export to file from QGIS" width="70%"/>
</p>

8. **Update `miovision_api.centreline_miovision`**

	[`miovision_api.centreline_miovision`](../sql/readme.md#centreline_miovision) links Miovision intersection legs to `gis_core.centreline` street segments. 

	Use [**this script**](../sql/updates/update-centreline_miovision.sql) to add new intersections to `centreline_miovision`. The script can automatically identify the correct direction and centreline segment for most Miovision intersections, but manual adjustments are needed for the following situations:
	- Segments are not aligned in a North-South or East-West direction (like Kingston Road)
	- Segments intersect at odd angles (like Kingston Road and Eglinton Avenue)
	- One or more "legs" is not a street segment (like the entrance to the shopping centre at Danforth and Jones)

	The script above also contains checks for duplicates and values missing from the table. 

## Update `miovision_api.intersection_movements`  

Now that the updated table of [`miovision_api.intersections`](../readme.md#intersections) is ready, we have to update the table [`miovision_api.intersection_movements`](../readme.md#intersection_movements). Intersection movements determines which movements should be aggregated, by classification, typically for reporting purposes. Yes, we can see all kinds of wacky behaviour out there, but analyzing that is rarer than reporting on the main movements, so this makes basic analysis a little bit easier.

We need to find out all valid movements for the new intersections from the data but we don't have that yet, so the following has to be done.

1. **Populate `miovision_api.volumes`**  
    If there is no data for the intersections in `miovision_api.volumes`, you will first need to run the [api script](../api/intersection_tmc.py) with the following command line to only include intersections that we want as well as skipping the data processing process:  
		`python3 intersection_tmc.py run-api --start_date={DATE INSTALLED} --end_date={TODAYS DATE} --intersection=35 --intersection=38 --pull --path=/data/airflow/data_scripts/volumes/miovision/api/config.cfg`  

	Include `--pull` and not `--agg` to only pull data and skip data processing and gaps finding since we are only interested in finding valid movements in this step. Note that multiple intersections have to be stated that way in order to be included in the list of intersections to be pulled. 

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

1. **Backfill `miovision_api.volumes`**   
    If not already complete, use the [api script](../api/intersection_tmc.py) with `--pull` to backfill `miovision_api.volumes` table between the date_installed and current date (exclusive). Skip aggregating data by omitting `--agg` flag. 

2. **Backfill additional tables**  
	Next use the [api script](../api/intersection_tmc.py) with `--agg` to backfill the aggregate tables between the date_installed and current date (exclusive). Skip pulling data by omitting `--pull` flag. 

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

# [New Intersection Activation Dates.py](new_intersection_activation_dates.py)

Jupyter notebook to help identify new intersections and first date of data for each new intersection.

# Adding many intersections  

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
