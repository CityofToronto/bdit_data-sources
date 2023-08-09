# Miovision Intersection Update Resources

	- [Removing Intersections](#removing-intersections)
	- [Adding Intersections](#adding-intersections)
		- [Update `intersections`](#1-update-miovision_apiintersections)
		- [Update `intersections_intersection_movements`](#2update-miovision_apiintersection_movements)
		- [Backfill and Aggregate New Intersections](#3-backfillaggregate-new-intersection-data)

The main resource for adding new intersections is now located in the main readme [here](../README.md#1-update-miovision_apiintersections).  
This folder contains additional resources to assist with manually adding Miovision intersections. 

- [`New Intersection Activation Dates.ipynb`](#new-intersection-activation-datesipynb)
- [`Adding many intersections`](#adding-many-intersections)

There have been some changes to the Miovision cameras and below documents on the steps to make that changes on our end. Remove the decommissioned cameras first then include the newly installed ones. **Always run your test and put all new things in a different table/function name so that they do not disrupt the current process until everything has been finalized.** Ideally, the whole adding and removing process should be able to be done in one day.

### Removing Intersections
Once we are informed of the decommissioned date of a Miovision camera, we can carry out the following steps.

1) Update the column `date_decommissioned` on table [`miovision_api.intersections`](#intersections) to include the decommissioned date. The `date_decommissioned` is the date of the *last timestamp from the location* (so if the last row has a `datetime_bin` of '2020-06-15 18:39', the `date_decommissioned` is '2020-06-15').

2) Remove aggregated data on the date the camera is decommissioned. Manually remove decommissioned machines' data from tables `miovision_api.volumes_15min_mvt` and `miovision_api.volumes_15min`. Dont worry about other tables that they are linked to since we have set up the ON DELETE CASCADE functionality. If the machine is taken down on 2020-06-15, we are not aggregating any of the data on 2020-06-15 as it may stop working at any time of the day on that day.

3) Update the below table of when intersections were decommissioned for future references.

	|intersection_uid | last datetime_bin|
	|-----------------|------------------|
	9 | 2020-06-15 15:51:00|
	11 | 2020-06-15 09:46:00|
	13 | 2020-06-15 18:01:00|
	14 | NULL|
	16 | 2020-06-15 19:52:00|
	19 | 2020-06-15 20:18:00|
	30 | 2020-06-15 18:58:00|
	32 | 2020-06-15 18:30:00|

4) Done. Removing intersections is short and simple.

### Adding Intersections
Adding intersections is not as simple as removing an intersection. We will first have to find out some information before proceeding to aggregating the data. The steps are outlined below.

#### 1. Update `miovision_api.intersections`:
1. Look at the table [`miovision_api.intersections`](#intersections) to see what information about the new intersections is needed to update the table. The steps needed to find details such as id, coordinates, px, int_id, geom, which leg_restricted etc are descrived below. Once everything is done, have a member of `miovision_admins` do an INSERT INTO this table to include the new intersections.

	a) The new intersection's `intersection_name`, `id`, can be found using the [Miovision API](https://docs.api.miovision.com/#!/Intersections/get_intersections) /intersections endpoint. The key needed to authorize the API is the same one used by the Miovision Airflow user.
		
	b) `date_installed` is the *date of the first row of data from the location* (so if the first row has a `datetime_bin` of '2020-10-05 12:15', the `date_installed` is '2020-10-05'). `date_installed` can be found by by e-mailing Miovision, manually querying the Miovision API for the first available timestamp, or by running the Jupyter notebook in the `update_intersections` folder. 

	c) `date_decommissioned` is described under (#removing-intersections). 
		
	d) `px` can be found by searching the intersection name (location) in ITS Central (https://itscentral.corp.toronto.ca/) and finding the corresponding intersection id (PX####). `px` id can be used to look up the rest of the information (`street_main`, `street_cross`, `geom`, `lat`, `lng` and `int_id`) from table `gis.traffic_signal` as in the query below. Note that `px` is a zero padded text format in `gis.traffic_signal`, but stored as an integer in `miovision_api.intersections`. 

	<img src="image-1.png" alt="Identifying miovision `px` using ITS Central" width="600"/>
		
	f) In order to find out which leg of that intersection is restricted, go to Google Map to find out the direction of traffic.

	h) Prepare an insert statement for the new intersection(s). Alternatively [this](update_intersections/Readme.md#adding-many-intersections) readme contains a python snippet you can use to do the same, which may be helpful for adding a large number of intersections. 

	```sql
	INSERT INTO miovision_api.intersections(intersection_uid, id, intersection_name,
	date_installed, lat, lng, geom, street_main, street_cross, int_id, px, 
	n_leg_restricted, e_leg_restricted, s_leg_restricted, w_leg_restricted)

	WITH new_intersections (intersection_uid, id, intersection_name, date_installed, px,
							n_leg_restricted, e_leg_restricted, s_leg_restricted, w_leg_restricted) AS (
		VALUES
			(67, '11dcfdc5-2b37-45c0-ac79-3d6926553582', 'Sheppard Avenue West and Keele Street',
				'2021-06-16'::date, '0600', null, null, null, null),
			(68, '9ed9e7f3-9edc-4f58-ae5b-8c9add746886', 'Steeles Avenue West and Jane Street',
				'2021-05-12'::date, '0535', null, null, null, null)
	)

	SELECT
		ni.intersection_uid, --sequential 
		ni.id, --from api
		ni.intersection_name, --from api 
		ni.date_installed, --identify via communication or new_intersection_activation_dates.ipynb
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
		ni.w_leg_restricted
	FROM new_intersections AS ni
	JOIN gis.traffic_signal AS ts USING (px)
	```

#### Update `miovision_api.intersection_movements`  
2. Now that the updated table of [`miovision_api.intersections`](#intersections) is ready, we have to update the table [`miovision_api.intersection_movements`](#intersection_movements). We need to find out all valid movements for the new intersections from the data but we don't have that yet, so the following has to be done.

	a) If there is no data for the intersections in `miovision_api.volumes`, you will first need to run the [api script](https://github.com/CityofToronto/bdit_data-sources/blob/miovision_api_bugfix/volumes/miovision/api/intersection_tmc.py) with the following command line to only include intersections that we want as well as skipping the data processing process: `python3 intersection_tmc.py run-api --start_date=2020-06-15 --end_date=2020-06-16 --intersection=35 --intersection=38 --intersection=40  --pull`.  
	`--pull` has to be included in order to skip data processing and gaps finding since we are only interested in finding invalid movements in this step. Note that multiple intersections have to be stated that way in order to be included in the list of intersections to be pulled. Recommend to test it out with a day's worth of data first.

	b) Now that there is data in `miovision_api.volumes`, run the SELECT query below and validate those new intersection movements. The line `HAVING COUNT(DISTINCT datetime_bin::time) >= 20` is there to make sure that the movement is actually legit and not just a single observation. `volume::numeric / classification_volume >= 0.005` is a suggested addition to make sure that for lower volume modes (bicycles), we don't filter out a small volume but large percentage (5 / 1000).  
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
		GROUP BY intersection_uid, classification_uid, leg, movement_uid
		WINDOW w AS (PARTITION BY classification_uid)
	)

	-- Uncomment when you're ready to insert.
	-- INSERT INTO miovision_api.intersection_movements
	SELECT
		intersection_uid,
		classification_uid,
		leg,
		movement_uid
	FROM counts
	WHERE
		bins >= 20
		OR volume::numeric / classification_volume >= 0.005
	```

	ADD A QC SCRIPT HERE.

	If you find you need to manually add movements to the above,
	download the output of the query into a CSV, manually edit the CSV, then
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

	c) The step before only include valid intersection movements for
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

	d) Once the above is finished, we have completed updating the table [`miovision_api.intersection_movements`](#intersection_movements). **Though, the valid movements should be manually reviewed.**

### 3. Backfill/Aggregate new intersection data

3. Now that the intersection is configured and the raw volumes data is in the database, we have to finish aggregating the data.

	a) If not already complete, use the [api script](https://github.com/CityofToronto/bdit_data-sources/blob/miovision_api_bugfix/volumes/miovision/api/intersection_tmc.py) with `--pull` to backfill `miovision_api.volumes` table between the date_installed and previous day. 

	b) We now have to run a couple of functions manually with (%s::date, %s::date) being (start_date::date, end_date::date) to finish aggregating the backfilled data.  
	```sql
	SELECT miovision_api.find_gaps(%s::date, %s::date);
	SELECT miovision_api.aggregate_15_min_tmc(%s::date, %s::date);
	SELECT miovision_api.aggregate_15_min(%s::date, %s::date); 
	SELECT miovision_api.report_dates(%s::date, %s::date);
	```

	c) Check the data pulled for the new intersections to see if you find anything weird in the data. As a starting point, the following sample query can be used to check that the volumes correspond between `volumes`, `volumes_15min`, `volumes_15min_mvmt`, making sure to adjust all the datetime_bin filters and the intersection_uid filter.

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
	LEFT JOIN miovision_api.unacceptable_gaps un
		ON un.intersection_uid = v.intersection_uid
		AND v.datetime_bin >= DATE_TRUNC('hour', gap_start)
		AND v.datetime_bin < DATE_TRUNC('hour', gap_end) + interval '1 hour'
	LEFT JOIN LATERAL (
		SELECT
			intersection_uid,
			classification_uid,
			SUM(volume) AS volume
		FROM miovision_api.volumes_15min_mvt
		WHERE
			datetime_bin >= '2023-08-01 00:00:00'::timestamp - interval '1 hour'
			AND datetime_bin < '2023-08-02 00:00:00'::timestamp - interval '1 hour'
		GROUP BY
			intersection_uid,
			classification_uid
	) AS v15_mvmt ON
		v.intersection_uid = v15_mvmt.intersection_uid
		AND v.classification_uid = v15_mvmt.classification_uid
	LEFT JOIN LATERAL (
		SELECT
			intersection_uid,
			classification_uid,
			SUM(volume) AS volume
		FROM miovision_api.volumes_15min
		WHERE
			datetime_bin >= '2023-08-01 00:00:00'::timestamp - interval '1 hour'
			AND datetime_bin < '2023-08-02 00:00:00'::timestamp - interval '1 hour'
		GROUP BY
			intersection_uid,
			classification_uid
	) AS v15 ON
		v.intersection_uid = v15.intersection_uid
		AND v.classification_uid = v15.classification_uid
	WHERE
		v.datetime_bin >= '2023-08-01 00:00:00'::timestamp - interval '1 hour'
		AND v.datetime_bin < '2023-08-02 00:00:00'::timestamp - interval '1 hour'
		AND v.intersection_uid IN (64) 
		AND (
			un.accept is null
			OR un.accept IS TRUE)
	GROUP BY
		v.intersection_uid,
		v.classification_uid,
		un.accept,
		v15.volume,
		v15_mvmt.volume

	```

	d) From the next day onwards, the process will pull in both OLD and NEW intersections data via the automated Airflow process.

## `New Intersection Activation Dates.ipynb`
Jupyter notebook to help identify first date of data for each new intersection.

## Adding many intersections  
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
                                                        s_leg_restricted, w_leg_restricted) VALUES %s"""
        execute_values(cur, insert_data, df_list)
        if conn.notices != []:
            print(conn.notices)
```

Finally, to populate the geometries table, run the following query:

```sql
UPDATE miovision_api.intersections a
SET geom = ST_SetSRID(ST_MakePoint(b.lng, b.lat), 4326)
FROM miovision_api.intersections b
WHERE a.intersection_uid = b.intersection_uid
    AND a.intersection_uid IN ({INSERT NEW INTERSECTIONS HERE});
```
