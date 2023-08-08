# Miovision Intersection Update Resources

The main resource for adding new intersections is now located in the main readme [here](../README.md#1-update-miovision_apiintersections).  
This folder contains additional resources to assist with manually adding Miovision intersections. 

- [`New Intersection Activation Dates.ipynb`](#new-intersection-activation-datesipynb)
- [`Adding many intersections`](#adding-many-intersections)

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
