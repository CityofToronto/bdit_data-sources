# Legacy Collisions

> [!Important]
>
> We are building out a new collision intake pipeline to replace the legacy collisions. Eventually (within the next few months-year), we will be winding down this legacy collision dataset. Please see [the new documentation here](./NewCollisions.md).

The collisions dataset contains information about traffic collisions and the people involved, that occurred in the City of Toronto’s Right-of-Way, from approximately 1985 to present, as reported by Toronto Police Services (TPS) or Collision Reporting Centres (CRC). Most of the information in this document pertains to collision data stored in the `bigdata` PostgreSQL database.


## Data Sources & Pipelines

The collision data comes from Toronto Police Services (TPS) and Collision Reporting Centres (CRC). The two sources of data are combined into one table through legacy data pipelines. The dataset owner is the Data Collection Team, part of the [Transportation Data & Analytics Unit](https://www.toronto.ca/services-payments/streets-parking-transportation/road-safety/big-data-innovation-team/), currently led by David McElroy (July 2024).

Legacy intake scripts convert raw data (from CSV and XML files) into legacy database table. The legacy table is copied to MOVE (`flashcrow`) nightly, and parsed from a flat collisions table into two relational tables: `events` and `involved`. These tables are then copied from MOVE (`flashcrow`) into the `bigdata` PostgreSQL data platform daily.

See more about [the data sources here](https://github.com/CityofToronto/bdit_collisions/blob/qa/docs/source/1_collision_overview.md), and more about [the data pipelines here](https://github.com/CityofToronto/bdit_collisions/blob/qa/docs/source/6_internal_references.md).


## Table Structure on the `bigdata` PostgreSQL Database

The `collisions` schema contains raw and derived collision data tables. The `collision_factors` schema contains lookup tables to convert numeric Motor Vehicle Collision Report (MVCR/MVA) report codes to human-readable descriptions (discussed further below). Both are owned by `collision_admins`.

### Collisions Schema (`events` and `involved` Tables)

There are four tables that are replicated daily from the MOVE (`flashcrow`) database:

- `collisions.acc`: A true copy of `"TRAFFIC"."ACC"`. Contains raw collision data retrieved from XML and CSV files. **This table is not accessible and should not be used except by collision core dataset admins**.
- `collisions.events`: Originally a materialized view in `flashcrow` derived from `collisions.acc`. Contains all event-level collision data from 1985-01-01 to present. Columns have proper data types and categorical columns contain text descriptions rather than numeric codes. The `events` table contains one row per collision event.
- `collisions.involved`:  Originally a materialized view in `flashcrow` derived from `collisions.acc`. Contains all involved-level collision data for individuals involved in collisions from 1985-01-01 to present. Column data type and categorical variable refinements similar to `collisions.events`. The `involved` table contains one row per involved person in each collision.
- `collisions.events_centreline`: Contains a mapping between `events` and `centreline` (based on a 20m conflation buffer).

A data dictionary (list of fields, definitions, and caveats) for the above tables can be found [here](https://github.com/CityofToronto/bdit_collisions/blob/qa/docs/source/4_data_dictionary.md).


### Important Limitations and Caveats About `collisions.acc`

Even though `collisions.acc` should not be directly queried (has that been mentioned yet?!), this information may be useful if you're trying to solve a collision data mystery:
- Each row represents one individual involved in a collision, so some data is repeated between rows. The data dictionary indicates which values are at the **event** level, and which at the individual **involved** level.
- `ACCNB` is not a UID. It *kind of* serves as one starting in 2013, but prior to that the number would reset annually, and even now, the TPS-derived `ACCNB`s might repeat every decade. Derived tables use `collision_no`, as defined in `collisions.events.collision_no`. You could use `REC_ID` if you want an involved-level UID.
- `ACCNB` is generated from TPS and CRC counterparts when data is loaded into the Oracle database. It can only be 10 characters long (by antiquated convention). 
  - TPS GO numbers, with format `GO-{YEAR_REPORTED}{NUMBER}`, (example: `GO-2021267847`) are converted into `ACCNB` by:
      - extracting `{NUMBER}`, 
      - zero-padding it to 9 digits (by adding zeros before the `{NUMBER}`), and 
      - adding the last digit of the year as the first digit (and that's how `GO-2021267847` becomes `1000267847`). 
  - CRC collision numbers are recycled annually, with convention dictating that the first CRC number be `8000000` (then the next `8000001`, etc.). To convert these to `ACCNB`s: 
      - take the last two digits of the year and 
      - add the 'year' digits as the first two digits of the `ACCNB` (so `8000285` reported in 2019 becomes `198000285`). 
  - The length of each `ACCNB` is used to determine the source of the data for the `src` column in `collisions.events` (since TPS `ACCNB`s have 10 digits while CRC `ACCNB`s have 9).
  - To keep the dataset current, particularly for fatal collisions, Data Collections will occasionally manually enter collisions using information directly obtained from TPS, or from public media. These entries may not follow `ACCNB` naming conventions. When formal data is transferred from TPS, they are manually merged with these human-generated entries.
- The date the collision was reported is not included in `acc`.
- Some columns are interpreted from others for internal use; for example `LOCCOORD` ("location coordinate") is a simplified version of `ACCLOC` ("accident location").
- Categorical data is coded using numbers. These numbers come from the Motor Vehicle Collision Report (MVCR/MVA) reporting scheme, set out by the province.
- Categorical data codes are stored in the `collision_factors` schema as tables. Each table name corresponds to the categorical column in `collisions.acc`. These are joined against `collisions.acc` to produce the derived materialized views.
- Some columns, such as the TPS officer badge number, are not included due to privacy concerns.
- TPS and CRC send collision records once they are reported and entered into their respective databases, which sometimes leads to collisions being reported months, or occasionally years, after they occur. 
- TPS and CRC will also send changes to existing collision records (using the same XML/CSV pipeline described above) to correct data entry errors or update the health status of an injured individual. 
- Moreover, staff at Data & Analytics continuously verify collision records manually, writing data changes directly to the legacy Oracle database. Therefore, one **cannot compare** historical control totals on e.g. the number of individuals involved with recently-generated ones.
- Speaking of verifying collision records... a value of `1` or `-1` (anything other than `0`) in `acc.CHANGED` means that a record has been changed. Records may be updated multiple times. 


## The Daily Collision Replicator

All collision tables are copies of other tables, materialized views, and views in MOVE (`flashcrow`). They are replicated daily from MOVE (`flashcrow`) into the `move_staging` schema in BigData by the DAG `bigdata_replicator`, which is maintained by MOVE team. The below figure shows the structure of this DAG as it reads the list of replicated tables from an Airflow variable, copies each table into `move_staging` schema, and triggers another DAG maintained by the Data Operations team.

![bigdata_replicator DAG Structure](./assets/bigdata_replicator_dag.png)

The downstream replicator DAG that is maintained by the Data Operations team is called `collisions_replicator` and is generated dynamically by [`replicator.py`](../dags/replicator.py) via the Airflow variable `replicators`. It is only triggered by the upstream MOVE replicator using Airflow REST APIs. The `collisions_replicator` DAG contains the following tasks:
- `wait_for_external_trigger` waits for a trigger from the upstream MOVE replicator DAG.
- `get_list_of_tables` reads the list of replicated tables from the Airflow variable `collisions_tables`, which contains pairs of source and destination tables. It usually copies tables from `move_staging` to either `collisions` or `collisions_factors`.
- `copy_tables` takes each of the tables loaded from `get_list_of_tables` and copies to its final destination. This is implemented by dynamically applying the `copy_tables` [common Airflow tasks](../dags/common_tasks.py) to each entry from `get_list_of_tables`.
- `status_message` task sends either a "success" Slack message, or lists out all the failures from the `copy_table` tasks. 

![collisions_replicator](./assets/collisions_replicator_dag.png)

<!-- replicator_table_check_doc_md -->
The [`replicator_table_check`](../dags/replicator_table_check.py) DAG runs daily and keeps track of tables being replicated by MOVE replicator vs. those by BigData replicators and sends Slack notifications when any issues are identified. This helps keep the MOVE/BigData replicator variables up to date with each other. NOTE: this one DAG covers tables included in both `collisions_replicator` and `traffic_replicator`. 
- `no_backfill`: a `LatestOnlyOperator` prevents old dates from running since this DAG involves comparison to current table comments. 
- `updated_tables`: identifies up-to-date tables in BigData `move_staging` via table comments like `Last updated on {ds}`. 
- `tables_to_copy`: identifies tables staged for copying via Airflow variables (`collisions_tables` and `traffic_tables`).
- `not_copied`: identifies up-to-date tables which are not being copied.
- `not_up_to_date`: identifies **not** up-to-date tables which are being copied.
- `outdated_remove`: identifies out of date tables in `move_staging` which should be deleted.
<!-- replicator_table_check_doc_md -->

![replicator_table_check_dag](./assets/replicator_table_check_dag.png)

### Replicating New Tables

If a new table or view needs to be replicated from MOVE, MOVE team should update their DAG first and replicate the new table/view into the BigData `move_staging` schema. Then, in BigData, you should create the new table in the appropriate schema to match the table definition in `move_staging`, then update the `collisions_tables` Airflow variable with the source and destination schema and table names. For instance, if you want to replicate table `collisions_a` from `move_staging` into the `collisions` schema and rename it to just `a`, you should add the following entry to the `collisions_tables` variable:

```JSON
[
    "move_staging.collisions_a",
    "collisions.a"
]
```

Then you will need to add appropriate permissions for the bot: 
```
GRANT SELECT ON TABLE move_staging.collisions_a TO collisions_bot;
--the bot needs to own the downstream table in order to update the table comment
ALTER TABLE collisions.a OWNER TO collisions_bot;
```

### Updating Existing Tables

If you need to update an existing table to match any new modifications introduced by the MOVE team, e.g., dropping columns or changing column types, you should update the replicated table definition according to these changes. If the updated table has dependencies, you need to save and drop them to apply the new changes and then update these dependencies and re-create them again. The [`public.deps_save_and_drop_dependencies_dryrun`](https://github.com/CityofToronto/bdit_pgutils/blob/master/create-function-deps_save_and_drop_dependencies_dryrun.sql) and `public.deps_restore_dependencies` functions might help updating the dependencies in complex cases. Finally, if there are any changes in the table's name or schema, you should also update the `collisions_tables` variable.
