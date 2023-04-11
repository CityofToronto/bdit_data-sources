# Collisions

The collisions dataset consists of data on individuals involved in traffic collisions from approximately 1985 to the present day (though some historical collisions are included). Most of the information in this document pertains to collision data stored in the `bigdata` postgres database.

## Table Structure on the `bigdata` Postgres Database

The `collisions_replicator` schema houses raw and derived collision data tables. The `collision_factors` schema houses tables to convert raw Motor Vehicle Accident (MVA) report codes to human-readable categories (discussed further below). Both are owned by `collision_admins`.

### `ACC` and `acc_safe_copy`

The raw dataset is `collisions_replicator.ACC`, a direct mirror of the same table on the MOVE server. `collisions_replicator.acc_safe_copy` is a copy of `collisions_replicator.ACC`. The data dictionary for `ACC` is maintained jointly with MOVE and found on [Notion here](https://www.notion.so/bditto/Collision-Data-Dictionary-adc798de04fb47edaf129d9a6316eddf?pvs=4).

The guides that define values and categories for most columns can be found in the [Manuals page on Notion](https://www.notion.so/bditto/ca4e026b4f20474cbb32ccfeecf9dd76?v=a9428dc0fb3447e5b9c1427f8868e7c8).
In particular see the Collision Coding Manual, Motor Vehicle Collision Report 2015, and Motor Vehicle Accident Codes Ver. 1.

In order to stay up to date the `collisions_replicator.ACC` table is dropped (or deleted) and recreated every night. A third copy of the collisions table, called `collisions_replicator.acc_safe_copy`, was created to facilitate automatic updates on the `bigdata` postgres database. The `bigdata` postgres pipeline operates as follows:
1. The table `collisions_replicator.acc_safe_copy` is updated based on any differences between it and `collisions_replicator.ACC` (this is known as an "upsert" query).
2. The `collisions_replicator.collision_no` materialized view, which creates a unique id (UID) for each collision, is refreshed.
3. The `collisions_replicator.events` and `collisions_replicator.involved` materialized views are refreshed (more on this below).
4. New records, deletions and updates to existing records are tracked in a table called `collisions_replicator.logged_actions`.

**Please note:** `collisions_replicator.ACC` should never be queried directly, because any dependent views or tables would prevent `collisions_replicator.ACC` from being dropped and replaced (which would essentially freeze the whole pipeline). 

The `collisions_replicator.events` and `collisions_replicator.involved` materialized views should be used instead of `collisions_replicator.acc_safe_copy`, mostly because these materialized views contain human-readable categories instead of obscure numeric codes.

### Derived Materialized Views

As stated above, there are three materialzed views that are generated based on `collisions_replicator.acc_safe_copy`:

- `collisions_replicator.collision_no`: assigns a UID to each collision between 1985-01-01 and the most recent data refresh date. This is really just a materialized view of ids - there's no juicy collision data here - but generating the `collisions_no` id is essential for building the other two materialized views.
- `collisions_replicator.events`: all collision event-level data for collisions between 1985-01-01 and present. Columns have proper data types and categorical columns contain text descriptions rather than numerical codes.
- `collisions_replicator.involved`: all collision individual-level data for collisions between 1985-01-01 and present, with data type and categorical variable refinements similar to `collisions_replicator.events`.

A list of fields and definitions for the `collisions_replicator.events` and `collisions_replicator.involved` materialized views can be found on Notion [here](https://www.notion.so/bditto/Collision-Data-Dictionary-adc798de04fb47edaf129d9a6316eddf?pvs=4).

#### Properties of `collisions_replicator.acc_safe_copy`

Even though `collisions_replicator.acc_safe_copy` should not be directly queried (since the information is presented in a much more user-friendly way in the derived materialized views), it may be helpful to have some information about it...
- Each row represents one individual involved in a collision, so some data is repeated between rows. The data dictionary indicates which values are at the **event** level, and which at the individual **involved** level.
- `ACCNB` is not a UID. It kind of serves as one starting in 2013, but prior to that the number would reset annually, and even now, the TPS-derived `ACCNB`s might repeat every decade. Derived tables use `collision_no`, as defined in `collisions_replicator.collision_no`. You could use `RECID` if you want an "involved persons" level ID.
- `ACCNB` is generated from TPS and CRC counterparts when data is loaded into the Oracle database. It can only be 10 characters long (by antiquated convention). 
  - TPS GO numbers, with format `GO-{YEAR_REPORTED}{NUMBER}`, (example: `GO-2021267847`) are converted into `ACCNB` by:
      - extracting `{NUMBER}`, 
      - zero-padding it to 9 digits (by adding zeros before the `{NUMBER}`), and 
      - adding the last digit of the year as the first digit (and that's how `GO-2021267847` becomes `1000267847`). 
  - CRC collision numbers are recycled annually, with convention dictating that the first CRC number be `8000000` (then the next `8000001`, etc.). To convert these to `ACCNB`s: 
      - take the last two digits of the year and 
      - add the 'year' digits as the first two digits of the `ACCNB` (so `8000285` reported in 2019 becomes `198000285`). 
  - The length of each `ACCNB` is used to determine the source of the data for the `data_source` column in `collisions_replicator.events` (since TPS `ACCNB`s have 10 digits while CRC `ACCNB`s have nine).
  - To keep the dataset current, particularly for fatal collisions, Data Collections will occasionally manually enter collisions using information directly obtained from TPS, or from public media. These entries may not follow `ACCNB` naming conventions. When formal data is transferred from TPS, they are manually merged with these human-generated entries.
- The date the collision was reported is not included in `acc_safe_copy`.
- Some rows are derived from other rows by the Data & Analytics team; for example `LOCCOORD` is a simplified version of `ACCLOC`.
- Categorical data is coded using numbers. These numbers come from the Motor Vehicle Accident (MVA) reporting scheme.
- Some columns, such as the TPS officer badge number, are not included due to privacy concerns. The most egregious of these are only available in the original Oracle database, and have already been removed in the MOVE server data.
- TPS and CRC send collision records once they are reported and entered into their respective databases, which often leads to collisions being reported months, or even years, after they occurred. 
- TPS and CRC will also send changes to existing collision records (using the same XML/CSV pipeline described above) to correct data entry errors or update the health status of an injured individual. 
- Moreover, staff at Data & Analytics are constantly validating collision records, writing these changes directly to the Oracle database. Therefore, one **cannot compare** historical control totals on eg. the number of individuals involved with recently-generated ones.
- Speaking of validating collision records... a value of `1` or `-1` (anything other than `0`) in `acc_safe_copy.CHANGED` means that a record has... been... changed... 

### Collision Factors

Categorical data codes are stored in the `collision_factors` schema as tables. Each table name corresponds to the categorical column in `collisions_replicator.acc_safe_copy`. These are joined against `collisions_replicator.acc_safe_copy` to produce the derived materialized views.

## Data Sources and Ingestion Pipeline

The data comes from the Toronto Police Services (TPS) Versadex and the Collision Reporting Centre (CRC) database, and is combined by a Data Collections team in Transportation Services Policy & Innovation, currently led by David McElroy.

Data are transferred to a Transportation Services file server from TPS on a weekly basis, as a set of XML files, and from the CRC on a monthly basis as a single CSV file. A set of scripts (managed by Jim Millington) read in these raw data into the Transportation Services Oracle database table. This table is manually validated by Data Collections, and edits are made using legacy software from the 1990s.

The collisions table is copied into the MOVE postgres data platform (`flashcrow`) and the `bigdata` postgres data platform on a daily basis. Several derived materialized views are subsequently updated on the `bigdata` postgres database to make querying fun, easy, and exciting! 