# Tables

This readme contains an overview of how Open Data schema is structured for Ecocounter. For detailed column descriptions, see documentation in the Open Data folder [here](../../open_data/sql/cycling_permanent_counts_readme.md), which mirrors what is posted to Open Data.

## `ecocounter.open_data_locations`
List of Ecocounter site-direction pairs for Open Data. Each site-direction pair can contain more than one flow_id, eg. regular, contraflow, scooter. 
- Only includes sites marked as `validated` in `ecocounter.sites`, `ecocounter.flows`.
- Sites are excluded if they have no data in `ecocounter.open_data_daily_counts`, for example because of an anomalous range. 

## ecocounter.open_data_daily_counts
Daily volumes (calibrated) for Open Data.
- Contains only complete days, excludes anomalous_ranges. 
- Data is inserted into this table each month by the `ecocounter_open_data` DAG using fn `ecocounter.open_data_daily_counts_insert`. 
- A primary key prevent us from accidentally overwriting older, already published data. 

## ecocounter.open_data_15min_counts
15 minute (or hourly for some older sites) volumes (calibrated) for Open Data.
- Contains only complete days, excludes anomalous_ranges.
- Data is inserted into this table each month by the `ecocounter_open_data` DAG using fn `ecocounter.open_data_15min_counts_insert`.
- A primary key prevent us from accidentally overwriting older, already published data. 