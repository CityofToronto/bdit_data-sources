# Development Notebooks

This folder contains Jupyter Notebooks used for developing API access and data processing methods.

- `volume_vs_gaps.ipynb` - comparison of gap-filling algorithms and rationale for selecting the one implemented in `miovision_api.find_gaps`.
- `01 - Test of Miovision Data Completeness With Cadence.ipynb` - check that Miovision API returns complete data when the time interval per API pull is set to 6 hours.
- `02 - Test New Data Insertion.ipynb` - check that a revised API pull script ([PR #332](https://github.com/CityofToronto/bdit_data-sources/pull/332)) can successfully update existing data without duplicates or conflicts.
- `03 - Postprocessing Smoke Test.ipynb` - basic testing of a new function to find unacceptable gaps in Miovision data, which replaces the `gapsize_lookup` matview with a CTE, against the existing version. Also contains visualizations of 15-minute aggregated data compared with legacy 15-min data.
- `intersection_tmc_notebook03test.py` - copy of `intersection_tmc.py` used by `03 - Postprocessing Smoke Test.ipynb` to test sequential processing.
- `04 - Manual Data Refresh Smoke Test.ipynb` - non-exhaustive check that the
  15-minute aggregation of CSV data was successful, and consistent with some
  hour-binned data saved in `covid.miovision_summary_20200922backup`.
- `05 - Compare Miovision API and CSV Data.ipynb` - three-way comparison between
  two separate API refreshes and CSV data. See [this comment in Issue
  #331](https://github.com/CityofToronto/bdit_data-sources/issues/331#issuecomment-718893812),
 [this one in #334](https://github.com/CityofToronto/bdit_data-sources/issues/331#issuecomment-718893812), and subsequent discussion in both issues.
- `06 - Bike Approach Sandbox` - initial look at bike approach volumes from Miovision, and prototype how to get it into our database.
- `06_bike_approach_sandbox_testdb.sql` - SQL to generate a prototype Miovision
  database ecosystem that handles bike approach volumes. Results are validated
  in `06 - Bike Approach Sandbox.ipynb`.
- `intersection_tmc_notebook06test.py` - copy of `intersection_tmc.py` from commit [74a239](https://github.com/CityofToronto/bdit_data-sources/commit/74a2392491bb8098c12bc779d63ea10277d4505c) used by 
  `06 - Bike Approach Sandbox.ipynb` to compare legacy and new API pullers.