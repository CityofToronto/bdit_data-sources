This folder contains Jupyter Notebooks used for developing API access and data processing methods.

- `volume_vs_gaps.ipynb` - comparison of gap-filling algorithms and rationale for selecting the one implemented in `miovision_api.find_gaps`.
- `01 - Test of Miovision Data Completeness With Cadence.ipynb` - check that Miovision API returns complete data when the time interval per API pull is set to 6 hours.
- `02 - Test New Data Insertion.ipynb` - check that a revised API pull script ([PR #332](https://github.com/CityofToronto/bdit_data-sources/pull/332)) can successfully update existing data without duplicates or conflicts.
- `03 - Postprocessing Smoke Test.ipynb` - basic testing of a new function to find unacceptable gaps in Miovision data, which replaces the `gapsize_lookup` matview with a CTE, against the existing version. Also contains visualizations of 15-minute aggregated data compared with legacy 15-min data.
- `intersection_tmc_notebook03test.py` - copy of `intersection_tmc.py` used by `03 - Postprocessing Smoke Test.ipynb` to test sequential processing.
