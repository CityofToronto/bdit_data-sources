This folder contains Jupyter Notebooks used for developing API access and data processing methods.

- `volume_vs_gaps.ipynb` - comparison of gap-filling algorithms and rationale for selecting the one implemented in `miovision_api.find_gaps`.
- `01 - Test of Miovision Data Completeness With Cadence.ipynb` - check that Miovision API returns complete data when the time interval per API pull is set to 6 hours.
- `02 - Test New Data Insertion.ipynb` - check that a revised API pull script ([PR #332](https://github.com/CityofToronto/bdit_data-sources/pull/332)) can successfully update existing data without duplicates or conflicts.