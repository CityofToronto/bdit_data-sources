# Development Notebooks

This folder contains Jupyter Notebooks used for developing API access and data processing methods.

- `volume_vs_gaps.ipynb` - comparison of gap-filling algorithms and rationale for selecting the one implemented in `miovision_api.find_gaps`.

- `01 - Test of Miovision Data Completeness With Cadence.ipynb` - check that Miovision API returns complete data when the time interval per API pull is set to 6 hours.

- `02 - Test New Data Insertion.ipynb` - check that a revised API pull script ([PR #332](https://github.com/CityofToronto/bdit_data-sources/pull/332)) can successfully update existing data without duplicates or conflicts.

- `03 - Postprocessing Smoke Test.ipynb` - basic testing of a new function to find unacceptable gaps in Miovision data, which replaces the `gapsize_lookup` matview with a CTE, against the existing version. Also contains visualizations of 15-minute aggregated data compared with legacy 15-min data.

- `intersection_tmc_notebook03test.py` - copy of `intersection_tmc.py` used by `03 - Postprocessing Smoke Test.ipynb` to test sequential processing.

- `04 - Manual Data Refresh Smoke Test.ipynb` - non-exhaustive check that the 15-minute aggregation of CSV data was successful, and consistent with some hour-binned data saved in `covid.miovision_summary_20200922backup`.

- `05 - Compare Miovision API and CSV Data.ipynb` - three-way comparison between two separate API refreshes and CSV data. See [this comment in Issue #331](https://github.com/CityofToronto/bdit_data-sources/issues/331#issuecomment-718893812), [this one in #334](https://github.com/CityofToronto/bdit_data-sources/issues/331#issuecomment-718893812), and subsequent discussion in both issues.

- `06 - Bike Approach Sandbox` - initial look at bike approach volumes from Miovision, and prototype how to get it into our database.

- `06_files/06_bike_approach_sandbox_testdb.sql` - SQL to generate a prototype Miovision database ecosystem that handles bike approach volumes. Results are validated in `06 - Bike Approach Sandbox.ipynb`.

- `06_files/intersection_tmc_bikeapprch.py` - hacked version of `intersection_tmc.py` that inserts data into tables created by `06_bike_approach_sandbox_testdb.sql`. Results are validated in `06 - Bike Approach Sandbox.ipynb`.

- `intersection_tmc_notebook06test.py` - copy of `intersection_tmc.py` from commit [74a239](https://github.com/CityofToronto/bdit_data-sources/commit/74a2392491bb8098c12bc779d63ea10277d4505c) used by `06 - Bike Approach Sandbox.ipynb` to compare legacy and new API pullers.

- `lousy_dates.ipynb` contains monthly and weekly line graphs of light vehicle volumes. The aim is to visually inspect and identify unusually low (or high) volumes. This notebook evaluates data from January 2019 to March 2023. It directly queries the `bigdata` postgres database to grab monthly and weekly miovision volume counts for light vehicles only. The weekly graphs were used to identify dates with low or no volumes which were then used to populate `scannon.miovision_bad_weeks` (the script for which can be found [here](qc_sqls/t1_create_x_weeks.sql)).

- The folder `qc_sqls` contains the scripts that were used initially to generate the `miovision_api.anomalous_ranges` table. In Spring 2023, staff examined data from each Miovision camera to identify date ranges with questionable volumes. Here is a description of the process:
    1) Weekly volumes for 'lights' were graphed. There was one graph for each intersection.
    2) The line graphs were visually inspected. Weeks with lower-than-typical volumes were recorded in Excel.
    3) The Excel table was imported into a postgres table.
    4) In postgres, data from the initial table was augmented to:
        a) convert dates identifying bad weeks into date ranges using the gaps and islands approach
        b) add a field that describes the severity of the data quality issue
        c) add a field that describes the results of any investigations that have taken place.

    - `qc_sqls` contains the following scripts:
        - `t1_create_x_weeks.sql`: a `CREATE TABLE` script to store the 'bad dates' initially recorded in Excel
        - `t2_create_mio_dq_notes`: a script that uses the gaps and islands approach to group contiguous bad weeks into date ranges and that differentiates between weeks with low or no volume.



