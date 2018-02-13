# Overview

(to be filled in)

# Raw Data

(to be filled in)

# Processing

1. Import raw data into `raw_data`.
2. Populate `volumes` with normalized 1-minute volume data, with links to `intersections`, `movements`, and `classifications`.
3. Populate `volumes_15min_tmc` using following criteria:
   1. **Remove**: Eliminate all 15-minute bins where 5 or fewer 1-minute bins are populated with data
   2. **Keep**: If 15-minute bin consists of 15 populated 1-minute bins, leave as is
   3. **Keep**: If difference between first and last 1-minute bin within 15-minute period is **greater than** the total number of populated 1-minute bins
