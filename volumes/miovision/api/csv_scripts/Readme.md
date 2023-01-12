# CSV Processing Scripts

This folder contains scripts for processing CSV data dumps from Miovision. Some
scripts will also work for API data.

- `miov_bulk_postprocessor.py` - script that performs only the gap-finding and
  15-minute aggregation steps of `intersection_tmc.py`. Works on either CSV or
  API data.
