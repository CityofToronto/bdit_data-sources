# WYS Validation

The purpose of this folder is to compare WYS sign data against manually-collected control data at selected locations.

## Data count files

Count data were manually collected at 14 locations for 6 class types ('Cars', 'Trucks', 'Bicycles', 'Transit', 'Pedestrians', 'Other') in 5-min timestamps in the AM period and PM period (generally from 10h00-12h00 and from 16h00-18h00). These data were stored in xlsx files (one for each location).

## Pre-processing and import to database

The manual count data were pre-processed in notebook `notebooks/tmc_data_preprocessing.ipynb`, where the 14 xslx files are read in, formatted into a single table, and saved to csv. The csv file was then imported into the database:

```
CREATE TABLE data_analysis.wys_validation_counts
(
        datetime_bin timestamp without time zone,
        location text,
        class_type text,
        count numeric
);

```

For QC, the notebook checks to make sure that all the rows in each xlsx file are read in, and lastly, plots the counts as a function of time for all locations, separately for each class type, for a quick overview of the data.
