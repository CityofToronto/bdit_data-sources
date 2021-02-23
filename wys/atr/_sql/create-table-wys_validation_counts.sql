/*
Creates table to store data from xlsx files in
L:\TDCSB\PROJECT\DATA COLLECTION GROUP\3 - MANUAL COUNTS\2020 Manual Counts\WYS SIGN OBSERVANCE\Kertcher

Files were read in and coverted to csv in notebook:
wys/atr/notebooks/tmc_data_preprocessing.ipynb
*/

CREATE TABLE data_analysis.wys_validation_counts
(
	datetime_bin timestamp without time zone,
	location text,
	class_type text,
	count numeric
);

ALTER TABLE data_analysis.wys_validation_counts
    OWNER to bdit_humans;

