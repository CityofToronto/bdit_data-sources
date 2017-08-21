* NO check in place for duplicate loading. Make sure that the data you are about to load is not loaded!

# Uploading SCOOT monthly 15min data (from downloaded files) to database [upload_scoot.py](upload_scoot.py)
## Packages & Modules & Authentication
1. pandas
2. pg (PyGreSQL)
3. db.cfg

## Usage
1. Modify datapath to direct to the folder that stores the data. Data file name should be 'yyyymmddRAW/M29_yyyymmddHhhR0[1-3].txt'.
2. Change the year and month you wish to load in the script (start_year, start_month, end_year, end_month)
3. Run the script
 
## Notes
1. The script loads backwards within the specified period. Most recent will be loaded first. There's data bleeding into the next month's file sometimes.
2. Content in db.cfg should be the following 
	[DBSETTINGS]
	database=bigdata
	host=10.160.12.47
	user=username
	password=password
3. Progress will be printed to screen
4. If data is stored on L drive, make sure the drive is accessible.

# Uploading SCOOT cycle level data (from downloaded files) to database [cyclelevel_upload.py](cyclelevel_upload.py)
## Packages & Modules & Authentication
1. pandas
2. pg (PyGreSQL)
3. db.cfg 

## Usage
1. Modify datapath to direct to the folder that stores the data. Data file name should be 'yyyymmddRAW/M29_yyyymmddHhhR0[1-3].txt'.
2. Change [start_date, end_date)
3. Run the script
 
## Notes
1. Content in db.cfg should be the following 
	[DBSETTINGS]
	database=bigdata
	host=10.160.12.47
	user=username
	password=password
2. Count_time in the files represent the end of the cycle. (shifted to the start in the script)
3. Progress will be printed to screen
4. If data is stored on L drive, make sure the drive is accessible.