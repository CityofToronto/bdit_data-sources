# Uploading MTO data (from ONE-ITS) to database [upload.py](upload.py)
## Packages & Modules & Authentication
1. pandas
2. requests
3. ast (for parsing authentication file)
4. utilities (for db_connect, get_sql_results)
5. auth.txt
6. db.cfg (database authentication) 

## Usage
1. Make sure the packages and modules and files mentioned above are in the same folder
2. Change the year and month you wish to load in the script (start_year, start_month, end_year, end_month)
3. Run the script on the EC2 (doesn't work locally because of firewall restrictions)


## Notes
1. The script assumes that table mto.sensors is up-to-date. If there are new loops, update mto.sensors first.
2. Content in auth.txt should be the following {"username":"yourusername", "password":"yourpassword", "baseurl":"http://128.100.217.245/oneits_client/DownloadTrafficReport"}
3. Content in db.cfg should be the following 
	[DBSETTINGS]
	database=bigdata
	host=10.160.12.47
	user=username
	password=password
4. start/end year/month are inclusive.
5. A log will be kept (upload_mto_data.log). There will be information on the statuses of operations and the detectors that are not functioning (and there are a couple)
