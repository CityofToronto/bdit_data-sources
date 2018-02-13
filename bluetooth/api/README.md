# Blip API Pulling Script

## Contents

### blip_api

Script to pull Bluetooth data from the Blip api. Defaults to getting the previous day's data.

```shell
usage: blip_api.py [-h] [-y YYYYMMDD YYYYMMDD] [-d DBSETTING] [--direct]
                   [--live]

Pull data from blip API and send to database

optional arguments:
  -h, --help            show this help message and exit
  -y YYYYMMDD YYYYMMDD, --years YYYYMMDD YYYYMMDD
                        Range of dates (YYYYMMDD) to operate over from
                        startdate to enddate, else defaults to previous day.
  -d DBSETTING, --dbsetting DBSETTING
                        Filename with connection settings to the database
                        (default: opens config.cfg)
  --direct              Use DIRECT proxy if using from workstation
  --live                Pull most recent clock hour of live data
```

`--direct` is a work-around to make this work on a workstation instead of the terminal server.
`--live` is for pulling "live" data, versus "historical" data. This is to update the King Street Transit Pilot internal dashboards more frequently than daily. "live" data is less well filtered by the vendor's database.

#### Steps

1. Update route configurations in database
2. Identify configurations to pull data from
3. For each configuration pull data from YYYYMMDD to YYYYMMDD, or yesterday. Data is pulled in 4 steps because of a 10,000 row limit on API calls.
4. Send data for that configuration to Database
5. Move raw data to the `observations` partitioned table structure and trigger aggregations.

### notify_routes

Compares Blip route configurations in the database with the previous day's tables and sends an email if there are new or updated routes.
This script runs on the EC2 in order to be able to use the [`email_notifications`](https://github.com/CityofToronto/bdit_python_utilities/tree/master/email_notifications) module