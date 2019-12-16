# Blip API Pulling Script

## Contents

### blip_api

Script to pull Bluetooth data from the Blip api. Defaults to getting the previous day's data. This script is currently running on a Windows terminal server to access the Blip server. The script runs nightly at 6AM to pull the previous day's "historical" data and hourly to pull "live" data for our internal dashboards (see the flags below in [*Usage*](#usage) for more information).

#### Installation

Install the necessary packages using `pip` or `pipenv`: `pip install -r requirements.txt`

Make sure [`parsing_utilities`](https://github.com/CityofToronto/bdit_python_utilities) is available to your python environment.

#### Usage

```shell
usage: blip_api.py [-h] [-y YYYYMMDD YYYYMMDD] [-a ANALYSIS] [-d DBSETTING]
                   [--direct] [--live]

Pull data from blip API and send to database

optional arguments:
  -h, --help            show this help message and exit
  -y YYYYMMDD YYYYMMDD, --years YYYYMMDD YYYYMMDD
                        Range of dates (YYYYMMDD) to operate over from
                        startdate to enddate, else defaults to previous day.
  -a ANALYSIS, --analysis ANALYSIS
                        Analysis ID to pull. Add more flags for multiple IDs, 
                        else defaults to all pullable routes
  -d DBSETTING, --dbsetting DBSETTING
                        Filename with connection settings to the database
                        (default: opens config.cfg)
  --direct              Use DIRECT proxy if using from workstation
  --live                Pull most recent clock hour of live data
```

`--direct` is a work-around to make this work on a workstation instead of the terminal server.
`--live` is for pulling "live" data, versus "historical" data. This is to update the King Street Transit Pilot internal dashboards more frequently than daily. "live" data is less well filtered by the vendor's database.

If you want to pull individual analyses for a particular date, use the `-a` flag. For example, the below command will pull data for analysis IDs 156435 and  165375 from May 1st to May 12th 2018 inclusive:

```shell
python blip_api.py -y 20180501 20180512 -a 156435 -a 165375 -d config.cfg
```

#### Steps

1. Update route configurations in database
2. Identify configurations to pull data from
3. For each configuration pull data from YYYYMMDD to YYYYMMDD, or yesterday. Data is pulled in 4 steps because of a 10,000 row limit on API calls.
4. Send data for that configuration to Database
5. Move raw data to the `observations` partitioned table structure and trigger aggregations.

### notify_routes

Compares Blip route configurations in the database with the previous day's tables and sends an email if there are new or updated routes.
This script runs on the EC2 in order to be able to use the [`email_notifications`](https://github.com/CityofToronto/bdit_python_utilities/tree/master/email_notifications) module
