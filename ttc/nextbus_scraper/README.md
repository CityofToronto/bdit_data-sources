# TTC Scraper
## Setup
Modify `sample_conf.py` by setting the required `host`, `name`, `user`, and `password` fields. 
Then set the tables to:
```
'trips':'ttc_scrape.ttc_trips',
'stops':'ttc_scrape.ttc_stops',
'stop_times':'ttc_scrape.ttc_stop_times',
'directions':'ttc_scrape.ttc_directions'
```
Then set projection to:
```
	'projection':partial(
		 pyproj.transform,
		 pyproj.Proj('+init=EPSG:4326'),
		 pyproj.Proj('+init=EPSG:3857')
	),
```
And `localEPSG` to `3857` which is the same CRS as the tables.

Save the file and rename it to `conf.py`

Run the file using `$ nohup python store.py` which will run the script in the background continuously.

## More Info
Go to the retro-gtfs [wiki](https://github.com/SAUSy-Lab/retro-gtfs/wiki/Running-the-scripts) for more info on running the scripts.
