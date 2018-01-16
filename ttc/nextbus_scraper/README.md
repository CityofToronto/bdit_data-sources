# TTC Scraper
## Setup

1. Create a Python 2 virtual environment: `pipenv --two`
2. Install the requirements from `requirements.txt` with `pipenv install`
3. Create the schema and tables with the modified `create_nb_tables.sql`
4. Modify `sample_conf.py` by setting the required `host`, `name`, `user`, and `password` fields. 
5. Then set the tables to:
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
6. Save the file and rename it to `conf.py`

## Storing Data

Run the file using `$ nohup python store.py > nextbus_store.log &` which will run the script in the background continuously.

## Processing Data

### Install Docker

Installed Docker for Ubuntu using [these instructions](https://docs.docker.com/engine/installation/linux/docker-ce/ubuntu/#set-up-the-repository)

### Set up OSRM

Generally following instructions from the [OSRM-Backend repository](https://github.com/Project-OSRM/osrm-backend#using-docker) (they're not that good though)

1. In the scraper folder, run `docker pull osrm/osrm-backend`  https://hub.docker.com/r/osrm/osrm-backend/
2. Downloaded a Metro Extract from Mapzen (RIP). Using `wget https://s3.amazonaws.com/metro-extracts.mapzen.com/toronto_canada.osm.pbf`
3. Reuse the `ttc.lua` configuration file for OSRM from `retro-gtfs` (do nothing here)
4. To set up OSRM, run `docker run -t -v $(pwd):/data osrm/osrm-backend osrm-extract -p /data/etc/ttc.lua /data/toronto_canada.osm.pbf`
   This binds the working directory `$(pwd)` to the `/data` folder within the docker container, and points osrm to the `ttc.lua` file and the `toronto_canada.osm.pbf` OSM extract. Note that their paths are absolute relative to within the container, thus starting with `/data/`
5. Run the next two commands
   ```shell
   docker run -t -v $(pwd):/data osrm/osrm-backend osrm-partition /data/toronto_canada.osrm
   docker run -t -v $(pwd):/data osrm/osrm-backend osrm-customize /data/toronto_canada.osrm
   ```
6. And then to run serve OSRM routing, run the below. Note that this will run in the foreground (within the terminal window), which is fine for a few processing tasks, but you'll want to run it in the background for continous processing. 
	```shell
	docker run -t -i -p 5000:5000 -v $(pwd):/data osrm/osrm-backend osrm-routed --algorithm mld /data/berlin-latest.osrm
	```

### Process Data
Edit `conf.py` to point to the local version of OSRM: `http://127.0.0.1:5000`
And then run `python process.py`and choose one of the [relevant prompts](https://github.com/SAUSy-Lab/retro-gtfs/wiki/Running-the-scripts#running-processpy)

## More Info
Go to the retro-gtfs [wiki](https://github.com/SAUSy-Lab/retro-gtfs/wiki/Running-the-scripts) for more info on running the scripts.
