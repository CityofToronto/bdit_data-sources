# Matching MOVE Stats

Hey wouldn't it be great if we could just sql ourselves some MOVE stats? 

Well, now we can!

As of January 2024, this folder contains code (in sql - not js!!!) that calculates summary stats from TMCs and speed volume data.

## TMC summary stats:
Calculate the following TMC summary stats with sql using [this code](tmc_stats.sql):
- AM peak hour volume (based on 15 minute bins)
- PM peak hour volume (based on 15 minute bins)

## Speed volume summary stats:
So far we are replicating [this type of summary data](https://move.intra.prod-toronto.ca/view/location/s1:Ab_DAA/POINTS/reports/ATR_SPEED_VOLUME).
Calculate the following speed volume summary stats with sql using [this code](speedvol_stats.sql):
- AM peak hour volume (based on hours, as per the Speed Perfcentile Report)
- PM peak hour volume (based on hours, as per the Speed Perfcentile Report)
- 15th, 50th, 85th and 95th percentile speeds
- mean speed
- daily volume