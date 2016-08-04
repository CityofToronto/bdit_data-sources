# Traffic Segment Speeds

Data collected from a variety of traffic probes from 2007 to 2016 for major streets and arterials.

Format is:

tx                 | tmc     | spd | score 
-------------------|---------|-----|-------
2012-07-01 00:00:00|9LX7Q6DDB| 32  | 10    

Where:  
 - **tx**: is the timestamp of the record in UTC
 - **tmc**: is the traffic management code, the unique ID for this segment
 - **spd**: is the speed, in mph
 - **score**: is "quality" of the record {10,20,30}, with 30 being based on observed data, and 10 based on historical data.
 
 
