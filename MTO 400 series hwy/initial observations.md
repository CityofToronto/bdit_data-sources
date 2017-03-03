# MTO Data Sample
## Overview:
Hwy|# detectors
---|-----------
400|64
401|610
403|7
404|55
407|91
410|140
412|36
QEW|305
TOTAL|1308

## Raw XML
* Text File contains (theoretically) 4319 rows. (Row 5 was split up into 2, accidental new line after xml header) 
* Each row is a standalone xml (i.e. trying to parse everything at once will fail due to the lack of a root element)
* Each row contains detections for all detectors in a 20-second period.
* Schema is shown in the diagram below  
[!Schema](XML Schema.png)


## ONE-ITS Monthly Traffic Count Summary
* Each csv contains one output (vol,speed,occ,or vol_30min) from one detector
* Cross-tabulated 
	- Rows: start of time bins (hh:mm:ss) 
	- Columns: date (dd-MMM-yyyy)
	- Value: missing values marked as X, also has unpopulated cells (very few)
* vol,speed,occ
	- 23:59:40 is not populated
* vol_30min
	- report on invalid data at the end: number of modified 20s periods in each 30min interval